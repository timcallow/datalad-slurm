# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Reschedule commands recorded with `datalad schedule`"""

__docformat__ = "restructuredtext"


import logging
import os.path as op
import re
import sys
from copy import copy
from functools import partial
from itertools import dropwhile

from datalad.consts import PRE_INIT_COMMIT_SHA

from datalad.distribution.dataset import (
    EnsureDataset,
    datasetmethod,
    require_dataset,
)
from datalad.interface.base import (
    Interface,
    build_doc,
    eval_results,
)
from datalad.interface.common_opts import jobs_opt
from datalad.interface.results import get_status_dict
from datalad.support.constraints import (
    EnsureNone,
    EnsureStr,
)
from datalad.support.exceptions import CapturedException
from datalad.support.param import Parameter

from datalad.core.local.run import (
    _format_cmd_shorty,
    format_command,
    assume_ready_opt,
)

# from .schedule import _execute_slurm_command
from .schedule import schedule_cmd

from .common import get_finish_info

lgr = logging.getLogger("datalad.local.reschedule")

reschedule_assume_ready_opt = copy(assume_ready_opt)
reschedule_assume_ready_opt._doc += """
Note that this option also affects any additional outputs that are
automatically inferred based on inspecting changed files in the schedule commit."""


@build_doc
class Reschedule(Interface):
    """Re-execute previous `datalad schedule` commands.

    This will unlock any dataset content that is on record to have
    been modified by the command in the specified revision.  It will
    then re-execute the command in the recorded path (if it was inside
    the dataset). Afterwards, all modifications will be saved.

    *Report mode*

    || REFLOW >>
    When called with [CMD: --report CMD][PY: report=True PY], this command
    reports information about what would be re-executed as a series of records.
    There will be a record for each revision in the specified revision range.
    Each of these will have one of the following "reschedule_action" values:
    << REFLOW ||

      - run: the revision has a recorded command that would be re-executed
      - skip-or-pick: the revision does not have a recorded command and would
        be either skipped or cherry picked
      - merge: the revision is a merge commit and a corresponding merge would
        be made

    The decision to skip rather than cherry pick a revision is based on whether
    the revision would be reachable from HEAD at the time of execution.

    In addition, when a starting point other than HEAD is specified, there is a
    rerun_action value "checkout", in which case the record includes
    information about the revision the would be checked out before rerunning
    any commands.
    """

    _params_ = dict(
        revision=Parameter(
            args=("revision",),
            metavar="REVISION",
            nargs="?",
            doc="""reschedule command(s) in `revision`. By default, the command from
            this commit will be executed, but [CMD: --since CMD][PY: `since`
            PY] can be used to construct a revision range. The default value is
            like "HEAD" but resolves to the main branch when on an adjusted
            branch.""",
            default=None,
            constraints=EnsureStr() | EnsureNone(),
        ),
        since=Parameter(
            args=("--since",),
            doc="""If `since` is a commit-ish, the commands from all commits
            that are reachable from `revision` but not `since` will be
            re-executed (in other words, the commands in :command:`git log
            SINCE..REVISION`). If SINCE is an empty string, it is set to the
            parent of the first commit that contains a recorded command (i.e.,
            all commands in :command:`git log REVISION` will be
            re-executed).""",
            constraints=EnsureStr() | EnsureNone(),
        ),
        message=Parameter(
            args=(
                "-m",
                "--message",
            ),
            metavar="MESSAGE",
            doc="""use MESSAGE for the reran commit rather than the
            recorded commit message.  In the case of a multi-commit
            rerun, all the reran commits will have this message.""",
            constraints=EnsureStr() | EnsureNone(),
        ),
        script=Parameter(
            args=("--script",),
            metavar="FILE",
            doc="""extract the commands into [CMD: FILE CMD][PY: this file PY]
            rather than rerunning. Use - to write to stdout instead. [CMD: This
            option implies --report. CMD]""",
            constraints=EnsureStr() | EnsureNone(),
        ),
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""specify the dataset from which to rerun a recorded
            command. If no dataset is given, an attempt is made to
            identify the dataset based on the current working
            directory. If a dataset is given, the command will be
            executed in the root directory of this dataset.""",
            constraints=EnsureDataset() | EnsureNone(),
        ),
        report=Parameter(
            args=("--report",),
            action="store_true",
            doc="""Don't actually re-execute anything, just display what would
            be done. [CMD: Note: If you give this option, you most likely want
            to set --output-format to 'json' or 'json_pp'. CMD]""",
        ),
        with_failed_jobs=Parameter(
            args=("--with-failed-jobs",),
            action="store_true",
            doc="""Reschedule jobs that did not succesfully run, i.e. whose status
            is not 'Completed'. By default, these are ignored in a reschedule.""",
        ),        
        assume_ready=reschedule_assume_ready_opt,
        jobs=jobs_opt,
    )

    _examples_ = [
        dict(
            text="Re-schedule the command from the previous commit",
            code_py="reschedule()",
            code_cmd="datalad reschedule",
        ),
        dict(
            text="Re-schedule any commands in the last five commits",
            code_py="reschedule(since='HEAD~5')",
            code_cmd="datalad reschedule --since=HEAD~5",
        ),
    ]

    @staticmethod
    @datasetmethod(name="slurm_reschedule")
    @eval_results
    def __call__(
        revision=None,
        *,
        since=None,
        dataset=None,
        message=None,
        script=None,
        report=False,
        with_failed_jobs=False,
        assume_ready=None,
        jobs=None,
    ):
        ds = require_dataset(
            dataset, check_installed=True, purpose="reschedule a command"
        )
        ds_repo = ds.repo

        lgr.debug("rescheduling command output underneath %s", ds)

        if not ds_repo.get_hexsha():
            yield get_status_dict(
                "slurm-reschedule",
                ds=ds,
                status="impossible",
                message="cannot reschedule command, nothing recorded",
            )
            return

        # ATTN: Use get_corresponding_branch() rather than is_managed_branch()
        # for compatibility with a plain GitRepo.

        # get branch
        rev_branch = (
            ds_repo.get_corresponding_branch() or ds_repo.get_active_branch() or "HEAD"
        )

        if revision is None:
            revision = rev_branch

        if not ds_repo.commit_exists(revision + "^"):
            # Only a single commit is reachable from `revision`.  In
            # this case, --since has no effect on the range construction.
            revrange = revision
        elif since is None:
            revrange = "{rev}^..{rev}".format(rev=revision)
        elif since.strip() == "":
            revrange = revision
        else:
            revrange = "{}..{}".format(since, revision)

        results = _rerun_as_results(ds, revrange, since, message, rev_branch, with_failed_jobs)
        if script:
            handler = _get_script_handler(script, since, revision)
        elif report:
            handler = _report
        else:
            handler = partial(
                _rerun, assume_ready=assume_ready, explicit=True, jobs=jobs
            )

        for res in handler(ds, results):
            yield res


def _revrange_as_results(dset, revrange):
    """
    Generate results for a given revision range in a dataset.

    Parameters
    ----------
    dset : Dataset
        The dataset object containing the repository.
    revrange : str
        The revision range to process.

    Yields
    ------
    dict
        A dictionary containing the status and run information for each commit
        in the revision range. The dictionary includes:
        - 'status': The status of the operation, always 'ok'.
        - 'ds': The dataset object.
        - 'commit': The commit hash.
        - 'parents': The parent commits of the commit.
        - 'slurm_run_info': The run information if available.
        - 'run_message': The run message if available.

    Raises
    ------
    ValueError
        If there is an error processing the commit message.
    """
    ds_repo = dset.repo
    rev_lines = ds_repo.get_revisions(
        revrange, fmt="%H %P", options=["--reverse", "--topo-order"]
    )
    if not rev_lines:
        return

    for rev_line in rev_lines:
        # The strip() below is necessary because, with the format above, a
        # commit without any parent has a trailing space. (We could also use a
        # custom `rev-list --parents ...` call to avoid this.)
        fields = rev_line.strip().split(" ")
        rev, parents = fields[0], fields[1:]
        res = get_status_dict("slurm-reschedule", ds=dset, commit=rev, parents=parents)
        full_msg = ds_repo.format_commit("%B", rev)
        try:
            msg, info = get_finish_info(dset, full_msg)
        except ValueError as exc:
            # Recast the error so the message includes the revision.
            raise ValueError("Error on {}'s message".format(rev)) from exc
            
        if msg and info:
            job_failed = parse_job_status(msg)
            
        if info is not None:
            if len(parents) != 1:
                lgr.warning(
                    "%s has run information but is a %s commit; "
                    "it will not be re-executed",
                    rev,
                    "merge" if len(parents) > 1 else "root",
                )
                continue
            res["slurm_run_info"] = info
            res["run_message"] = msg
            res["job_failed"] = job_failed
        yield dict(res, status="ok")


def _rerun_as_results(dset, revrange, since, message, rev_branch, with_failed_jobs):
    """
    Represent the rerun as result records.

    Parameters
    ----------
    dset : Dataset
        The dataset to operate on.
    revrange : str
        The range of revisions to consider for rerunning.
    since : str
        The starting point for the range of revisions.
    message : str
        The message to use for the rerun commits.
    rev_branch : str
        The branch to use for the rerun commits.
    with_failed_jobs: bool
        Re-schedule those commits where the job (partially) failed.

    Yields
    ------
    dict
        A dictionary representing the result of processing each commit in the
        specified revision range. Each dictionary contains information about
        the rerun action, status, and any relevant messages or exceptions.
    """
    try:
        results = _revrange_as_results(dset, revrange)
    except ValueError as exc:
        ce = CapturedException(exc)
        yield get_status_dict(
            "slurm-reschedule", status="error", message=str(ce), exception=ce
        )
        return

    ds_repo = dset.repo
    # Drop any leading commits that don't have a slurm run command. These would be
    # skipped anyways.
    results = list(dropwhile(lambda r: "slurm_run_info" not in r, results))
    # now drop the results which did not run succesfully
    if not with_failed_jobs:
        results = [result for result in results if not result["job_failed"]]
    
    if not results:
        yield get_status_dict(
            "slurm-reschedule",
            status="impossible",
            ds=dset,
            message=("No schedule commits found in range %s", revrange),
        )
        return

    def skip_or_pick(hexsha, result, msg):
        result["rerun_action"] = "skip-or-pick"
        shortrev = ds_repo.get_hexsha(hexsha, short=True)
        result["message"] = ("%s %s; %s", shortrev, msg, "skipping or cherry picking")

    for res in results:
        hexsha = res["commit"]
        if "slurm_run_info" in res:
            rerun_dsid = res["slurm_run_info"].get("dsid")
            if rerun_dsid is not None and rerun_dsid != dset.id:
                skip_or_pick(hexsha, res, "was ran from a different dataset")
                res["status"] = "impossible"
            else:
                res["rerun_action"] = "run"
                res["diff"] = diff_revision(dset, hexsha)
                # This is the overriding message, if any, passed to this rerun.
                res["rerun_message"] = message
        else:
            if len(res["parents"]) > 1:
                res["rerun_action"] = "merge"
            else:
                skip_or_pick(hexsha, res, "does not have a command")
        yield res


def _mark_nonrun_result(result, which):
    msg = dict(skip="skipping", pick="cherry picking")[which]
    result["rerun_action"] = which
    result["message"] = result["message"][:-1] + (msg,)
    return result


def _rerun(dset, results, assume_ready=None, explicit=True, jobs=None):
    """
    Rerun a series of actions on a dataset.

    Parameters
    ----------
    dset : Dataset
        The dataset on which to rerun the actions.
    results : list of dict
        A list of result dictionaries, each representing an action to rerun.
    assume_ready : bool, optional
        If True, assume that the dataset is ready for rerun without additional checks.
    explicit : bool, optional
        If True, rerun actions explicitly specified in the results.
    jobs : int, optional
        The number of jobs to use for parallel processing.

    Yields
    ------
    dict
        Result dictionaries after rerunning the actions.

    Notes
    -----
    This function handles various rerun actions such as 'checkout', 'merge', 'skip-or-pick', and 'run'.
    It maintains a map from original commit hashes to new commit hashes created during the rerun process.
    """
    ds_repo = dset.repo
    # Keep a map from an original hexsha to a new hexsha created by the rerun
    # (i.e. a reran, cherry-picked, or merged commit).
    new_bases = {}  # original hexsha => reran hexsha
    branch_to_restore = ds_repo.get_active_branch()
    head = onto = ds_repo.get_hexsha()
    for res in results:
        lgr.info(_get_rerun_log_msg(res))
        rerun_action = res.get("rerun_action")
        if not rerun_action:
            yield res
            continue

        res_hexsha = res["commit"]
        if rerun_action == "checkout":
            if res.get("branch"):
                branch = res["branch"]
                checkout_options = ["-b", branch]
                branch_to_restore = branch
            else:
                checkout_options = ["--detach"]
                branch_to_restore = None
            ds_repo.checkout(res_hexsha, options=checkout_options)
            head = onto = res_hexsha
            continue

        # First handle the two cases that don't require additional steps to
        # identify the base, a root commit or a merge commit.

        if not res["parents"]:
            _mark_nonrun_result(res, "skip")
            yield res
            continue

        if rerun_action == "merge":
            old_parents = res["parents"]
            new_parents = [new_bases.get(p, p) for p in old_parents]
            if old_parents == new_parents:
                if not ds_repo.is_ancestor(res_hexsha, head):
                    ds_repo.checkout(res_hexsha)
            elif res_hexsha != head:
                if ds_repo.is_ancestor(res_hexsha, onto):
                    new_parents = [
                        p for p in new_parents if not ds_repo.is_ancestor(p, onto)
                    ]
                if new_parents:
                    if new_parents[0] != head:
                        # Keep the direction of the original merge.
                        ds_repo.checkout(new_parents[0])
                    if len(new_parents) > 1:
                        msg = ds_repo.format_commit("%B", res_hexsha)
                        ds_repo.call_git(
                            [
                                "merge",
                                "-m",
                                msg,
                                "--no-ff",
                                "--allow-unrelated-histories",
                            ]
                            + new_parents[1:]
                        )
                    head = ds_repo.get_hexsha()
                    new_bases[res_hexsha] = head
            yield res
            continue

        # For all the remaining actions, first make sure we're on the
        # appropriate base.

        parent = res["parents"][0]
        new_base = new_bases.get(parent)
        head_to_restore = None  # ... to find our way back if we skip.

        if new_base:
            if new_base != head:
                ds_repo.checkout(new_base)
                head_to_restore, head = head, new_base
        elif parent != head and ds_repo.is_ancestor(onto, parent):
            if rerun_action == "run":
                ds_repo.checkout(parent)
                head = parent
            else:
                _mark_nonrun_result(res, "skip")
                yield res
                continue
        else:
            if parent != head:
                new_bases[parent] = head

        # We've adjusted base. Now skip, pick, or run the commit.

        if rerun_action == "skip-or-pick":
            if ds_repo.is_ancestor(res_hexsha, head):
                _mark_nonrun_result(res, "skip")
                if head_to_restore:
                    ds_repo.checkout(head_to_restore)
                    head, head_to_restore = head_to_restore, None
                yield res
                continue
            else:
                ds_repo.cherry_pick(res_hexsha)
                _mark_nonrun_result(res, "pick")
                yield res
        elif rerun_action == "run":
            slurm_run_info = res["slurm_run_info"]
            # Keep a "rerun" trail.
            if "chain" in slurm_run_info:
                slurm_run_info["chain"].append(res_hexsha)
            else:
                slurm_run_info["chain"] = [res_hexsha]

            # now we have to find out what was modified during the last run,
            # and enable re-modification ideally, we would bring back the
            # entire state of the tree with #1424, but we limit ourself to file
            # addition/not-in-place-modification for now
            auto_outputs = (ap["path"] for ap in new_or_modified(res["diff"]))
            outputs = slurm_run_info.get("outputs", [])
            outputs_dir = op.join(dset.path, slurm_run_info["pwd"])
            auto_outputs = [
                p
                for p in auto_outputs
                # run records outputs relative to the "pwd" field.
                if op.relpath(p, outputs_dir) not in outputs
            ]

            # remove the slurm outputs from the previous run from the outputs
            old_slurm_outputs = slurm_run_info.get("slurm_outputs", [])
            outputs = [output for output in outputs if output not in old_slurm_outputs]

            message = res["rerun_message"] or res["run_message"]
            message = check_job_pattern(message)
            for r in schedule_cmd(
                slurm_run_info["cmd"],
                dataset=dset,
                inputs=slurm_run_info.get("inputs", []),
                extra_inputs=slurm_run_info.get("extra_inputs", []),
                outputs=outputs,
                assume_ready=assume_ready,
                explicit=explicit,
                rerun_outputs=auto_outputs,
                message=message,
                jobs=jobs,
                reslurm_run_info=slurm_run_info,
            ):
                yield r
        new_head = ds_repo.get_hexsha()
        if new_head not in [head, res_hexsha]:
            new_bases[res_hexsha] = new_head
        head = new_head

    if branch_to_restore:
        # The user asked us to replay the sequence onto a branch, but the
        # history had merges, so we're in a detached state.
        ds_repo.update_ref("refs/heads/" + branch_to_restore, "HEAD")
        ds_repo.checkout(branch_to_restore)


def _get_rerun_log_msg(res):
    "Prepare log message for a rerun to summarize an action about to happen"
    msg = ""
    rerun_action = res.get("rerun_action")
    if rerun_action:
        msg += rerun_action
    if res.get("commit"):
        msg += " commit %s;" % res.get("commit")[:7]
    rerun_run_message = res.get("run_message")
    if rerun_run_message:
        if len(rerun_run_message) > 20:
            rerun_run_message = rerun_run_message[:17] + "..."
        msg += " (%s)" % rerun_run_message
    rerun_message = res.get("message")
    if rerun_message:
        msg += " " + rerun_message[0] % rerun_message[1:]
    msg = msg.lstrip()
    return msg


def _report(dset, results):
    ds_repo = dset.repo
    for res in results:
        if "slurm_run_info" in res:
            if res["status"] != "impossible":
                res["diff"] = list(res["diff"])
                # Add extra information that is useful in the report but not
                # needed for the rerun.
                out = ds_repo.format_commit("%an%x00%aI", res["commit"])
                res["author"], res["date"] = out.split("\0")
        yield res


def _get_script_handler(script, since, revision):
    """
    Generate a handler function to create a shell script for rerunning commands.

    Parameters
    ----------
    script : str
        The file path where the script will be written. If the value is "-", the script will be written to stdout.
    since : str or None
        The starting point for the `datalad rerun` command. If None, no `--since` option will be included.
    revision : str
        The revision or commit hash to be used in the `datalad rerun` command.

    Returns
    -------
    fn : function
        A function that takes a dataset and results, and writes a shell script based on the rerun commands.
    """
    ofh = sys.stdout if script.strip() == "-" else open(script, "w")

    def fn(dset, results):
        ds_repo = dset.repo
        header = """\
#!/bin/sh
#
# This file was generated by running (the equivalent of)
#
#   datalad rerun --script={script}{since} {revision}
#
# in {ds}{path}\n"""
        ofh.write(
            header.format(
                script=script,
                since="" if since is None else " --since=" + since,
                revision=ds_repo.get_hexsha(revision),
                ds="dataset {} at ".format(dset.id) if dset.id else "",
                path=dset.path,
            )
        )

        for res in results:
            if res["status"] != "ok":
                yield res
                return

            if "slurm_run_info" not in res:
                continue

            slurm_run_info = res["slurm_run_info"]
            cmd = slurm_run_info["cmd"]

            expanded_cmd = format_command(
                dset,
                cmd,
                **dict(
                    slurm_run_info, dspath=dset.path, pwd=op.join(dset.path, slurm_run_info["pwd"])
                ),
            )

            msg = res["run_message"]
            if msg == _format_cmd_shorty(expanded_cmd):
                msg = ""

            ofh.write("\n" + "".join("# " + ln for ln in msg.splitlines(True)) + "\n")
            commit_descr = ds_repo.describe(res["commit"])
            ofh.write(
                "# (record: {})\n".format(
                    commit_descr if commit_descr else res["commit"]
                )
            )

            ofh.write(expanded_cmd + "\n")
        if ofh is not sys.stdout:
            ofh.close()

        if ofh is sys.stdout:
            yield None
        else:
            yield get_status_dict(
                "slurm-reschedule",
                ds=dset,
                status="ok",
                path=script,
                message=("Script written to %s", script),
            )

    return fn


def diff_revision(dataset, revision="HEAD"):
    """Yield files that have been added or modified in `revision`.

    Parameters
    ----------
    dataset : Dataset
    revision : string, optional
        Commit-ish of interest.

    Returns
    -------
    Generator that yields AnnotatePaths instances
    """
    if dataset.repo.commit_exists(revision + "^"):
        fr = revision + "^"
    else:
        # No other commits are reachable from this revision.  Diff
        # with an empty tree instead.
        fr = PRE_INIT_COMMIT_SHA

    def changed(res):
        return res.get("action") == "diff" and res.get("state") != "clean"

    diff = dataset.diff(
        recursive=True,
        fr=fr,
        to=revision,
        result_filter=changed,
        return_type="generator",
        result_renderer="disabled",
    )
    for r in diff:
        yield r


def new_or_modified(diff_results):
    """Filter diff result records to those for new or modified files."""
    for r in diff_results:
        if r.get("type") in ("file", "symlink") and r.get("state") in [
            "added",
            "modified",
        ]:
            r.pop("status", None)
            yield r


def check_job_pattern(text):
    r"""Check if the text contains a slurm job id and remove it."""
    pattern = r"Submitted batch job \d+: Pending"
    match = re.search(pattern, text)

    if not match:
        return text

    if text == match.group(0):
        return None

    return text.replace(match.group(0), "").strip()

def parse_job_status(text):
    # Find the first line that starts with "Slurm job"
    for line in text.split('\n'):
        if line.startswith('Slurm job'):
            # Split by colon and get everything after it
            status = line.split(':')[1].strip()
            if status == "Completed":
                return False
            else:
                return True
    
    # Return None if no matching line is found
    return None
