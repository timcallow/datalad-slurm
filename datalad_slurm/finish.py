"""Finish a scheduled slurm command"""

__docformat__ = "restructuredtext"

import json
import logging
import os
import subprocess
import re
import os.path as op
import warnings
from argparse import REMAINDER
from pathlib import Path
from tempfile import mkdtemp
import glob

import datalad
import datalad.support.ansi_colors as ac
from datalad.config import anything2bool
from datalad.core.local.save import Save
from datalad.core.local.status import Status
from datalad.distribution.dataset import (
    Dataset,
    EnsureDataset,
    datasetmethod,
    require_dataset,
)
from datalad.distribution.get import Get
from datalad.distribution.install import Install
from datalad.interface.base import (
    Interface,
    build_doc,
    eval_results,
)
from datalad.interface.common_opts import (
    jobs_opt,
    save_message_opt,
)
from datalad.interface.results import get_status_dict
from datalad.interface.utils import generic_result_renderer
from datalad.local.unlock import Unlock
from datalad.support.constraints import (
    EnsureBool,
    EnsureChoice,
    EnsureNone,
    EnsureStr,
)
from datalad.support.exceptions import (
    CapturedException,
    CommandError,
)
from datalad.support.globbedpaths import GlobbedPaths
from datalad.support.json_py import dump2stream
from datalad.support.param import Parameter
from datalad.ui import ui
from datalad.utils import (
    SequenceFormatter,
    chpwd,
    ensure_list,
    ensure_unicode,
    get_dataset_root,
    getpwd,
    join_cmdline,
    quote_cmdlinearg,
)

from .common import check_finish_exists, get_schedule_info

from datalad.core.local.run import _create_record, get_command_pwds

lgr = logging.getLogger("datalad.slurm.finish")


class Finish(Interface):
    """Finishes (i.e. saves outputs) a slurm submitted job."""

    _params_ = dict(
        commit=Parameter(
            args=("commit",),
            metavar="COMMIT",
            nargs="?",
            doc=""" `commit`. Finishes the slurm job from the specified commit.""",
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
        branch=Parameter(
            metavar="NAME",
            args=(
                "-b",
                "--branch",
            ),
            doc="create and checkout this branch before rerunning the commands.",
            constraints=EnsureStr() | EnsureNone(),
        ),
        onto=Parameter(
            metavar="base",
            args=("--onto",),
            doc="""start point for rerunning the commands. If not specified,
            commands are executed at HEAD. This option can be used to specify
            an alternative start point, which will be checked out with the
            branch name specified by [CMD: --branch CMD][PY: `branch` PY] or in
            a detached state otherwise. As a special case, an empty value for
            this option means the parent of the first run commit in the
            specified revision list.""",
            constraints=EnsureStr() | EnsureNone(),
        ),
        message=Parameter(
            args=(
                "-m",
                "--message",
            ),
            metavar="MESSAGE",
            doc="""Message for the finished `commit`.""",
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
        outputs=Parameter(
            args=("-o", "--output"),
            dest="outputs",
            metavar=("PATH"),
            action="append",
            doc="""Prepare this relative path to be an output file of the command. A
            value of "." means "run :command:`datalad unlock .`" (and will fail
            if some content isn't present). For any other value, if the content
            of this file is present, unlock the file. Otherwise, remove it. The
            value can also be a glob. [CMD: This option can be given more than
            once. CMD]""",
        ),
        explicit=Parameter(
            args=("--explicit",),
            action="store_true",
            doc="""Consider the specification of inputs and outputs in the run
            record to be explicit. Don't warn if the repository is dirty, and
            only save modifications to the outputs from the original record.
            Note that when several run commits are specified, this applies to
            every one. Care should also be taken when using [CMD: --onto
            CMD][PY: `onto` PY] because checking out a new HEAD can easily fail
            when the working tree has modifications.""",
        ),
        jobs=jobs_opt,
    )

    @staticmethod
    @datasetmethod(name="finish")
    @eval_results
    def __call__(
        commit=None,
        *,
        since=None,
        dataset=None,
        message=None,
        outputs=None,
        onto=None,
        explicit=True,
        branch=None,
        jobs=None,
    ):
        ds = require_dataset(
            dataset, check_installed=True, purpose="finish a SLURM job"
        )
        ds_repo = ds.repo
        if since is None:
            if commit:
                commit_list = [commit]
            else:
                commit_list = get_scheduled_commits("", ds, branch)
        else:
            commit_list = get_scheduled_commits(since, ds, branch)
        for commit_element in commit_list:
            for r in finish_cmd(
                commit_element,
                since=since,
                dataset=dataset,
                message=message,
                outputs=outputs,
                onto=None,
                explicit=explicit,
                branch=None,
                jobs=None,
            ):
                yield r


def get_scheduled_commits(since, dset, branch):
    ds_repo = dset.repo
    # get branch
    rev_branch = (
        ds_repo.get_corresponding_branch() or ds_repo.get_active_branch() or "HEAD"
    )
    revision = rev_branch
    if since.strip() == "":
        revrange = revision
    else:
        revrange = "{}..{}".format(since, revision)

    rev_lines = ds_repo.get_revisions(
        revrange, fmt="%H %P", options=["--reverse", "--topo-order"]
    )
    if not rev_lines:
        return

    commit_list = []
    for rev_line in rev_lines:
        # The strip() below is necessary because, with the format above, a
        # commit without any parent has a trailing space. (We could also use a
        # custom `rev-list --parents ...` call to avoid this.)
        fields = rev_line.strip().split(" ")
        rev, parents = fields[0], fields[1:]
        res = get_status_dict("run", ds=dset, commit=rev, parents=parents)
        full_msg = ds_repo.format_commit("%B", rev)
        try:
            msg, info = get_schedule_info(dset, full_msg)
            if msg and info:
                # then we have a hit on the schedule
                # check if a corresponding finish command exists
                job_finished = check_finish_exists(dset, rev, rev_branch)
                if not job_finished:
                    commit_list.append(rev)
        except ValueError as exc:
            # Recast the error so the message includes the revision.
            raise ValueError("Error on {}'s message".format(rev)) from exc

    return commit_list


def finish_cmd(
    commit,
    since=None,
    dataset=None,
    message=None,
    outputs=None,
    onto=None,
    explicit=True,
    branch=None,
    jobs=None,
):

    ds = require_dataset(dataset, check_installed=True, purpose="finish a SLURM job")
    ds_repo = ds.repo

    lgr.debug("rerunning command output underneath %s", ds)

    if not explicit:
        yield get_status_dict(
            "run",
            ds=ds,
            status="impossible",
            message=(
                "clean dataset required to detect changes from command; "
                "use `datalad status` to inspect unsaved changes"
            ),
        )
        return

    if not ds_repo.get_hexsha():
        yield get_status_dict(
            "run",
            ds=ds,
            status="impossible",
            message="cannot rerun command, nothing recorded",
        )
        return

    # ATTN: Use get_corresponding_branch() rather than is_managed_branch()
    # for compatibility with a plain GitRepo.
    if (onto is not None or branch is not None) and ds_repo.get_corresponding_branch():
        yield get_status_dict(
            "run",
            ds=ds,
            status="impossible",
            message=(
                "--%s is incompatible with adjusted branch",
                "branch" if onto is None else "onto",
            ),
        )
        return

    if branch and branch in ds_repo.get_branches():
        yield get_status_dict(
            "run",
            ds=ds,
            status="error",
            message="branch '{}' already exists".format(branch),
        )
        return

    if commit is None:
        commit = (
            ds_repo.get_corresponding_branch() or ds_repo.get_active_branch() or "HEAD"
        )

    # for now, we just assume this to be run on a single commit
    revrange = "{rev}^..{rev}".format(rev=commit)

    results = _revrange_as_results(ds, revrange)
    if not results:
        yield get_status_dict("finish", status="error", message="The commit message {} is in an incorrect format and cannot be parsed".format(commit[:7]))
        return

    run_message = results["run_message"]
    run_info = results["run_info"]
    # concatenate outputs from both submission and completion
    outputs_to_save = ensure_list(outputs) + ensure_list(results["run_info"]["outputs"])

    # should throw an error if user doesn't specify outputs or directory
    if not outputs_to_save:
        err_msg = "You must specify which outputs to save from this slurm run."
        yield get_status_dict("run", status="error", message=err_msg)
        return

    slurm_job_id = results["run_info"]["slurm_job_id"]

    job_status = get_job_status(slurm_job_id)
    if job_status != "COMPLETED":
        message = f"Slurm job for commit {commit[:7]} is not complete. Status is {job_status}."
        yield get_status_dict("finish", status="error", message=message)
        return

    # delete the slurm_job_id file
    # slurm_submission_file = f"slurm-job-submission-{slurm_job_id}"
    # os.remove(slurm_submission_file)

    # expand the wildcards
    # TODO do this in a better way with GlobbedPaths
    globbed_outputs = []
    for k in outputs_to_save:
        globbed_outputs.extend(glob.glob(k))
    # globbed_outputs.append(slurm_submission_file)

    # TODO: this is not saving model files (outputs from first job) for some reason
    # rel_pwd = rerun_info.get('pwd') if rerun_info else None
    rel_pwd = None  # TODO might be able to get this from rerun info
    if rel_pwd and dataset:
        # recording is relative to the dataset
        pwd = op.normpath(op.join(dataset.path, rel_pwd))
        rel_pwd = op.relpath(pwd, dataset.path)
    else:
        pwd, rel_pwd = get_command_pwds(dataset)

    do_save = True
    msg = """\
[DATALAD FINISH] {}

=== Do not change lines below ===
{}
^^^ Do not change lines above ^^^
        """
    message = f"Processed batch job {slurm_job_id}: Complete"

    # create the run record, either as a string, or written to a file
    # depending on the config/request
    # TODO sidecar param
    record, record_path = _create_record(run_info, False, ds)

    msg = msg.format(
        message if message is not None else cmd_shorty,
        '"{}"'.format(record) if record_path else record,
    )

    if do_save:
        with chpwd(pwd):
            for r in Save.__call__(
                dataset=ds,
                path=globbed_outputs,
                recursive=True,
                message=msg,
                jobs=jobs,
                return_type="generator",
                # we want this command and its parameterization to be in full
                # control about the rendering of results, hence we must turn
                # off internal rendering
                result_renderer="disabled",
                on_failure="ignore",
            ):
                yield r


def _revrange_as_results(dset, revrange):
    ds_repo = dset.repo
    rev_line = ds_repo.get_revisions(
        revrange, fmt="%H %P", options=["--reverse", "--topo-order"]
    )[0]
    if not rev_line:
        return

    # The strip() below is necessary because, with the format above, a
    # commit without any parent has a trailing space. (We could also use a
    # custom `rev-list --parents ...` call to avoid this.)
    fields = rev_line.strip().split(" ")
    rev, parents = fields[0], fields[1:]
    res = get_status_dict("finish", ds=dset, commit=rev, parents=parents)
    full_msg = ds_repo.format_commit("%B", rev)
    msg, info = get_schedule_info(dset, full_msg)
    if msg is None or info is None:
        return
    res["run_info"] = info
    res["run_message"] = msg

    # TODO - what is happening here?
    # if info is not None:
    #     if len(parents) != 1:
    #         lgr.warning(
    #             "%s has run information but is a %s commit; "
    #             "it will not be re-executed",
    #             rev,
    #             "merge" if len(parents) > 1 else "root")
    #         continue
    #     res["run_info"] = info
    #     res["run_message"] = msg
    return dict(res, status="ok")


def get_job_status(job_id):
    """
    Check the status of a Slurm job using sacct command.

    Args:
        job_id (Union[str, int]): The Slurm job ID to check

    Returns:
        str: "COMPLETED" if the job completed successfully,
             otherwise returns the actual job state (e.g., "RUNNING", "FAILED", "PENDING")

    Raises:
        subprocess.CalledProcessError: If the sacct command fails
        ValueError: If the job_id is invalid or job not found
    """
    # Convert job_id to string if it's an integer
    job_id = str(job_id)

    # Validate job_id format (should be a positive integer)
    if not job_id.isdigit():
        raise ValueError(
            f"Invalid job ID: {job_id}. Job ID must be a positive integer."
        )

    try:
        # Run sacct command to get job status
        # -n: no header
        # -X: no step info
        # -j: specify job ID
        # -o State: only output the state
        # --parsable2: machine-friendly output format
        result = subprocess.run(
            ["sacct", "-n", "-X", "-j", job_id, "-o", "State", "--parsable2"],
            capture_output=True,
            text=True,
            check=True,
        )

        # Get the state from the output
        state = result.stdout.strip()

        # If there's no output, the job doesn't exist
        if not state:
            raise ValueError(f"Job {job_id} not found")

        # Return the state as is
        return state

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error running sacct command: {e.stderr}")
