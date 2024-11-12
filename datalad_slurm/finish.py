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
            action='append',
            doc="""Prepare this relative path to be an output file of the command. A
            value of "." means "run :command:`datalad unlock .`" (and will fail
            if some content isn't present). For any other value, if the content
            of this file is present, unlock the file. Otherwise, remove it. The
            value can also be a glob. [CMD: This option can be given more than
            once. CMD]"""),
        explicit=Parameter(
            args=("--explicit",),
            action="store_true",
            doc="""Consider the specification of inputs and outputs in the run
            record to be explicit. Don't warn if the repository is dirty, and
            only save modifications to the outputs from the original record.
            Note that when several run commits are specified, this applies to
            every one. Care should also be taken when using [CMD: --onto
            CMD][PY: `onto` PY] because checking out a new HEAD can easily fail
            when the working tree has modifications."""),
        jobs=jobs_opt,
    )

    @staticmethod
    @datasetmethod(name="finish")
    @eval_results
    def __call__(
        commit=None,
        *,
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
        if (onto is not None or branch is not None) and \
           ds_repo.get_corresponding_branch():
            yield get_status_dict(
                "run", ds=ds, status="impossible",
                message=("--%s is incompatible with adjusted branch",
                         "branch" if onto is None else "onto"))
            return

        if branch and branch in ds_repo.get_branches():
            yield get_status_dict(
                "run", ds=ds, status="error",
                message="branch '{}' already exists".format(branch))
            return

        if commit is None:
            commit = ds_repo.get_corresponding_branch() or \
                ds_repo.get_active_branch() or "HEAD"
            
        # for now, we just assume this to be run on a single commit
        revrange = "{rev}^..{rev}".format(rev=commit)

        try:
            results = _revrange_as_results(ds, revrange)
        except ValueError as exc:
            ce = CapturedException(exc)
            yield get_status_dict("run", status="error", message=str(ce),
                                  exception=ce)
            return
        
        run_message = results["run_message"]
        run_info = results["run_info"]
        # concatenate outputs from both submission and completion
        outputs_to_save = ensure_list(outputs) + results["run_info"]["outputs"]

        # should throw an error if user doesn't specify outputs or directory
        if not outputs_to_save:
            err_msg = "You must specify which outputs to save from this slurm run."
            yield get_status_dict("run", status="error", message=err_msg)
            return

        slurm_job_id = re.search(r'job (\d+):', run_message).group(1)

        job_complete = check_job_complete(slurm_job_id)
        if not job_complete:
            yield get_status_dict("run", status="error", message="Slurm job still running",
                                  exception=subprocess.CalledProcessError)
            return

        # delete the slurm_job_id file
        slurm_submission_file = f"slurm-job-submission-{slurm_job_id}"
        os.remove(slurm_submission_file)
        outputs_to_save.append(slurm_submission_file)

        #rel_pwd = rerun_info.get('pwd') if rerun_info else None
        rel_pwd = None # TODO might be able to get this from rerun info
        if rel_pwd and dataset:
            # recording is relative to the dataset
            pwd = op.normpath(op.join(dataset.path, rel_pwd))
            rel_pwd = op.relpath(pwd, dataset.path)
        else:
            pwd, rel_pwd = get_command_pwds(dataset)
    
        do_save = True
        msg = u"""\
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
            '"{}"'.format(record) if record_path else record)

        if do_save:
            with chpwd(pwd):
                for r in Save.__call__(
                        dataset=ds,
                        path=outputs_to_save,
                        recursive=True,
                        message=msg,
                        jobs=jobs,
                        return_type='generator',
                        # we want this command and its parameterization to be in full
                        # control about the rendering of results, hence we must turn
                        # off internal rendering
                        result_renderer='disabled',
                        on_failure='ignore'):
                    yield r



def _revrange_as_results(dset, revrange):
    ds_repo = dset.repo
    rev_line = ds_repo.get_revisions(
        revrange, fmt="%H %P", options=["--reverse", "--topo-order"])[0]
    if not rev_line:
        return

    # The strip() below is necessary because, with the format above, a
    # commit without any parent has a trailing space. (We could also use a
    # custom `rev-list --parents ...` call to avoid this.)
    fields = rev_line.strip().split(" ")
    rev, parents = fields[0], fields[1:]
    res = get_status_dict("run", ds=dset, commit=rev, parents=parents)
    full_msg = ds_repo.format_commit("%B", rev)
    try:
        msg, info = get_run_info(dset, full_msg)
    except ValueError as exc:
        # Recast the error so the message includes the revision.
        raise ValueError(
            "Error on {}'s message".format(rev)) from exc
    res["run_info"] = info
    res["run_message"] = msg

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

    


def get_run_info(dset, message):
    """Extract run information from `message`

    Parameters
    ----------
    message : str
        A commit message.

    Returns
    -------
    A tuple with the command's message and a dict with run information. Both
    these values are None if `message` doesn't have a run command.

    Raises
    ------
    A ValueError if the information in `message` is invalid.
    """
    cmdrun_regex = r'\[DATALAD SCHEDULE\] (.*)=== Do not change lines below ' \
                   r'===\n(.*)\n\^\^\^ Do not change lines above \^\^\^'
    runinfo = re.match(cmdrun_regex, message, re.MULTILINE | re.DOTALL)
    if not runinfo:
        return None, None

    rec_msg, runinfo = runinfo.groups()

    try:
        runinfo = json.loads(runinfo)
    except Exception as e:
        raise ValueError(
            'cannot rerun command, command specification is not valid JSON'
        ) from e
    if not isinstance(runinfo, (list, dict)):
        # this is a run record ID -> load the beast
        record_dir = dset.config.get(
            'datalad.run.record-directory',
            default=op.join('.datalad', 'runinfo'))
        record_path = op.join(dset.path, record_dir, runinfo)
        if not op.lexists(record_path):
            raise ValueError("Run record sidecar file not found: {}".format(record_path))
        # TODO `get` the file
        recs = load_stream(record_path, compressed=True)
        # TODO check if there is a record
        runinfo = next(recs)
    if 'cmd' not in runinfo:
        raise ValueError("Looks like a run commit but does not have a command")
    return rec_msg.rstrip(), runinfo

def get_command_pwds(dataset):
    """Return the current directory for the dataset.

    Parameters
    ----------
    dataset : Dataset

    Returns
    -------
    A tuple, where the first item is the absolute path of the pwd and the
    second is the pwd relative to the dataset's path.
    """
    # Follow path resolution logic describe in gh-3435.
    if isinstance(dataset, Dataset):  # Paths relative to dataset.
        pwd = dataset.path
        rel_pwd = op.curdir
    else:                             # Paths relative to current directory.
        pwd = getpwd()
        # Pass pwd to get_dataset_root instead of os.path.curdir to handle
        # repos whose leading paths have a symlinked directory (see the
        # TMPDIR="/var/tmp/sym link" test case).
        if not dataset:
            dataset = get_dataset_root(pwd)

        if dataset:
            rel_pwd = op.relpath(pwd, dataset)
        else:
            rel_pwd = pwd  # and leave handling to caller
    return pwd, rel_pwd


def _create_record(run_info, sidecar_flag, ds):
    """
    Returns
    -------
    str or None, str or None
      The first value is either the full run record in JSON serialized form,
      or content-based ID hash, if the record was written to a file. In that
      latter case, the second value is the path to the record sidecar file,
      or None otherwise.
    """
    record = json.dumps(run_info, indent=1, sort_keys=True, ensure_ascii=False)
    if sidecar_flag is None:
        use_sidecar = ds.config.get(
            'datalad.run.record-sidecar', default=False)
        use_sidecar = anything2bool(use_sidecar)
    else:
        use_sidecar = sidecar_flag

    record_id = None
    record_path = None
    if use_sidecar:
        # record ID is hash of record itself
        from hashlib import md5
        record_id = md5(record.encode('utf-8')).hexdigest()  # nosec
        record_dir = ds.config.get(
            'datalad.run.record-directory',
            default=op.join('.datalad', 'runinfo'))
        record_path = ds.pathobj / record_dir / record_id
        if not op.lexists(record_path):
            # go for compression, even for minimal records not much difference,
            # despite offset cost
            # wrap in list -- there is just one record
            dump2stream([run_info], record_path, compressed=True)
    return record_id or record, record_path

def _format_cmd_shorty(cmd):
    """Get short string representation from a cmd argument list"""
    cmd_shorty = (join_cmdline(cmd) if isinstance(cmd, list) else cmd)
    cmd_shorty = u'{}{}'.format(
        cmd_shorty[:40],
        '...' if len(cmd_shorty) > 40 else '')
    return cmd_shorty

def format_command(dset, command, **kwds):
    """Plug in placeholders in `command`.

    Parameters
    ----------
    dset : Dataset
    command : str or list

    `kwds` is passed to the `format` call. `inputs` and `outputs` are converted
    to GlobbedPaths if necessary.

    Returns
    -------
    formatted command (str)
    """
    command = normalize_command(command)
    sfmt = SequenceFormatter()

    for k, v in dset.config.items("datalad.run.substitutions"):
        sub_key = k.replace("datalad.run.substitutions.", "")
        if sub_key not in kwds:
            kwds[sub_key] = v

    for name in ["inputs", "outputs"]:
        io_val = kwds.pop(name, None)
        if not isinstance(io_val, GlobbedPaths):
            io_val = GlobbedPaths(io_val, pwd=kwds.get("pwd"))
        kwds[name] = list(map(quote_cmdlinearg, io_val.expand(dot=False)))
    return sfmt.format(command, **kwds)


def check_job_complete(job_id):
    """
    Check if a Slurm job is currently running using squeue command.
    
    Args:
        job_id: The Slurm job ID to check (can be integer or string)
        
    Returns:
        bool: True if the job is running, False if not found or completed
        
    Raises:
        subprocess.CalledProcessError: If squeue command fails
        ValueError: If job_id is not a valid integer
    """

    # Convert job_id to string and verify it's a valid integer
    job_id = str(job_id)
    if not job_id.isdigit():
        raise ValueError(f"Invalid job ID: {job_id}. Must be a positive integer.")

    # Run squeue command and capture output
    cmd = ["squeue", "-j", job_id, "-h"]  # -h removes the header line

    try:
        result = subprocess.run(cmd, 
                          capture_output=True, 
                          text=True, 
                          check=True)
        if result.stdout.strip():
            return False
        else:
            return True

    except subprocess.CalledProcessError:
        return True
