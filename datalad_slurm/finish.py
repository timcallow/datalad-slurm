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
import sqlite3

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

from .common import connect_to_database

from datalad.core.local.run import _create_record, get_command_pwds

lgr = logging.getLogger("datalad.slurm.finish")


class Finish(Interface):
    """Finishes (i.e. saves outputs) a slurm submitted job."""

    _params_ = dict(
        slurm_job_id=Parameter(
            args=("--slurm-job-id",),
            nargs="?",
            doc="""Finishes the slurm job from the specified slurm job id.""",
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
        close_failed_jobs=Parameter(
            args=("--close-failed-jobs",),
            action="store_true",
            doc="""Close any jobs which failed or were cancelled.
            Note that pending or running jobs will never be closed.
            They first have to be cancelled with `scancel`. """,
        ),
        list_open_jobs=Parameter(
            args=("--list-open-jobs",),
            action="store_true",
            doc="""List all open scheduled jobs (those which haven't been finished).""",
        ),
        jobs=jobs_opt,
    )

    @staticmethod
    @datasetmethod(name="finish")
    @eval_results
    def __call__(
        slurm_job_id=None,
        *,
        dataset=None,
        message=None,
        outputs=None,
        onto=None,
        explicit=True,
        close_failed_jobs=False,
        list_open_jobs=False,
        branch=None,
        jobs=None,
    ):
        ds = require_dataset(
            dataset, check_installed=True, purpose="finish a SLURM job"
        )
        ds_repo = ds.repo
        if slurm_job_id:
            slurm_job_id_list = [slurm_job_id]
        else:
            slurm_job_id_list, status_ok = get_scheduled_commits(
                ds, branch
            )
            if not status_ok:
                yield get_status_dict(
                    "finish",
                    ds=ds,
                    status="error",
                    message=("Database connection cannot be established"),
                )
                return                                    

        # list the open jobs if requested
        # if a single commit was specified, nothing happens
        # TODO: code with triple list and multiple prints is a bit ugly, consider refactor
        if list_open_jobs:
            if slurm_job_id_list:
                print("The following jobs are open: \n")
                print(f"{'slurm-job-id':<14} {'slurm-job-status'}")
                for i, slurm_job_id in enumerate(slurm_job_id_list):
                    job_status = get_job_status(slurm_job_id)[1]
                    print(
                        f"{slurm_job_id:<10} {job_status}"
                    )
            return
        for slurm_job_id in slurm_job_id_list:
            for r in finish_cmd(
                slurm_job_id,
                dataset=dataset,
                message=message,
                outputs=outputs,
                onto=None,
                explicit=explicit,
                close_failed_jobs=close_failed_jobs,
                branch=None,
                jobs=None,
            ):
                yield r


def get_scheduled_commits(dset, branch):
    """Return the slurm job ids of all open jobs."""
    # connect to the database
    con, cur = connect_to_database(dset, row_factory=True)
    if not con or not cur:
        return None, None

    # select the slurm job ids into a list
    cur.execute("SELECT slurm_job_id FROM open_jobs")
    slurm_job_ids = cur.fetchall()

    return slurm_job_ids, True

def finish_cmd(
    slurm_job_id,
    dataset=None,
    message=None,
    outputs=None,
    onto=None,
    explicit=True,
    close_failed_jobs=False,
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

    results = extract_from_db(ds, slurm_job_id)

    if not results:
        yield get_status_dict(
            "finish",
            status="error",
            message="Error accessing slurm job {} in database".format(
                commit[:7]
            ),
        )
        return

    run_message = results["run_message"]
    run_info = results["run_info"]
    # concatenate outputs from both submission and completion
    outputs_to_save = ensure_list(outputs) + ensure_list(run_info["outputs"])

    # should throw an error if user doesn't specify outputs or directory
    if not outputs_to_save:
        err_msg = "You must specify which outputs to save from this slurm run."
        yield get_status_dict("run", status="error", message=err_msg)
        return

    slurm_job_id = run_info["slurm_job_id"]

    # get a list of job ids and status (if we have an array job)
    job_states, job_status_group = get_job_status(slurm_job_id)

    status_text = "Job ID: Job state \n"
    for job_id, state in job_states.items():
        status_text += f"{job_id}: {state} \n"

    # process these job ids and job statuses
    if not all(status == "COMPLETED" for status in job_states.values()):
        status_summary = ", ".join(
            f"{job_id}: {status}" for job_id, status in job_states.items()
        )
        message = f"Slurm job(s) for job {slurm_job_id} are not complete. Statuses: {status_summary}"
        if any(
            status in ["PENDING", "RUNNING"] for status in job_states.values()
        ):
            yield get_status_dict("finish", status="error", message=message)
            return
        else:
            if not close_failed_jobs:
                yield get_status_dict("finish", status="error", message=message)
                return
            else:
                if job_status_group != "PARTIALLY COMPLETED":
                    # remove the job
                    status = remove_from_database(ds, run_info)
                    message = f"Closing failed / cancelled jobs. Statuses: {status_summary}"
                    yield get_status_dict("finish", status="ok", message=message)
                    return            
        
    # if array job is partially succesful then we close it succesfully
    if job_status_group == "PARTIALLY COMPLETED":
        # TODO path
        array_filename = f"array-job-info-{slurm_job_id}.out"
        yield get_status_dict(
            "finish",
            status="ok",
            message=f"Some array jobs failed / cancelled."
            f" Job breakdown is wrriten to {array_filename}.",
        )
        with open(array_filename, "w") as f:
            f.write(status_text)
        outputs_to_save.append(array_filename)

    # expand the wildcards
    globbed_outputs = GlobbedPaths(outputs_to_save, expand=True).paths

    # update the run info with the new outputs
    run_info["outputs"] = globbed_outputs

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
[DATALAD SLURM RUN] {}

=== Do not change lines below ===
{}
^^^ Do not change lines above ^^^
        """
    job_status_group = job_status_group.capitalize()
    message_entry = f"Slurm job {slurm_job_id}: {job_status_group}"
    
    # Add the user messages from schedule and finish
    if message:
        message_entry += f"\n\n{message}"
    if run_message:
        message_entry += f"\n\n{run_message}"

    # create the run record, either as a string, or written to a file
    # depending on the config/request
    # TODO sidecar param
    record, record_path = _create_record(run_info, False, ds)

    msg = msg.format(
        message_entry if message_entry is not None else cmd_shorty,
        '"{}"'.format(record) if record_path else record,
    )

    # remove the job
    status = remove_from_database(ds, run_info)

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

def extract_from_db(dset, slurm_job_id):
    """Extract the run info from the database entry."""
    con, cur = connect_to_database(dset)

    # select all columns
    query = "SELECT * FROM open_jobs WHERE slurm_job_id = ?"
    cur.execute(query, (slurm_job_id,))
    
    # Fetch the record
    record = cur.fetchone()
    
    if not record:
        return None

    # Get column names from cursor description
    column_names = [desc[0] for desc in cur.description]

    # extract as dictionary
    run_info = dict(zip(column_names, record))


    # convert json columns to list
    json_columns = ["chain", "inputs", "extra_inputs", "outputs", "slurm_outputs"]

    for column in json_columns:
        run_info[column] = json.loads(run_info[column])
    
    message = run_info["message"]
    del run_info["message"]

    res = {"run_message": message, "run_info": run_info}
    
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
        # -o JobID,State: output job ID and state columns
        # --parsable2: machine-friendly output format with | delimiter
        result = subprocess.run(
            ["sacct", "-n", "-X", "-j", job_id, "-o", "JobID,State", "--parsable2"],
            capture_output=True,
            text=True,
            check=True,
        )
        # Get the output lines
        output = result.stdout.strip()
        # If there's no output, the job doesn't exist
        if not output:
            raise ValueError(f"Job {job_id} not found")

        # Create dictionary of job_id: state pairs
        job_states = {}
        for line in output.splitlines():
            job_id, state = line.split("|")
            if "CANCELLED" in state:
                state = "CANCELLED"
            job_states[job_id] = state

        unique_statuses = set(job_states.values())
        if len(unique_statuses) == 1:
            job_status_group = unique_statuses.pop()  # Get the single status value
        else:
            job_status_group = "PARTIALLY COMPLETED"

        return job_states, job_status_group

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error running sacct command: {e.stderr}")

def remove_from_database(dset, run_info):
    """Remove a job from the database based on its slurm_job_id."""
    con, cur = connect_to_database(dset)
    
    # Remove the row matching the slurm_job_id
    cur.execute("""
    DELETE FROM open_jobs 
    WHERE slurm_job_id = ?
    """, (run_info["slurm_job_id"],))
    
    con.commit()
    con.close()
    return "ok"
