__docformat__ = "restructuredtext"

import json
import logging
import os.path as op
import re
import sys
from copy import copy
from functools import partial
from itertools import dropwhile
import sqlite3

from datalad.consts import PRE_INIT_COMMIT_SHA
from datalad.core.local.run import (
    _format_cmd_shorty,
    assume_ready_opt,
    format_command,
)
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
from datalad.support.json_py import load_stream
from datalad.support.param import Parameter

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

from datalad.core.local.run import (
    _format_cmd_shorty,
    get_command_pwds,
    _display_basic,
    prepare_inputs,
    _prep_worktree,
    format_command,
    normalize_command,
    _create_record,
    _format_iospecs,
    _get_substitutions,
)

from datalad.support.globbedpaths import GlobbedPaths


def get_finish_info(dset, message):
    """Extract finish information from `message`

    Parameters
    ----------
    message : str
        A commit message.

    Returns
    -------
    A tuple with the command's message and a dict with finish information. Both
    these values are None if `message` doesn't have a finish command.

    Raises
    ------
    A ValueError if the information in `message` is invalid.
    """
    # TODO fix the cmd_regex

    cmdrun_regex = (
        r"\[DATALAD FINISH\] (.*)=== Do not change lines below "
        r"===\n(.*)\n\^\^\^ Do not change lines above \^\^\^"
    )
    runinfo = re.match(cmdrun_regex, message, re.MULTILINE | re.DOTALL)
    if not runinfo:
        return None, None

    rec_msg, runinfo = runinfo.groups()

    try:
        runinfo = json.loads(runinfo)
    except Exception as e:
        raise ValueError(
            "cannot rerun command, command specification is not valid JSON"
        ) from e
    if not isinstance(runinfo, (list, dict)):
        # this is a run record ID -> load the beast
        record_dir = dset.config.get(
            "datalad.run.record-directory", default=op.join(".datalad", "runinfo")
        )
        record_path = op.join(dset.path, record_dir, runinfo)
        if not op.lexists(record_path):
            raise ValueError(
                "Run record sidecar file not found: {}".format(record_path)
            )
        # TODO `get` the file
        recs = load_stream(record_path, compressed=True)
        # TODO check if there is a record
        runinfo = next(recs)
    if "cmd" not in runinfo:
        raise ValueError("Looks like a finish commit but does not have a command")
    return rec_msg.rstrip(), runinfo

def get_schedule_info(dset, message, allow_reschedule=True):
    """Extract (re)schedule information from `message`

    Parameters
    ----------
    message : str
        A commit message.

    Returns
    -------
    A tuple with the command's message and a dict with schedule information. Both
    these values are None if `message` doesn't have a schedule command.

    Raises
    ------
    A ValueError if the information in `message` is invalid.
    """
    # sometimes this operates on schedule or reschedule, sometimes only schedule
    if allow_reschedule:
        cmdrun_regex = (
            r"\[DATALAD (?:SCHEDULE|RESCHEDULE)\] (.*)=== Do not change lines below "
            r"===\n(.*)\n\^\^\^ Do not change lines above \^\^\^"
        )
    else:
        cmdrun_regex = (
            r"\[DATALAD SCHEDULE\] (.*)=== Do not change lines below "
            r"===\n(.*)\n\^\^\^ Do not change lines above \^\^\^"
        )
    runinfo = re.match(cmdrun_regex, message, re.MULTILINE | re.DOTALL)
    if not runinfo:
        return None, None

    rec_msg, runinfo = runinfo.groups()

    try:
        runinfo = json.loads(runinfo)
    except Exception as e:
        raise ValueError(
            "cannot rerun command, command specification is not valid JSON"
        ) from e
    if not isinstance(runinfo, (list, dict)):
        # this is a run record ID -> load the beast
        record_dir = dset.config.get(
            "datalad.run.record-directory", default=op.join(".datalad", "runinfo")
        )
        record_path = op.join(dset.path, record_dir, runinfo)
        if not op.lexists(record_path):
            raise ValueError(
                "Run record sidecar file not found: {}".format(record_path)
            )
        # TODO `get` the file
        recs = load_stream(record_path, compressed=True)
        # TODO check if there is a record
        runinfo = next(recs)
    if "cmd" not in runinfo:
        raise ValueError("Looks like a (re)schedule commit but does not have a command")
    return rec_msg.rstrip(), runinfo

def get_slurm_job_id(dset, revision, allow_reschedule=True):
    revrange = "{rev}^..{rev}".format(rev=revision)
    ds_repo = dset.repo
    rev_line = ds_repo.get_revisions(
        revrange, fmt="%H %P", options=["--reverse", "--topo-order"]
    )[0]
    if not rev_line:
        return
    fields = rev_line.strip().split(" ")
    rev, parents = fields[0], fields[1:]
    res = get_status_dict("run", ds=dset, commit=rev, parents=parents)
    full_msg = ds_repo.format_commit("%B", rev)
    try:
        msg, info = get_schedule_info(dset, full_msg, allow_reschedule=allow_reschedule)
        if msg is None or info is None:
            return
    except ValueError as exc:
        # Recast the error so the message includes the revision.
        raise ValueError("Error on {}'s message".format(rev)) from exc

    return info["slurm_job_id"]

def check_finish_exists(dset, revision, rev_branch, allow_reschedule=True):
    # now check the finish exists
    ds_repo = dset.repo

    db_name = f"{dset.id}_{rev_branch}.db"
    db_path = dset.pathobj / ".git" / db_name
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    
    # check the open jobs for the commit
    cur.execute("SELECT 1 FROM open_jobs WHERE commit_id LIKE ?", (revision + "%",))
    finish_exists = cur.fetchone() is None
    
    con.close()
    return finish_exists
    
def extract_incomplete_jobs(dset):
    """
    Get the number of incomplete jobs, (re)-scheduled jobs with no finish command.

    Finds the most recent finish or (re)-scheduled job. If none is found, returns 0.
    Used to make datalad finish more efficient.
    """
    ds_repo = dset.repo
    revrange = ds_repo.get_corresponding_branch() or ds_repo.get_active_branch() or "HEAD"
    rev_lines = ds_repo.get_revisions(
        revrange, fmt="%H %P", options=["--topo-order"]
    )
    if not rev_lines:
        return 0

    for rev_line in rev_lines:
        # The strip() below is necessary because, with the format above, a
        # commit without any parent has a trailing space. (We could also use a
        # custom `rev-list --parents ...` call to avoid this.)
        fields = rev_line.strip().split(" ")
        rev, parents = fields[0], fields[1:]
        full_msg = ds_repo.format_commit("%B", rev)
        # see if we get a hit on a finish command
        msg, info = get_finish_info(dset, full_msg)
        if msg and info:
            try:
                return info["incomplete_job_number"]
            except KeyError:
                return 0
        # see if we get a hit on a (re)schedule command
        msg, info = get_schedule_info(dset, full_msg)
        if msg and info:
            try:
                return info["incomplete_job_number"]
            except KeyError:
                return 0
    return 0

