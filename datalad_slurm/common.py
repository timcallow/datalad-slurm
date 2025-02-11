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
        r"\[DATALAD SLURM RUN\] (.*)=== Do not change lines below "
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


def check_finish_exists(dset, revision, rev_branch, allow_reschedule=True):
    """Check if a job is open or already finished."""
    # connect to the database
    con, cur = connect_to_database(dset)
    if con is None or cur is None:
        return None, None

    # check the open jobs for the commit
    cur.execute("SELECT 1 FROM open_jobs WHERE commit_id LIKE ?", (revision + "%",))
    finish_exists = cur.fetchone() is None
    con.close()

    return finish_exists, True

def connect_to_database(dset, row_factory=False):
    """Connect to sqlite3 database and return the connection and cursor."""
    # define the database path from the dataset and branch
    ds_repo = dset.repo
    branch = ds_repo.get_corresponding_branch() or ds_repo.get_active_branch() or "HEAD"
    db_name = f"{dset.id}_{branch}.db"
    db_path = dset.pathobj / ".git" / db_name
    
    # try to connect to the database
    try:
        con = sqlite3.connect(db_path)
        if row_factory:
            con.row_factory = lambda cursor, row: row[0]
        cur = con.cursor()
    except sqlite3.Error:
        return None, None
    
    return con, cur
            
