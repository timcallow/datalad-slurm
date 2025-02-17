__docformat__ = "restructuredtext"

import json
import os.path as op
import re
import sqlite3

from datalad.support.json_py import load_stream


def get_finish_info(dset, message):
   """
   Extract information about a finished slurm job from its commit message.

   Parameters
   ----------
   dset : Dataset
       Dataset object containing the run record
   message : str
       A commit message

   Returns
   -------
   tuple
       (str or None, dict or None)
       - str: Command message if found, None otherwise
       - dict: Finish information if found, None otherwise

   Raises
   ------
   ValueError
       If message contains invalid JSON or missing command information
   """
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


def connect_to_database(dset, row_factory=False):
   """
   Connect to sqlite3 database and return the connection and cursor.

   Parameters
   ----------
   dset : Dataset
       Dataset object with repo and path information
   row_factory : bool, optional
       If True, return single-column results as scalars instead of tuples, default False

   Returns
   -------
   tuple
       (sqlite3.Connection or None, sqlite3.Cursor or None)
       Connection and cursor objects if successful, (None, None) if connection fails

   Notes
   -----
   Database path is constructed from dataset ID and branch in .git directory
   """
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
