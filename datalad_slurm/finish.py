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
    )

    @staticmethod
    @datasetmethod(name="finish")
    @eval_results
    def __call__(
        commit=None,
        *,
        dataset=None,
        message=None,
        onto=None,
        explicit=False,
        branch=None,
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



def _revrange_as_results(dset, revrange):
    ds_repo = dset.repo
    rev_lines = ds_repo.get_revisions(
        revrange, fmt="%H %P", options=["--reverse", "--topo-order"])
    if not rev_lines:
        return

    for rev_line in rev_lines:
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

        if info is not None:
            if len(parents) != 1:
                lgr.warning(
                    "%s has run information but is a %s commit; "
                    "it will not be re-executed",
                    rev,
                    "merge" if len(parents) > 1 else "root")
                continue
            res["run_info"] = info
            res["run_message"] = msg
        yield dict(res, status="ok")        
