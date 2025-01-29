# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 et:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Schedule a slurm command"""

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

from .common import check_finish_exists, connect_to_database

lgr = logging.getLogger("datalad.slurm.schedule")

assume_ready_opt = Parameter(
    args=("--assume-ready",),
    constraints=EnsureChoice(None, "inputs", "outputs", "both"),
    doc="""Assume that inputs do not need to be retrieved and/or outputs do not
    need to unlocked or removed before running the command. This option allows
    you to avoid the expense of these preparation steps if you know that they
    are unnecessary.""",
)


@build_doc
class Schedule(Interface):
    """Schedule a slurm script to be run and record it in the git history.

    It is recommended to craft the command such that it can run in the root
    directory of the dataset that the command will be recorded in. However,
    as long as the command is executed somewhere underneath the dataset root,
    the exact location will be recorded relative to the dataset root.

    If the executed command did not alter the dataset in any way, no record of
    the command execution is made.

    If the given command errors, a `CommandError` exception with the same exit
    code will be raised, and no modifications will be saved. A command
    execution will not be attempted, by default, when an error occurred during
    input or output preparation. This default ``stop`` behavior can be
    overridden via [CMD: --on-failure ... CMD][PY: `on_failure=...` PY].

    In the presence of subdatasets, the full dataset hierarchy will be checked
    for unsaved changes prior command execution, and changes in any dataset
    will be saved after execution. Any modification of subdatasets is also
    saved in their respective superdatasets to capture a comprehensive record
    of the entire dataset hierarchy state. The associated provenance record is
    duplicated in each modified (sub)dataset, although only being fully
    interpretable and re-executable in the actual top-level superdataset. For
    this reason the provenance record contains the dataset ID of that
    superdataset.

    *Command format*

    || REFLOW >>
    A few placeholders are supported in the command via Python format
    specification. "{pwd}" will be replaced with the full path of the current
    working directory. "{dspath}" will be replaced with the full path of the
    dataset that run is invoked on. "{tmpdir}" will be replaced with the full
    path of a temporary directory. "{inputs}" and "{outputs}" represent the
    values specified by [CMD: --input and --output CMD][PY: `inputs` and
    `outputs` PY]. If multiple values are specified, the values will be joined
    by a space. The order of the values will match that order from the command
    line, with any globs expanded in alphabetical order (like bash). Individual
    values can be accessed with an integer index (e.g., "{inputs[0]}").
    << REFLOW ||

    || REFLOW >>
    Note that the representation of the inputs or outputs in the formatted
    command string depends on whether the command is given as a list of
    arguments or as a string[CMD:  (quotes surrounding the command) CMD]. The
    concatenated list of inputs or outputs will be surrounded by quotes when
    the command is given as a list but not when it is given as a string. This
    means that the string form is required if you need to pass each input as a
    separate argument to a preceding script (i.e., write the command as
    "./script {inputs}", quotes included). The string form should also be used
    if the input or output paths contain spaces or other characters that need
    to be escaped.
    << REFLOW ||

    To escape a brace character, double it (i.e., "{{" or "}}").

    Custom placeholders can be added as configuration variables under
    "datalad.run.substitutions".  As an example:

      Add a placeholder "name" with the value "joe"::

        % datalad configuration --scope branch set datalad.run.substitutions.name=joe
        % datalad save -m "Configure name placeholder" .datalad/config

      Access the new placeholder in a command::

        % datalad run "echo my name is {name} >me"
    """

    result_renderer = "tailored"
    # make run stop immediately on non-success results.
    # this prevents command execution after failure to obtain inputs of prepare
    # outputs. but it can be overriding via the common 'on_failure' parameter
    # if needed.
    on_failure = "stop"

    _params_ = dict(
        cmd=Parameter(
            args=("cmd",),
            nargs=REMAINDER,
            metavar="COMMAND",
            doc="""command for execution. A leading '--' can be used to
            disambiguate this command from the preceding options to
            DataLad.""",
        ),
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""specify the dataset to record the command results in.
            An attempt is made to identify the dataset based on the current
            working directory. If a dataset is given, the command will be
            executed in the root directory of this dataset.""",
            constraints=EnsureDataset() | EnsureNone(),
        ),
        inputs=Parameter(
            args=("-i", "--input"),
            dest="inputs",
            metavar=("PATH"),
            action="append",
            doc="""A dependency for the run. Before running the command, the
            content for this relative path will be retrieved. A value of "." means "run
            :command:`datalad get .`". The value can also be a glob. [CMD: This
            option can be given more than once. CMD]""",
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
            value can also be a glob if --allow-wildcard-outputs is enabled.
            N.B.: If outputs contain wildcards, it is esssential to enclose them in
            quotations, e.g. -o "file*.txt", NOT -o file*.txt. This is so that wildcard
            expansion is performed inside datalad-slurm and not on the command line.
            [CMD: This option can be given more than once. CMD]""",
        ),
        expand=Parameter(
            args=("--expand",),
            doc="""Expand globs when storing inputs and/or outputs in the
            commit message.""",
            constraints=EnsureChoice(None, "inputs", "outputs", "both"),
        ),
        allow_wildcard_outputs=Parameter(
            args=("--allow-wildcard-outputs",),
            action="store_true",
            doc="""Allow outputs to contain wildcard entries.
            This is disabled by default because the outputs cannot be expanded
            at submission time if the files don't exist yet.
            If enabled, no expansion of wildcards is performed, and only the raw
            string is checked against raw strings from previous schedule commands.
            So take care to ensure proper output definitions with this argument enabled.""",
        ),
        assume_ready=assume_ready_opt,
        message=save_message_opt,
        check_outputs=Parameter(
            args=("--check-outputs",),
            doc="""Check previous scheduled commits to see if there is any overlap in the outputs.""",
            constraints=EnsureNone() | EnsureBool(),
        ),
        sidecar=Parameter(
            args=("--sidecar",),
            metavar="{yes|no}",
            doc="""By default, the configuration variable
            'datalad.run.record-sidecar' determines whether a record with
            information on a command's execution is placed into a separate
            record file instead of the commit message (default: off). This
            option can be used to override the configured behavior on a
            case-by-case basis. Sidecar files are placed into the dataset's
            '.datalad/runinfo' directory (customizable via the
            'datalad.run.record-directory' configuration variable).""",
            constraints=EnsureNone() | EnsureBool(),
        ),
        dry_run=Parameter(
            # Leave out common -n short flag to avoid confusion with
            # `containers-run [-n|--container-name]`.
            args=("--dry-run",),
            doc="""Do not run the command; just display details about the
            command execution. A value of "basic" reports a few important
            details about the execution, including the expanded command and
            expanded inputs and outputs. "command" displays the expanded
            command only. Note that input and output globs underneath an
            uninstalled dataset will be left unexpanded because no subdatasets
            will be installed for a dry run.""",
            constraints=EnsureChoice(None, "basic", "command"),
        ),
        jobs=jobs_opt,
    )
    _params_[
        "jobs"
    ]._doc += """\
        NOTE: This option can only parallelize input retrieval (get) and output
        recording (save). DataLad does NOT parallelize your scripts for you.
    """

    @staticmethod
    @datasetmethod(name="run")
    @eval_results
    def __call__(
        cmd=None,
        *,
        dataset=None,
        inputs=None,
        outputs=None,
        expand=None,
        assume_ready=None,
        message=None,
        check_outputs=True,
        allow_wildcard_outputs=False,
        dry_run=None,
        jobs=None,
    ):
        for r in run_command(
            cmd,
            dataset=dataset,
            inputs=inputs,
            outputs=outputs,
            expand=expand,
            assume_ready=assume_ready,
            message=message,
            check_outputs=check_outputs,
            allow_wildcard_outputs=allow_wildcard_outputs,
            dry_run=dry_run,
            jobs=jobs,
        ):
            yield r

    @staticmethod
    def custom_result_renderer(res, **kwargs):
        dry_run = kwargs.get("dry_run")
        if dry_run and "dry_run_info" in res:
            if dry_run == "basic":
                _display_basic(res)
            elif dry_run == "command":
                ui.message(res["dry_run_info"]["cmd_expanded"])
            else:
                raise ValueError(f"Unknown dry-run mode: {dry_run!r}")
        else:
            if (
                kwargs.get("on_failure") == "stop"
                and res.get("action") == "run"
                and res.get("status") == "error"
            ):
                msg_path = res.get("msg_path")
                if msg_path:
                    ds_path = res["path"]
                    if datalad.get_apimode() == "python":
                        help = (
                            f"\"Dataset('{ds_path}').save(path='.', "
                            "recursive=True, message_file='%s')\""
                        )
                    else:
                        help = "'datalad save -d . -r -F %s'"
                    lgr.info(
                        "The command had a non-zero exit code. "
                        "If this is expected, you can save the changes with "
                        f"{help}",
                        # shorten to the relative path for a more concise
                        # message
                        Path(msg_path).relative_to(ds_path),
                    )
            generic_result_renderer(res)


def _execute_slurm_command(command, pwd):
    """Execute a Slurm submission command and create a job tracking file.

    Parameters
    ----------
    command : str
        Command to execute (typically an sbatch command)
    pwd : str
        Working directory for command execution

    Returns
    -------
    tuple
        (exit_code, exception)
        exit_code is 0 on success, exception is None on success
    """
    from datalad.cmd import WitlessRunner

    exc = None
    cmd_exitcode = None

    try:
        lgr.info("== Slurm submission start (output follows) =====")
        # Run the command and capture output
        result = subprocess.run(
            command, shell=True, capture_output=True, text=True, cwd=pwd
        )

        # Extract job ID from Slurm output
        # Typical output: "Submitted batch job 123456"
        stdout = result.stdout
        match = re.search(r"Submitted batch job (\d+)", stdout)

        # if match and save_tracking_file:
        #     job_id = match.group(1)
        #     # Create job tracking file
        #     tracking_file = os.path.join(pwd, f"slurm-job-submission-{job_id}")
        #     try:
        #         with open(tracking_file, 'w') as f:
        #             # Could add additional metadata here if needed
        #             pass
        #         lgr.info(f"Created tracking file: {tracking_file}")
        #     except IOError as e:
        #         lgr.warning(f"Failed to create tracking file: {e}")
        #         # Don't fail the command just because tracking file creation failed
        # else:
        #     lgr.warning("Could not extract job ID from Slurm output")

        if match:
            job_id = match.group(1)
        else:
            lgr.warning("Could not extract job ID from Slurm output")

    except subprocess.SubprocessError as e:
        exc = e
        cmd_exitcode = e.returncode if hasattr(e, "returncode") else 1
        lgr.error(f"Command failed with exit code {cmd_exitcode}")

    lgr.info("== Slurm submission complete =====")
    return cmd_exitcode or 0, exc, job_id


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
        use_sidecar = ds.config.get("datalad.run.record-sidecar", default=False)
        use_sidecar = anything2bool(use_sidecar)
    else:
        use_sidecar = sidecar_flag

    record_id = None
    record_path = None
    if use_sidecar:
        # record ID is hash of record itself
        from hashlib import md5

        record_id = md5(record.encode("utf-8")).hexdigest()  # nosec
        record_dir = ds.config.get(
            "datalad.run.record-directory", default=op.join(".datalad", "runinfo")
        )
        record_path = ds.pathobj / record_dir / record_id
        if not op.lexists(record_path):
            # go for compression, even for minimal records not much difference,
            # despite offset cost
            # wrap in list -- there is just one record
            dump2stream([run_info], record_path, compressed=True)
    return record_id or record, record_path


def run_command(
    cmd,
    dataset=None,
    inputs=None,
    outputs=None,
    expand=None,
    assume_ready=None,
    message=None,
    check_outputs=True,
    allow_wildcard_outputs=False,
    sidecar=None,
    dry_run=False,
    jobs=None,
    explicit=True,
    extra_info=None,
    rerun_info=None,
    extra_inputs=None,
    rerun_outputs=None,
    inject=False,
    parametric_record=False,
    remove_outputs=False,
    skip_dirtycheck=False,
    yield_expanded=None,
):
    """Run `cmd` in `dataset` and record the results.

    `Run.__call__` is a simple wrapper over this function. Aside from backward
    compatibility kludges, the only difference is that `Run.__call__` doesn't
    expose all the parameters of this function. The unexposed parameters are
    listed below.

    Parameters
    ----------
    extra_info : dict, optional
        Additional information to dump with the json run record. Any value
        given here will take precedence over the standard run key. Warning: To
        avoid collisions with future keys added by `run`, callers should try to
        use fairly specific key names and are encouraged to nest fields under a
        top-level "namespace" key (e.g., the project or extension name).
    rerun_info : dict, optional
        Record from a previous run. This is used internally by `rerun`.
    extra_inputs : list, optional
        Inputs to use in addition to those specified by `inputs`. Unlike
        `inputs`, these will not be injected into the {inputs} format field.
    rerun_outputs : list, optional
        Outputs, in addition to those in `outputs`, determined automatically
        from a previous run. This is used internally by `rerun`.
    inject : bool, optional
        Record results as if a command was run, skipping input and output
        preparation and command execution. In this mode, the caller is
        responsible for ensuring that the state of the working tree is
        appropriate for recording the command's results.
    parametric_record : bool, optional
        If enabled, substitution placeholders in the input/output specification
        are retained verbatim in the run record. This enables using a single
        run record for multiple different re-runs via individual
        parametrization.
    remove_outputs : bool, optional
        If enabled, all declared outputs will be removed prior command
        execution, except for paths that are also declared inputs.
    skip_dirtycheck : bool, optional
        If enabled, a check for dataset modifications is unconditionally
        disabled, even if other parameters would indicate otherwise. This
        can be used by callers that already performed analog verififcations
        to avoid duplicate processing.
    yield_expanded : {'inputs', 'outputs', 'both'}, optional
        Include a 'expanded_%s' item into the run result with the expanded list
        of paths matching the inputs and/or outputs specification,
        respectively.


    Yields
    ------
    Result records for the run.
    """
    if not cmd:
        lgr.warning("No command given")
        return
    specs = {
        k: ensure_list(v)
        for k, v in (
            ("inputs", inputs),
            ("extra_inputs", extra_inputs),
            ("outputs", outputs),
        )
    }

    rel_pwd = rerun_info.get("pwd") if rerun_info else None
    if rel_pwd and dataset:
        # recording is relative to the dataset
        pwd = op.normpath(op.join(dataset.path, rel_pwd))
        rel_pwd = op.relpath(pwd, dataset.path)
    else:
        pwd, rel_pwd = get_command_pwds(dataset)

    ds = require_dataset(
        dataset, check_installed=True, purpose="track command outcomes"
    )
    ds_path = ds.path

    lgr.debug("tracking command output underneath %s", ds)

    # skip for callers that already take care of this
    if not (skip_dirtycheck or rerun_info or inject):
        # For explicit=True, we probably want to check whether any inputs have
        # modifications. However, we can't just do is_dirty(..., path=inputs)
        # because we need to consider subdatasets and untracked files.
        # MIH: is_dirty() is gone, but status() can do all of the above!
        if not explicit and ds.repo.dirty:
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
    
    if not rerun_info and not allow_wildcard_outputs and outputs:
        wildcard_list = ["*", "?", "[", "]", "!", "^", "{", "}"]
        if any(char in output for char in wildcard_list for output in outputs):
            yield get_status_dict(
                "run",
                ds=ds,
                status="impossible",
                message=(
                    "Outputs include wildcards. This error can be disabled "
                    "with the parameter --allow-wildcard-outputs."
                ),
            )
            return

    # everything below expects the string-form of the command
    cmd = normalize_command(cmd)
    # pull substitutions from config
    cmd_fmt_kwargs = _get_substitutions(ds)
    # amend with unexpanded dependency/output specifications, which might
    # themselves contain substitution placeholder
    for n, val in specs.items():
        if val:
            cmd_fmt_kwargs[n] = val

    # apply the substitution to the IO specs
    expanded_specs = {k: _format_iospecs(v, **cmd_fmt_kwargs) for k, v in specs.items()}
    # try-expect to catch expansion issues in _format_iospecs() which
    # expands placeholders in dependency/output specification before
    # globbing
    try:
        globbed = {
            k: GlobbedPaths(
                v,
                pwd=pwd,
                expand=expand
                in (
                    # extra_inputs follow same expansion rules as `inputs`.
                    ["both"]
                    + (["outputs"] if k == "outputs" else ["inputs"])
                ),
            )
            for k, v in expanded_specs.items()
        }
    except KeyError as exc:
        yield get_status_dict(
            "run",
            ds=ds,
            status="impossible",
            message=(
                "input/output specification has an unrecognized " "placeholder: %s",
                exc,
            ),
        )
        return

    if not (inject or dry_run):
        yield from _prep_worktree(
            ds_path,
            pwd,
            globbed,
            assume_ready=assume_ready,
            remove_outputs=remove_outputs,
            rerun_outputs=rerun_outputs,
            jobs=None,
        )
    else:
        # If an inject=True caller wants to override the exit code, they can do
        # so in extra_info.
        cmd_exitcode = 0
        exc = None

    # prepare command formatting by extending the set of configurable
    # substitutions with the essential components
    cmd_fmt_kwargs.update(
        pwd=pwd,
        dspath=ds_path,
        # Check if the command contains "{tmpdir}" to avoid creating an
        # unnecessary temporary directory in most but not all cases.
        tmpdir=mkdtemp(prefix="datalad-run-") if "{tmpdir}" in cmd else "",
        # the following override any matching non-glob substitution
        # values
        inputs=globbed["inputs"],
        outputs=globbed["outputs"],
    )
    try:
        cmd_expanded = format_command(ds, cmd, **cmd_fmt_kwargs)
    except KeyError as exc:
        yield get_status_dict(
            "run",
            ds=ds,
            status="impossible",
            message=("command has an unrecognized placeholder: %s", exc),
        )
        return

    # amend commit message with `run` info:
    # - pwd if inside the dataset
    # - the command itself
    # - exit code of the command
    run_info = {
        "cmd": cmd,
        # rerun does not handle any prop being None, hence all
        # the `or/else []`
        "chain": rerun_info["chain"] if rerun_info else [],
    }
    # for all following we need to make sure that the raw
    # specifications, incl. any placeholders make it into
    # the run-record to enable "parametric" re-runs
    # ...except when expansion was requested
    for k, v in specs.items():
        run_info[k] = (
            globbed[k].paths
            if expand in ["both"] + (["outputs"] if k == "outputs" else ["inputs"])
            else (v if parametric_record else expanded_specs[k]) or []
        )

    if rel_pwd is not None:
        # only when inside the dataset to not leak information
        run_info["pwd"] = rel_pwd
    if ds.id:
        run_info["dsid"] = ds.id
    if extra_info:
        run_info.update(extra_info)

    if dry_run:
        yield get_status_dict(
            "run [dry-run]",
            ds=ds,
            status="ok",
            message="Dry run",
            run_info=run_info,
            dry_run_info=dict(
                cmd_expanded=cmd_expanded,
                pwd_full=pwd,
                **{k: globbed[k].expand() for k in ("inputs", "outputs")},
            ),
        )
        return

    # now check history of outputs in un-finished slurm commands
    if check_outputs:
        output_conflict, status_ok = check_output_conflict(ds, run_info["outputs"])
        if not status_ok:
            yield get_status_dict(
                "schedule",
                ds=ds,
                status="error",
                message=("Database connection cannot be established"),
            )
            return
        if output_conflict:
            yield get_status_dict(
                "schedule",
                ds=ds,
                status="error",
                message=(
                    "There are conflicting outputs with the previously scheduled jobs: {}. \n"
                    "Finish those jobs or adjust output for the current job first."
                ).format(output_conflict),
            )
            return
    
    # TODO what happens in case of inject??
    if not inject:
        cmd_exitcode, exc, slurm_job_id = _execute_slurm_command(cmd_expanded, pwd)
        run_info["exit"] = cmd_exitcode
        slurm_outputs, slurm_env_file = get_slurm_output_files(slurm_job_id)
        run_info["outputs"].extend(slurm_outputs)
        run_info["outputs"].append(slurm_env_file)
        run_info["slurm_run_outputs"] = slurm_outputs
        run_info["slurm_run_outputs"].append(slurm_env_file)

    # add the slurm job id to the run info
    run_info["slurm_job_id"] = slurm_job_id

    # Re-glob to capture any new outputs.
    #
    # TODO: If a warning or error is desired when an --output pattern doesn't
    # have a match, this would be the spot to do it.
    if explicit or expand in ["outputs", "both"]:
        # also for explicit mode we have to re-glob to be able to save all
        # matching outputs
        globbed["outputs"].expand(refresh=True)
        if expand in ["outputs", "both"]:
            run_info["outputs"] = globbed["outputs"].paths
            # add the slurm outputs and environment files
            # these are not captured in the initial globbing
            run_info["outputs"].extend(slurm_outputs)

    # create the run record, either as a string, or written to a file
    # depending on the config/request
    record, record_path = _create_record(run_info, sidecar, ds)

    # abbreviate version of the command for illustrative purposes
    cmd_shorty = _format_cmd_shorty(cmd_expanded)

    # compose commit message
    if rerun_info:
        schedule_msg = "RESCHEDULE"
    else:
        schedule_msg = "SCHEDULE"
    prefix = f"[DATALAD {schedule_msg}] "
    msg = (
        prefix
        + """\
{}

=== Do not change lines below ===
{}
^^^ Do not change lines above ^^^
"""
    )
    # append pending to the message
    if message is not None:
        message += f"\n Submitted batch job {slurm_job_id}: Pending"
    else:
        message = f"Submitted batch job {slurm_job_id}: Pending"

    # add extra info for re-scheduled jobs
    if rerun_info:
        slurm_id_old = rerun_info["slurm_job_id"]
        message += f"\n\nRe-submission of job {slurm_id_old}."

    msg = msg.format(
        message if message is not None else cmd_shorty,
        '"{}"'.format(record) if record_path else record,
    )

    msg_path = None
    if not rerun_info and cmd_exitcode:
        if do_save:
            repo = ds.repo
            # must record path to be relative to ds.path to meet
            # result record semantics (think symlink resolution, etc)
            msg_path = (
                ds.pathobj / repo.dot_git.relative_to(repo.pathobj) / "COMMIT_EDITMSG"
            )
            msg_path.write_text(msg)

    expected_exit = rerun_info.get("exit", 0) if rerun_info else None
    if cmd_exitcode and expected_exit != cmd_exitcode:
        status = "error"
    else:
        status = "ok"

    status_ok = add_to_database(ds, run_info, msg)
    if not status_ok:
        yield get_status_dict(
            "schedule",
            ds=ds,
            status="error",
            message=("Database connection cannot be established"),
        )
        return                    

    run_result = get_status_dict(
        "run",
        ds=ds,
        status=status,
        # use the abbrev. command as the message to give immediate clarity what
        # completed/errors in the generic result rendering
        message=cmd_shorty,
        run_info=run_info,
        # use the same key that `get_status_dict()` would/will use
        # to record the exit code in case of an exception
        exit_code=cmd_exitcode,
        exception=exc,
        # Provide msg_path and explicit outputs so that, under
        # on_failure='stop', callers can react to a failure and then call
        # save().
        msg_path=str(msg_path) if msg_path else None,
    )
    if record_path:
        # we the record is in a sidecar file, report its ID
        run_result["record_id"] = record
    for s in ("inputs", "outputs"):
        # this enables callers to further inspect the outputs without
        # performing globbing again. Together with remove_outputs=True
        # these would be guaranteed to be the outcome of the executed
        # command. in contrast to `outputs_to_save` this does not
        # include aux file, such as the run record sidecar file.
        # calling .expand_strict() again is largely reporting cached
        # information
        # (format: relative paths)
        if yield_expanded in (s, "both"):
            run_result[f"expanded_{s}"] = globbed[s].expand_strict()
    yield run_result

def check_output_conflict(dset, outputs):
    """
    Check for conflicts between provided outputs and existing outputs in the database.
    
    Args:
        dset: Dataset object containing repository information
        outputs: List of strings representing output paths to check
        
    Returns:
        list: List of slurm_job_ids that have conflicting outputs. Empty list if no conflicts
              or if database error occurs.
    """
    # Connect to database
    con, cur = connect_to_database(dset)
    if not con or not cur:
        return None, None

    # Get all existing outputs from database
    try:
        cur.execute("SELECT slurm_job_id, outputs FROM open_jobs")
        existing_records = cur.fetchall()

        # Check each record for conflicts
        conflicting_jobs = []
        for job_id, json_outputs in existing_records:
            try:
                existing_outputs = json.loads(json_outputs)
                if set(outputs) & set(existing_outputs):
                    conflicting_jobs.append(job_id)
            except json.JSONDecodeError:
                continue
    except sqlite3.Error:
        conflicting_jobs = []
    return conflicting_jobs, True


def get_slurm_output_files(job_id):
    """
    Gets the relative paths to StdOut and StdErr files for a Slurm job.

    Args:
        job_id (str): The Slurm job ID

    Returns:
        list: List containing relative path(s) to output files. If StdOut and StdErr
              are the same file, returns a single path.

    Raises:
        subprocess.CalledProcessError: If scontrol command fails
        ValueError: If required file paths cannot be found in scontrol output
    """
    # Run scontrol command and get output
    try:
        result = subprocess.run(
            ["scontrol", "show", "job", str(job_id)],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        raise subprocess.CalledProcessError(
            e.returncode, e.cmd, f"Failed to get job information: {e.output}"
        )

    # Parse output to find StdOut and StdErr
    parsed_data = parse_slurm_output(result.stdout)
    if "ArrayJobId" in parsed_data:
        array_task_id = parsed_data["ArrayTaskId"]
        slurm_job_ids = generate_array_job_names(str(job_id),str(array_task_id))
    else:
        slurm_job_ids = [job_id]
        
    slurm_out_paths = []
    for i, slurm_job_id in enumerate(slurm_job_ids):
        # Run scontrol command and get output
        try:
            result = subprocess.run(
                ["scontrol", "show", "job", str(slurm_job_id)],
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise subprocess.CalledProcessError(
                e.returncode, e.cmd, f"Failed to get job information: {e.output}"
            )

        # Parse output to find StdOut and StdErr
        parsed_data = parse_slurm_output(result.stdout)

        stdout_path = parsed_data.get("StdOut")
        stderr_path = parsed_data.get("StdErr")

        if not stdout_path or not stderr_path:
            raise ValueError("Could not find StdOut or StdErr paths in scontrol output")
        cwd = Path.cwd()
        stdout_path = Path(stdout_path)
        stderr_path = Path(stderr_path)
            
        if i==0:
            # Write parsed data to JSON file
            slurm_env_file = stdout_path.parent / f"slurm-job-{job_id}.env.json"
            with open(slurm_env_file, "w") as f:
                json.dump(parsed_data, f, indent=2)
            rel_slurmenv = os.path.relpath(slurm_env_file, cwd)

        # Get relative paths
        try:
            rel_stdout = os.path.relpath(stdout_path, cwd)
            rel_stderr = os.path.relpath(stderr_path, cwd)
        except ValueError as e:
            raise ValueError(f"Cannot compute relative path: {e}")
            
        slurm_out_paths.append(rel_stdout)
        if rel_stdout!=rel_stderr:
            slurm_out_paths.append(rel_stderr)

    return slurm_out_paths, rel_slurmenv


def parse_slurm_output(output):
    """Parse SLURM output into a dictionary, handling space-separated assignments"""
    result = {}
    # TODO Is this necessary for privacy purposes?
    # What is useful to oneself vs for the community when pushing to git
    excluded_keys = {"UserId", "JobId"}
    for line in output.split("\n"):
        # Split line into space-separated parts
        parts = line.strip().split()
        for part in parts:
            if "=" in part:
                key, value = part.split("=", 1)
                if key not in excluded_keys:
                    result[key] = value
    return result

def generate_array_job_names(job_id, job_task_id):
    """
    Generate individual job names for a Slurm array job.
    
    Args:
        job_id (str): The base Slurm job ID
        job_task_id (str): The array specification (e.g., "1-5", "1,3,5", "1-10:2")
    
    Returns:
        list[str]: List of job names in the format "job_id_array_index"
    
    Examples:
        >>> generate_array_job_names("12345", "1-3")
        ['12345_1', '12345_2', '12345_3']
        
        >>> generate_array_job_names("12345", "1,3,5")
        ['12345_1', '12345_3', '12345_5']
        
        >>> generate_array_job_names("12345", "1-5:2")
        ['12345_1', '12345_3', '12345_5']
    """
    job_names = []
    
    # Remove any % limitations if present
    if '%' in job_task_id:
        job_task_id = job_task_id.split('%')[0]
    
    # Split by comma to handle multiple ranges
    ranges = job_task_id.split(',')
    
    for range_spec in ranges:
        # Handle individual numbers
        if '-' not in range_spec:
            job_names.append(f"{job_id}_{range_spec}")
            continue
            
        # Handle ranges with optional step
        range_parts = range_spec.split(':')
        start, end = map(int, range_parts[0].split('-'))
        step = int(range_parts[1]) if len(range_parts) > 1 else 1
        
        for i in range(start, end + 1, step):
            job_names.append(f"{job_id}_{i}")
    
    return job_names


def add_to_database(dset, run_info, message):
    """Add a `datalad schedule` command to an sqlite database."""
    con, cur = connect_to_database(dset)
    if not cur or not con:
        return None
    
    # create an empty table if it doesn't exist
    cur.execute("""
    CREATE TABLE IF NOT EXISTS open_jobs (
    slurm_job_id INTEGER,
    message TEXT,
    chain TEXT CHECK (json_valid(chain)),
    cmd TEXT,
    dsid TEXT,
    inputs TEXT CHECK (json_valid(inputs)),
    extra_inputs TEXT CHECK (json_valid(extra_inputs)),
    outputs TEXT CHECK (json_valid(outputs)),
    slurm_outputs TEXT CHECK (json_valid(slurm_outputs)),
    pwd TEXT
    )
    """)

    # convert the inputs to json
    inputs_json = json.dumps(run_info["inputs"])

    # convert the extra inputs to json
    extra_inputs_json = json.dumps(run_info["extra_inputs"])

    # convert the outputs to json
    outputs_json = json.dumps(run_info["outputs"])

    # convert the slurm outputs to json
    slurm_outputs_json = json.dumps(run_info["slurm_run_outputs"])

    # convert chain to json
    chain_json = json.dumps(run_info["chain"])

    # add the most recent schedule command to the table
    cur.execute("""
    INSERT INTO open_jobs (slurm_job_id,
    message,
    chain,
    cmd,
    dsid,
    inputs,
    extra_inputs,
    outputs,
    slurm_outputs,
    pwd)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    (run_info["slurm_job_id"],
     message,
     chain_json,
     run_info["cmd"],
     run_info["dsid"],
     inputs_json,
     extra_inputs_json,
     outputs_json,
     slurm_outputs_json,
     run_info["pwd"]))
    
    # save and close
    con.commit()
    con.close()

    return True
    

    
    
