"""DataLad schedule command"""

__docformat__ = 'restructuredtext'

import json
import logging
import os.path as op
from hashlib import md5
import subprocess
import re
from pathlib import Path

from datalad.interface.base import Interface
from datalad.interface.base import build_doc
from datalad.support.param import Parameter
from datalad.distribution.dataset import datasetmethod, EnsureDataset, require_dataset
from datalad.interface.base import eval_results
from datalad.support.constraints import (
    EnsureChoice,
    EnsureNone,
    EnsureStr,
    EnsureBool
)
from datalad.interface.results import get_status_dict
from datalad.core.local.save import Save
from datalad.support.json_py import dump2stream
from datalad.config import anything2bool

import logging
lgr = logging.getLogger('datalad.slurm.schedule')

def extract_slurm_id(output):
    """Extract the SLURM job ID from command output."""
    match = re.search(r'Submitted batch job (\d+)', output)
    if match:
        return match.group(1)
    return None

def _create_record(run_info, sidecar_flag, ds):
    """Create a run record, either in the commit message or in a sidecar file.
    
    Parameters
    ----------
    run_info : dict
        Information about the scheduled job
    sidecar_flag : bool or None
        Whether to force using a sidecar file
    ds : Dataset
        Dataset to store the record in

    Returns
    -------
    str or None, str or None
        record content or ID, path to record file
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
        record_id = md5(record.encode('utf-8')).hexdigest()
        record_dir = ds.config.get(
            'datalad.run.record-directory',
            default=op.join('.datalad', 'runinfo'))
        record_path = ds.pathobj / record_dir / record_id
        if not op.lexists(record_path):
            dump2stream([run_info], record_path, compressed=True)
            
    return record_id or record, record_path

@build_doc
class Schedule(Interface):
    """Schedule a job using SLURM in DataLad
    
    Submit jobs to SLURM that will be tracked by DataLad.
    """

    _params_ = dict(
        cmd=Parameter(
            args=("cmd",),
            nargs="?",
            metavar="COMMAND",
            doc="""SLURM command to run. Common options include 'sbatch' for 
            submitting batch scripts and 'srun' for running interactive 
            jobs.""",
            constraints=EnsureStr() | EnsureNone()),
            
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""specify the dataset to record the command results in.
            If no dataset is given, an attempt is made to identify the dataset
            based on the current working directory""",
            constraints=EnsureDataset() | EnsureNone()),
        
        script=Parameter(
            args=("--script",),
            metavar="PATH",
            doc="""path to the SLURM script to be submitted. Optional if the script
            is specified as part of the command.""",
            constraints=EnsureStr() | EnsureNone()),

        expanded=Parameter(
            args=("--expanded",),
            doc="""if set, a separate record with a full description of 
            the executed command will be generated for each job.""",
            action="store_true"),

        explicit=Parameter(
            args=("--explicit",),
            doc="""if set, all modifications to be made by the command are 
            explicitly specified in the command's execution record.""",
            action="store_true"),

        message=Parameter(
            args=("-m", "--message",),
            metavar="MESSAGE",
            doc="""a description of the job to be recorded in the
            run record. This will be the commit message for the saved state in the
            dataset""",
            constraints=EnsureStr() | EnsureNone()),

        output_streams=Parameter(
            args=("-o", "--output-streams",),
            doc="""configuration item that enables the capturing of content of 
            stdout and stderr in the commit message.""",
            action="store_true"),

        sidecar=Parameter(
            args=('--sidecar',),
            metavar="{yes|no}",
            doc="""By default, the configuration variable
            'datalad.run.record-sidecar' determines whether a record with
            information about a command's execution is placed into a separate
            record file instead of the commit message (default: off). This
            option can be used to override the configured behavior on a
            case-by-case basis.""",
            constraints=EnsureNone() | EnsureBool()),
    )

    @staticmethod
    @datasetmethod(name='schedule')
    @eval_results
    def __call__(
        cmd=None,
        script=None,
        dataset=None,
        expanded=False,
        explicit=False,
        message=None,
        output_streams=False,
        sidecar=None,
    ):
        if cmd is None:
            yield get_status_dict(
                action='schedule',
                status='error',
                message='No SLURM command specified. Please provide a command (e.g., sbatch, srun)'
            )
            return

        ds = require_dataset(
            dataset, check_installed=True,
            purpose='schedule a command'
        )

        if not explicit and ds.repo.dirty:
            yield get_status_dict(
                'schedule',
                ds=ds,
                status='impossible',
                message=(
                    'clean dataset required to detect changes from command; '
                    'use `datalad status` to inspect unsaved changes'
                )
            )
            return

        # Construct the full command
        if script is not None:
            if not op.exists(script):
                yield get_status_dict(
                    action='schedule',
                    status='error',
                    message=f'Script file not found: {script}'
                )
                return
            full_cmd = f"{cmd} {script}"
        else:
            full_cmd = cmd

        # Execute the SLURM command
        try:
            process = subprocess.run(
                full_cmd,
                shell=True,
                capture_output=True,
                text=True,
                check=True
            )

            # Extract job ID from output
            job_id = extract_slurm_id(process.stdout)
            if not job_id:
                lgr.warning("Could not extract SLURM job ID from output")

            # Create run info
            run_info = {
                'cmd': full_cmd,
                'slurm_id': job_id,
                'expanded': expanded,
                'explicit': explicit,
                'output_streams': output_streams,
                'status': 'scheduled',
                'type': 'slurm'
            }

            # Create the run record
            record, record_path = _create_record(run_info, sidecar, ds)

            # Prepare commit message
            if message is None:
                message = f"[DATALAD SCHEDULE] {full_cmd}"

            msg = u"""\
[DATALAD SCHEDULE] {}

=== Do not change lines below ===
{}
^^^ Do not change lines above ^^^
"""
            msg = msg.format(
                message if message is not None else full_cmd,
                '"{}"'.format(record) if record_path else record
            )

            # Save the record
            outputs_to_save = [record_path] if record_path else None
            
            for r in Save.__call__(
                    dataset=ds.path,
                    path=outputs_to_save,
                    message=msg,
                    recursive=True,
                    return_type='generator',
                    result_renderer='disabled',
                    on_failure='ignore'):
                yield r

            yield get_status_dict(
                action='schedule',
                path=ds.path,
                type='dataset',
                status='ok',
                message=f"Scheduled job {job_id}: {full_cmd}",
                job_id=job_id
            )

        except subprocess.CalledProcessError as e:
            yield get_status_dict(
                action='schedule',
                path=ds.path,
                type='dataset',
                status='error',
                message=f"Command failed: {str(e)}\nStderr: {e.stderr}"
            )
            return
