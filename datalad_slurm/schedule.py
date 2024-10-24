"""DataLad schedule command"""

__docformat__ = 'restructuredtext'

from os.path import curdir, abspath, exists

from datalad.interface.base import Interface
from datalad.interface.base import build_doc
from datalad.support.param import Parameter
from datalad.distribution.dataset import datasetmethod, EnsureDataset
from datalad.interface.base import eval_results
from datalad.support.constraints import (
    EnsureChoice,
    EnsureNone,
    EnsureStr,
    EnsureBool
)

from datalad.interface.results import get_status_dict

import logging
lgr = logging.getLogger('datalad.slurm.schedule')

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
            the executed command, as well as a record of the command's 
            standard output will be generated for each job.""",
            action="store_true"),

        explicit=Parameter(
            args=("--explicit",),
            doc="""if set, all modifications to be made by the command are 
            explicitly specified in the command's execution record. This is 
            achieved by a detailed analysis of the command's impact based on 
            information about its inputs and outputs.""",
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
            stdout and stderr in the commit message, in addition to relying on 
            the command exit code alone. If any source contains binary content, it 
            will be stored in a separate file instead of being inspected for line 
            endings and appended to the commit message.""",
            action="store_true"),
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
    ):
        if cmd is None:
            yield get_status_dict(
                action='schedule',
                path=abspath(curdir),
                status='error',
                message='No SLURM command specified. Please provide a command (e.g., sbatch, srun)'
            )
            return

        # If script is provided, verify it exists
        if script is not None and not exists(script):
            yield get_status_dict(
                action='schedule',
                path=abspath(curdir),
                status='error',
                message=f'Script file not found: {script}'
            )
            return

        # Construct the message based on whether a script was provided
        if script is not None:
            msg = f'scheduling job with command: {cmd} {script}'
        else:
            msg = f'scheduling job with command: {cmd}'
            
        print(msg)

        # Add details about other parameters
        details = []
        if expanded:
            details.append("expanded mode enabled")
        if explicit:
            details.append("explicit mode enabled")
        if message:
            details.append(f"message: {message}")
        if output_streams:
            details.append("output streams will be captured")
        
        if details:
            print("Additional settings:")
            for detail in details:
                print(f"- {detail}")

        yield get_status_dict(
            action='schedule',
            path=abspath(curdir),
            status='ok',
            message=msg,
            cmd=cmd,
            script=script,
            expanded=expanded,
            explicit=explicit,
            output_streams=output_streams
        )
