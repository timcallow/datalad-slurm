"""DataLad demo extension"""

__docformat__ = 'restructuredtext'

import logging
lgr = logging.getLogger('datalad.slurm')

# Defines a datalad command suite.
# This variable must be bound as a setuptools entrypoint
# to be found by datalad
command_suite = (
    # description of the command suite, displayed in cmdline help
    "Demo DataLad command suite",
    [
        (
            # importable module that contains the schedule command implementation
            'datalad_slurm.schedule',
            # name of the command class implementation in above module
            'Schedule',
            # optional name of the command in the cmdline API
            'schedule',
            # optional name of the command in the Python API
            'schedule'
        ),
        (
            # importable module that contains the schedule command implementation
            'datalad_slurm.finish',
            # name of the command class implementation in above module
            'Finish',
            # optional name of the command in the cmdline API
            'finish',
            # optional name of the command in the Python API
            'finish'
        ),        
    ]
)

from . import _version
__version__ = _version.get_versions()['version']
