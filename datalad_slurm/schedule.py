"""DataLad schedule command"""

__docformat__ = 'restructuredtext'

from os.path import curdir
from os.path import abspath

from datalad.interface.base import Interface
from datalad.interface.base import build_doc
from datalad.support.param import Parameter
from datalad.distribution.dataset import datasetmethod
from datalad.interface.base import eval_results
from datalad.support.constraints import EnsureChoice

from datalad.interface.results import get_status_dict

import logging
lgr = logging.getLogger('datalad.slurm.schedule')

@build_doc
class Schedule(Interface):
    """Schedule a job in DataLad

    Minimal implementation that prints 'scheduling job'.
    """

    # parameters of the command, must be exhaustive
    _params_ = dict()  # empty for now since we don't need parameters yet

    @staticmethod
    @datasetmethod(name='schedule')
    @eval_results
    def __call__():
        msg = 'scheduling slurm job'
        print(msg)

        # Return a status dictionary as expected by DataLad
        yield get_status_dict(
            action='schedule',
            path=abspath(curdir),
            status='ok',
            message=msg)
