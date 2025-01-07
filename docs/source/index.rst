datalad-slurm: A DataLad extension for HPC (slurm) systems
**********************************************************

``datalad-slurm`` is an extension to the `DataLad <http://datalad.org>`_ package for high-performance computing (HPC), specifically slurm systems.

DataLad is a package which facilitates adherence to the `FAIR <https://www.nature.com/articles/sdata201618>`_ research data management principles.

``datalad-slurm`` sits on top of the main DataLad package, and it is designed to improve the DataLad workflow on HPC systems. The package is aimed at slurm systems due to the prominence of slurm in HPC settings, but in the future it may be extended to HPC systems more generally.

``datalad-slurm`` makes it easier for users to manage their research data on HPC systems with DataLad, and also solves the following conflicts of DataLad usage in HPC systems:

* **Inefficient** sequential sections in highly parallel HPC jobs
* **Critical** race conditions between git commands in concurrent jobs

Installation
------------
First, install the main `DataLad <http://datalad.org>`_ package and its dependencies.

Then, clone this repository and install the extension with::

    pip install -e .

Example usage
-------------
To **schedule** a slurm script::

    datalad schedule --output=<output_files_or_dir> <slurm_submission_command>

where ``<output_files_or_dir>`` are the expected outputs from the job, and ``<slurm_submission_command>`` is for example ``sbatch submit_script``. Further optional command line arguments can be found in the documentation.

To **finish** (i.e. post-process) a job that was previously scheduled and is since finished::

    datalad finish <commit_hash>

where ``<commit_hash>`` is the commit hash of the previously scheduled job. Alternatively, to post-process all scheduled jobs, or all scheduled jobs since a certain commit, one can run::

    datalad finish

or::

    datalad finish --since=<since_commit_hash>

where ``<since_commit_hash>`` is the commit hash before all the entries in the ``git log`` that you want to consider.

``datalad-slurm`` will flag an error for any jobs which could not be post-processed, either because they are still running, or the job failed.

To **reschedule** a previously scheduled job::

    datalad reschedule <schedule_commit_hash>

where ``<schedule_commit_hash>`` is the commit hash of the previously scheduled job. There must also be a corresponding ``datalad finish`` command to the original ``datalad schedule``, otherwise ``datalad reschedule`` will throw an error.

In the lingo of the original DataLad package, the combination of ``datalad schedule + datalad finish`` is similar to ``datalad run``, and ``datalad reschedule + datalad finish`` is similar to ``datalad rerun``. One important difference is that the ``datalad-slurm`` commands always produce a pair of commits in the git history, whereas ``datalad run`` produces just one commit, and ``datalad rerun`` produces one or no commits, depending if there is any change to the outputs.

The git history might look a bit like this after running a few of these commands::

    4992a23 [DATALAD FINISH] Processed batch job 9166291: Complete
    732264c [DATALAD RESCHEDULE] Submitted batch job 9166291: Pending
    0c982f3 [DATALAD FINISH] Processed batch job 9163380: Complete
    4c021f4 [DATALAD SCHEDULE] Submitted batch job 9163380: Pending

API
===

High-level API commands
-----------------------

.. toctree::
   :maxdepth: 2

   python_reference.rst


Command line reference
----------------------

.. toctree::
   :maxdepth: 2

   cli_reference.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. |---| unicode:: U+02014 .. em dash
