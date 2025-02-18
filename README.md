# datalad-slurm: A DataLad extension for HPC (slurm) systems

[![Build status](https://ci.appveyor.com/api/projects/status/g9von5wtpoidcecy/branch/main?svg=true)](https://ci.appveyor.com/project/mih/datalad-extension-template/branch/main) [![codecov.io](https://codecov.io/github/datalad/datalad-extension-template/coverage.svg?branch=main)](https://codecov.io/github/datalad/datalad-extension-template?branch=main) [![crippled-filesystems](https://github.com/datalad/datalad-extension-template/workflows/crippled-filesystems/badge.svg)](https://github.com/datalad/datalad-extension-template/actions?query=workflow%3Acrippled-filesystems) [![docs](https://github.com/datalad/datalad-extension-template/workflows/docs/badge.svg)](https://github.com/datalad/datalad-extension-template/actions?query=workflow%3Adocs)


`datalad-slurm` is an extension to the [DataLad](http://datalad.org) package for high-performance computing (HPC), specifically slurm systems. 

DataLad is a package which facilitates adherence to the [FAIR](https://www.nature.com/articles/sdata201618) research data management principles.

`datalad-slurm` sits on top of the main DataLad package, and it is designed to improve the DataLad workflow on HPC systems. The package is aimed at slurm systems due to the prominence of slurm in HPC settings, but in the future it may be extended to HPC systems more generally. 

`datalad-slurm` makes it easier for users to manage their research data on HPC systems with DataLad, and also solves the following conflicts of DataLad usage in HPC systems:

- **Inefficient** sequential sections in highly parallel HPC jobs
- **Critical** race conditions between git commands in concurrent jobs

## Installation

First, install the main [DataLad](http://datalad.org) package and its dependencies.

Then, clone this repository and install the extension with:

    pip install -e .

## Example usage

To **schedule** a slurm script:

    datalad schedule --output=<output_files_or_dir> <slurm_submission_command>

where `<output_files_or_dir>` are the expected outputs from the job, and `<slurm_submission_command>` is for example `sbatch submit_script`. Further optional command line arguments can be found in the documentation.

Multiple jobs (including array jobs) can be scheduled sequentially. They are tracked in an SQLite database. Note that any open jobs must not have conflicting outputs with previously scheduled jobs. This is so that the outputs of each slurm run can be tracked to their correct 

To **finish** (i.e. post-process) these jobs (once they are complete), simply run:

    datalad finish

Alternatively, to finish a particular scheduled job, run:

    datalad finish <slurm_job_id>

This will create a `[DATALAD SLURM RUN]` entry in the git log, analogous to a `datalad run` command.

`datalad-slurm` will flag an error for any jobs which could not be post-processed, either because they are still running, or the job failed. These are not automatically cleared from the SQLite database. The output files should first be removed or manually added in git, before running

    datalad finish --close-failed-jobs

To clear the SQLite database. To inspect the current status of all open jobs (without saving anything in git), run:

    datalad finish --list-open-jobs

To **reschedule** a previously scheduled job:

    datalad reschedule <schedule_commit_hash>

where `<schedule_commit_hash>` is the commit hash of the previously scheduled job. There must also be a corresponding `datalad finish` command to the original `datalad schedule`, otherwise `datalad reschedule` will throw an error.

In the lingo of the original DataLad package, the combination of `datalad schedule + datalad finish` is similar to `datalad run`, and `datalad reschedule + datalad finish` is similar to `datalad rerun`.

An example workflow could look like this (constructed deliberately to have some failed jobs):

    datalad schedule -o models/abrupt/gold/ sbatch submit_gold.slurm
    datalad schedule -o models/abrupt/silver/ sbatch submit_silver.slurm
    datalad schedule -o models/abrupt/bronze/ sbatch submit_bronze.slurm
    datalad schedule -o models/abrupt/platinum/ sbatch submit_array_platinum.slurm

Checking the job statuses at some point while they are running:

    datalad finish --list-open-jobs
    
    The following jobs are open: 

    slurm-job-id   slurm-job-status
    10524442       COMPLETED
    10524535       RUNNING
    10524556       FAILED
    10524620       PENDING

Later, once all the jobs have finished running:

    datalad finish
    
    add(ok): models/abrupt/gold/05_02/slurm-10524442.out (file)                                                                                                                                                         
    add(ok): models/abrupt/gold/05_02/slurm-job-10524442.env.json (file)                                                                                                                                                
    add(ok): models/abrupt/gold/05_02/model_0.model.gz (file)                                                                                                                                                           
    save(ok): . (dataset)                                                                                                                                                                                               
    add(ok): models/abrupt/silver/05_02/slurm-10524535.out (file)                                                                                                                                                       
    add(ok): models/abrupt/silver/05_02/slurm-job-10524535.env.json (file)                                                                                                                                              
    add(ok): models/abrupt/silver/05_02/model_0.model.gz (file)                                                                                                                                                         
    add(ok): models/abrupt/silver/05_02/model.scaler.gz (file)                                                                                                                                                          
    save(ok): . (dataset)                                                                                                                                                                                               
    finish(impossible): [Slurm job(s) for job 10524556 are not complete.Statuses: 10524556: FAILED]                                                                                                                     
    finish(impossible): [Slurm job(s) for job 10524620 are not complete.Statuses: 10524620_0: COMPLETED, 10524620_1: COMPLETED, 10524620_2: TIMEOUT]
    action summary:
      add (ok: 7)
      finish (impossible: 2)
      save (ok: 2)

To close the failed jobs:

    datalad finish --close-failed-jobs

    finish(ok): [Closing failed / cancelled jobs. Statuses: 10524556: FAILED]
    finish(ok): [Closing failed / cancelled jobs. Statuses: 10524620_0: COMPLETED, 10524620_1: COMPLETED, 10524620_2: TIMEOUT]
    action summary:
    finish (ok: 2)

Note that if any sub-job of an array job fails, that whole job is treated as a failed job. The user always has the option to manually commit the successful outputs if desired.

The git history would then appear like so:

    git log --oneline

    a8e4aa6 (HEAD -> master) [DATALAD SLURM RUN] Slurm job 10524535: Completed
    25067fe [DATALAD SLURM RUN] Slurm job 10524442: Completed

With one particular entry looking like:

    commit a8e4aa62519db3b5f63243cc925ee918984bf506 (HEAD -> master)
    Author: Tim Callow <tim@notmyrealemail.com>
    Date:   Tue Feb 18 09:31:47 2025 +0100

        [DATALAD SLURM RUN] Slurm job 10524535: Completed
    
        === Do not change lines below ===
        {
         "chain": [],
         "cmd": "sbatch submit_silver.slurm",
         "commit_id": null,
         "dsid": "61576cad-ea4f-4425-8f35-16b9955c9926",
         "extra_inputs": [],
         "inputs": [],
         "outputs": [
          "models/abrupt/silver",
          "models/abrupt/silver/05_02/slurm-10524535.out",
          "models/abrupt/silver/05_02/slurm-job-10524535.env.json"
         ],
         "pwd": ".",
         "slurm_job_id": 10524535,
         "slurm_outputs": [
          "models/abrupt/silver/05_02/slurm-10524535.out",
          "models/abrupt/silver/05_02/slurm-job-10524535.env.json"
         ]
        }
        ^^^ Do not change lines above ^^^


## Contributing

The `datalad-slurm` extension is still in the very early stages of development. We welcome contributors and testers of the package. Please document any issues on GitHub and we will try to resolve them.

See [CONTRIBUTING.md](CONTRIBUTING.md) if you are interested in internals or
contributing to the project.
