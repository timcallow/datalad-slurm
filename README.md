# Tests for the datalad Slurm extension

The following tests scripts can be executed manually and should run correctly or produce errors that should be handled as errors.

Since it needs to work on datalad repositories which are also git repositories, and because a working Slurm environment is required, this is not (yet) part of automated CI tests ... let's see later if this would be feasible via git CI anyway.



## In general

Each test should be run as:

`./test_x.sh <dir>`, where `<dir>` is some (temporary) directory to store the test results.

All tests will create their own temporary datalad repo inside `<dir>` and work inside that. They can be removed after with `chmod -R u+w datalad-slurm-test*/; rm -Rf datalad-slurm-test*/`

The `slurm_test*.template.sh` files need to be modified to match the local slurm environment.

## Test 01

Test creating many job dirs with job scripts in it, then `datalad schedule` and run all jobs, wait until all run through, then `datalad finish` all jobs.

This should run without any errors.

## Test 02

Test creating many job dirs with job scripts in it like in Test 01. However, they have conflicting output directories so datalad should refuse to schedule some of them.

This should produce some errors by datalad:
* The first bunch of jobs should run fine including a clean `datalad finish`
* The second bunch of jobs schould not get scheduled because datalad sees the conflict and refuses to schedule them.
