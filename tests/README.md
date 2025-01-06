# Tests for the datalad Slurm extension

The following tests scripts can be executed manually and should run correct or produce errors that should be handled as errors.

Since it needs to work on datalad repositories which are also git repositories and becauue a working Slurm environment is required, this is not (yet) part of automated CI tests ... let's see later if this would be feasible via git CI anyway.



## In general

All test will create their own temporary datalad repo and work inside. Feel free to remove them with `chmod -R u+w datalad-slurm-test*/; rm -Rf datalad-slurm-test*/`

The `slurm_test*.template.sh` files need to be modified to match the local slurm environment

## Test 01

Test creating many job dirs with job scripts in it, then `datalad schedule` and run all jobs, wait until all run through, then `datalad finish` all jobs.

This should run without any errors.


