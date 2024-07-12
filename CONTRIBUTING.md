# Contributing to cowtools

Since the tools here are built specifically for the [Wisconsin Analysis Facility](https://cms01.hep.wisc.edu:8000/), they are most easily tested at that Facility. For users of the facility with a custom image
already located at `/scratch/{$USER}/notebook.sif`, one method of testing is to log onto
cms01.hep.wisc.edu via ssh, and then launch a container that can install `cowtools` via
`singularity exec --bind /scratch/rsimeon/:/scratch/rsimeon /scratch/rsimeon/notebook.sif /bin/bash`.
From there, one can `python3 -m pip install .[dev]` and `python3 -m nox -s tests` to run the tests.

It is in principle possible to set up automated testing (CI/CD) via Github workflows in a way that
interacts nicely with the Condor requests in `cowtools`. If you are interested in adding this feature,
please fork the repo and create a pull request when you are ready.
