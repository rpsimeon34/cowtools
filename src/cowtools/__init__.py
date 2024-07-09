import os
from pathlib import Path
from dask_jobqueue import HTCondorCluster
from dask.distributed import Client

DEFAULT_SIF = "/home/vassal/notebook.sif"

def move_x509():
    '''
    Get x509 path, copy it to the correct location, and return the path. Primarily
    to be used in preparation for creating an HTCondorCluster object (like 
    via GetDefaultCondorClient.
    '''
    try:
        _x509_localpath = (
            [
                line
                for line in os.popen("voms-proxy-info").read().split("\n")
                if line.startswith("path")
            ][0]
            .split(":")[-1]
            .strip()
        )
    except Exception as err:
        raise RuntimeError(
            "x509 proxy could not be parsed, try creating it with 'voms-proxy-init'"
        ) from err
    _x509_path = f'/scratch/{os.environ["USER"]}/{_x509_localpath.split("/")[-1]}'
    os.system(f"cp {_x509_localpath} {_x509_path}")
    _x509_path = os.path.basename(_x509_localpath)
    return _x509_path

def GetDefaultCondorClient(x509_path, max_workers=50, mem_size=2, disk_size=1):
    '''
    Get a dask.distributed.Client object that can be used for distributed computation with
    an HTCondorCluster. Assumes some default settings for the cluster, including a reasonable
    timeout, location for log/output/error files, and Singularity image file to ship.

    Inputs:
        x509_path: (str) Path to the x509 proxy to ship to workers

    Returns:
        (dask.distributed.Client) A client connected to an HTCondor cluster
    '''
    os.environ["CONDOR_CONFIG"] = "/etc/condor/condor_config"

    memory = str(mem_size) + " GB"
    disk = str(disk_size) + " GB"
    initial_dir = f"/scratch/{os.environ['USER']}"

    custom_sif = Path(f"/scratch/os.environ['USER']/notebook.sif")
    if custom_sif.is_file():
        sif_loc = str(custom_sif)
    else:
        sif_loc = DEFAULT_SIF
        
    cluster = HTCondorCluster(
        cores=1,
        memory=memory,
        disk=disk,
        death_timeout = '60',
        job_extra_directives={
            "+JobFlavour": '"tomorrow"',
            "log": "dask_job_output.$(PROCESS).$(CLUSTER).log",
            "output": "dask_job_output.$(PROCESS).$(CLUSTER).out",
            "error": "dask_job_output.$(PROCESS).$(CLUSTER).err",
            "should_transfer_files": "yes",
            "when_to_transfer_output": "ON_EXIT_OR_EVICT",
            "+SingularityImage": '"notebook.sif"',
            "Requirements": "HasSingularityJobStart",
            "InitialDir": initial_dir,
            "transfer_input_files": f'{x509_path},{sif_loc}', 
        },
        job_script_prologue=[
            "export XRD_RUNFORKHANDLER=1",
            f"export X509_USER_PROXY={x509_path}",
        ]
    )
    print('Condor logs, output files, error files in {}'.format(initial_dir))
    cluster.adapt(minimum=1, maximum=max_workers)
    client = Client(cluster)
    return client