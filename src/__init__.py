import os
from dask_jobqueue import HTCondorCluster
from dask.distributed import Client

def move_x509():
    '''
    Get x509 path, copy it to the correct location, and return the path. Primarily
    to be used in preparation for creating an HTCondorCluster object (like 
    via GetDefaultCondorClient.
    '''
    os.environ["CONDOR_CONFIG"] = "/etc/condor/condor_config"

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
    timeout, location for log/output/error files, and 
    '''
    SINGULARITY_IMAGE = '"/cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-base:v2024.1.2"'
    print(f"Using image {SINGULARITY_IMAGE}. If this does not match your AF server's image, errors are likely at runtime.")
    memory = str(mem_size) + " GB"
    disk = str(disk_size) + " GB"
    initial_dir = f'/scratch/{os.environ["USER"]}'
    cluster = HTCondorCluster(
        cores=1,
        memory=memory,
        disk=disk,
        death_timeout = '60',
        #python="/usr/local/bin/python3.8",
        job_extra_directives={
            "+JobFlavour": '"tomorrow"',
            "log": "dask_job_output.$(PROCESS).$(CLUSTER).log",
            "output": "dask_job_output.$(PROCESS).$(CLUSTER).out",
            "error": "dask_job_output.$(PROCESS).$(CLUSTER).err",
            "should_transfer_files": "yes",
            "when_to_transfer_output": "ON_EXIT_OR_EVICT",
            "+SingularityImage": SINGULARITY_IMAGE,
            "Requirements": "HasSingularityJobStart",
            "InitialDir": initial_dir,
            "transfer_input_files": f'{x509_path}'
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