import yaml
import os
from pathlib import Path
from dask_jobqueue import HTCondorCluster
from dask.distributed import Client

# print when run from command line
def print_debug(message):
   if __name__ == "__main__":
       print(message)

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

def GetCondorClient(x509_path, image_loc=None, max_workers=50, mem_size=2, disk_size=1):
    '''
    Get a dask.distributed.Client object that can be used for distributed computation with
    an HTCondorCluster. Assumes some default settings for the cluster, including a reasonable
    timeout, location for log/output/error files, and Singularity image file to ship.

    Inputs:
        x509_path: (str) Path to the x509 proxy to ship to workers.
        image_loc: (str) Path to the image to be sent to worker nodes. Must be
                    something that HTCondor accepts under the "container_image"
                    classAd.

    Returns:
        (dask.distributed.Client) A client connected to an HTCondor cluster.
    '''
    os.environ["CONDOR_CONFIG"] = "/etc/condor/condor_config"

    memory = str(mem_size) + " GB"
    disk = str(disk_size) + " GB"
    initial_dir = f"/scratch/{os.environ['USER']}"

    if not image_loc:
        image_loc = _find_image()
        
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
            "container_image": f"{image_loc}",
            "InitialDir": initial_dir,
            "transfer_input_files": f'{x509_path},{image_loc}', 
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

def _find_image():
    custom_sif = Path(f"/scratch/{os.environ['USER']}/notebook.sif")
    container_info_file = Path(f"/tmp/container_info.yml")
    #If there is a custom SIF at /scratch/${USER}/notebook.sif, use that
    if custom_sif.is_file():
        return str(custom_sif)
    #If there is no custom SIF, but an image source is given in container_info_file, use that
    if container_info_file.is_file():
        with open(container_info_file) as f:
            container_info = yaml.safe_load(f)
            print_debug(container_info)
        try:
            container_source = container_info["container_source"]
        except KeyError:
            raise Exception(f"{container_info_file} is missing expected key 'container_source'")
        #For now, only using image source if it's from a Docker repo
        if container_source.startswith("docker.io"):
            return f"docker://{container_source}"
        else:
            raise Exception(f"""{container_info_file} indicates that the AF image is based on {container_source}.
                            However, only docker images (where container_source starts with "docker.io") can
                            currently be automatically detected and retrieved by cowtools. Please explicitly
                            specify the image to be used on workers to GetCondorClient with the image_loc
                            keyword.""")

    raise Exception(f"""Could not automatically find an image to ship to workers.
                     This likely means that there is no metadata file "{container_info_file}".
                     Please explicitly specify the image to be used on workers to GetCondorClient
                     with the image_loc keyword.""")

