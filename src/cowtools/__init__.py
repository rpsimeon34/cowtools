import yaml
import os
from pathlib import Path
from dask_jobqueue import HTCondorCluster
from dask.distributed import Client

def GetCondorClient(x509_path=None, image_loc=None, max_workers=50, mem_size=2, disk_size=1):
    '''
    Get a dask.distributed.Client object that can be used for distributed computation with
    an HTCondorCluster. Assumes some default settings for the cluster, including a reasonable
    timeout, location for log/output/error files, and image file to ship.

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

    x509_path = _find_x509(x509_path)

    # set up job_extra_directives
    job_extra_directives = {
        "container_image": image_loc,
        "+JobFlavour": '"tomorrow"',
         "log": "dask_job_output.$(PROCESS).$(CLUSTER).log",
         "output": "dask_job_output.$(PROCESS).$(CLUSTER).out",
         "error": "dask_job_output.$(PROCESS).$(CLUSTER).err",
         "when_to_transfer_output": "ON_EXIT_OR_EVICT",
         "InitialDir": initial_dir,
         'transfer_input_files':[],
     }
    # create transfer_input_files list:
    if os.path.isfile(image_loc) and 'cvmfs' not in image_loc:
        # this is a local file not in CVMFS or docker and needs to be
        # transferred to Condor
        job_extra_directives['transfer_input_files'].append(image_loc)
    if x509_path is not None:
        job_extra_directives['transfer_input_files'].append(x509_path)

    if job_extra_directives['transfer_input_files'] == []:
        # no files are set to be transferred 
        del(job_extra_directives['transfer_input_files'])
    else:
        job_extra_directives['transfer_input_files'] = ','.join(job_extra_directives['transfer_input_files'])

    if 'transfer_input_files' in job_extra_directives:
       job_extra_directives['should_transfer_files'] = 'YES'

    print_debug(job_extra_directives)
    cluster = HTCondorCluster(
        cores=1,
        memory=memory,
        disk=disk,
        death_timeout = '60',
        job_extra_directives=job_extra_directives,
        job_script_prologue=[
            "export XRD_RUNFORKHANDLER=1",
            f"export X509_USER_PROXY={x509_path}",
        ]
    )
    print('Condor logs, output files, error files in {}'.format(initial_dir))
    print(f"Condor will run dask workers in container {image_loc}")
    cluster.adapt(minimum=1, maximum=max_workers)
    return Client(cluster)

def _find_image():
    custom_sif = Path(f"/scratch/{os.environ['USER']}/notebook.sif")
    #If there is a custom SIF at /scratch/${USER}/notebook.sif, use that
    if custom_sif.is_file():
        return str(custom_sif)

    #If there is no custom SIF, but an image source is given in container_info_file, use that
    container_info_file = Path(f"/container_info.yml")
    if container_info_file.is_file():
        with open(container_info_file) as f:
            container_info = yaml.safe_load(f)
            print_debug(container_info)
        try:
            container_source = container_info["container_source"]
        except KeyError:
            raise Exception(f"{container_info_file} is missing expected key 'container_source'")

        print_debug(container_source)
        # assume container_source is a valid container_image value
        best_loc=container_source
        # list of CVMFS directories to check
        cvmfsdirs = ['/cvmfs/unpacked.cern.ch/']
        # try to improve the container_image path to something more local
        for dir in cvmfsdirs:
            if os.path.isdir(dir):
                print_debug(f"Directory '{dir}' exists.")
                cvmfspath = os.path.join(dir)
                # try to find the same container in the local path
                if 'docker' in container_source:
                    # assume registry.hub.docker.com
                    cvmfspath = os.path.join(cvmfspath,'registry.hub.docker.com')
                if 'coffeateam' in container_source:
                    cvmfspath = os.path.join(cvmfspath,'coffeateam')

                # append filename to constructed path
                cvmfspath = os.path.join(cvmfspath,os.path.basename(container_source))
                print_debug('verifing existence of ' + cvmfspath)
                if os.path.exists(cvmfspath):
                    best_loc = cvmfspath
                    # assume first found location is best
                    break
                else:
                    print_debug(f"{cvmfspath} does not exist, so leaving as {best_loc}")
            else:
                print_debug(f"Directory '{dir}' does not exist, so leaving as {best_loc}")

        # container_source follows the Dockerfile syntax of
        # FROM container:tag
        # which omits the 'docker://' part of the container URL
        # so prepend that to make a valid container_image path
        if best_loc.startswith('docker.io'):
            best_loc=f"docker://{best_loc}"

        return best_loc

    raise Exception(f"""Could not automatically find an image to ship to workers.
                     This likely means that there is no metadata file "{container_info_file}".
                     Please explicitly specify the image to be used on workers to GetCondorClient
                     with the image_loc keyword.""")

def _find_x509(x509_path):
    '''
    Attempt to find the voms x509 proxy.
    '''

    if x509_path and os.path.isfile(x509_path):
        return x509_path
    elif x509_path:
        # The user supplied x509_path could not be found.
        print(f"Could not find voms proxy at {x509_path}, but continuing anyway.")
        print("Xrootd transfers will most likely fail.")
        return None
    else:
        # try to find voms proxy automatically
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
            print(f"Could not find voms proxy, but continuing anyway.")
            print("Xrootd transfers will most likely fail.")
            return None

        return _x509_localpath


# print when run from command line
def print_debug(message):
   if __name__ == "__main__":
       print(message)

# main
print('Basic usage:')
print('import cowtools')
print('client = cowtools.GetCondorClient()')
print('')
print('for more usage info visit https://github.com/rpsimeon34/cowtools')

if __name__ == "__main__":
    # for testing at command line, won't be triggered by 'import cowtools'
    GetCondorClient()
