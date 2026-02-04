import cowtools.jobqueue


def test_GetCondorClient():
    """
    Test for GetDefaultCondorClient()

    Make sure that we can initialize a default client with all default options
    """
    client = cowtools.jobqueue.GetCondorClient(
        container_image="docker://docker.io/coffeateam/coffea-dask-almalinux9:2025.12.0-py3.12"
    )
    with client:
        assert isinstance(client.ncores(), dict)
        client.shutdown()
