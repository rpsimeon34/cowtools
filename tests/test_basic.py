import cowtools

def test_GetDefaultCondorClient():
    """
    Test for GetDefaultCondorClient()

    Make sure that we can initialize a default client with all default options
    """
    client = cowtools.GetDefaultCondorClient('resources/file1.txt')
    with client:
        assert type(client.ncores()) == dict
        client.shutdown()

def test_move_x509():
    """
    Test for move_x509()

    Try moving the x509 client and see if it returns a valid path
    """
    x509_path = cowtools.move_x509()
    #check that move_x509 at least returns a string
    assert type(x509_path) == str
