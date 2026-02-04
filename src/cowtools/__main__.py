# for testing at command line, won't be triggered by 'import cowtools'
# expand as convenient and then run:
# python3 -m path/to/module/dir/cowtools
from cowtools.jobqueue import GetCondorClient

GetCondorClient()
