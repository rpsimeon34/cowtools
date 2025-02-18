from .jobqueue import GetCondorClient
from .data_tools import combine_rename_results,scale_results,XSecScaler

__all__ = [
    "GetCondorClient",
    "combine_rename_results",
    "scale_results",
    "XSecScaler"
]

# main
if __name__ == "__main__":
    # for testing at command line, won't be triggered by 'import cowtools'
    GetCondorClient()