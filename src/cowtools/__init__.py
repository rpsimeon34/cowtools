from .jobqueue import GetCondorClient

__all__ = [
    "GetCondorClient"
]

# print when run from command line
def print_debug(message):
   if __name__ == "__main__":
       print(message)

if __name__ == "__main__":
    # for testing at command line, won't be triggered by 'import cowtools'
    GetCondorClient()