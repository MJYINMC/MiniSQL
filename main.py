from Interface import Interface
from BufferManager.BufferManager import BufferManager as bf
from Utils.Utils import DBFILES_FOLDER
import os
if __name__ == '__main__':
    if not os.path.exists(DBFILES_FOLDER):
        os.mkdir(DBFILES_FOLDER)
    try:
        Interface.start()
        print("Saving changes to disk...")
        bf.flush()
    except KeyboardInterrupt:
        print("Saving changes to disk...")
        bf.flush()