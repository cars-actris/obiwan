from collections import OrderedDict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Union, List, Dict

from obiwan.data.types import FileType
    
def available_raw_file_readers () -> Dict[FileType, 'LidarReader']:
    """
    Get all available raw lidar data file readers.
    
    Returns:
        :obj:`Dict` containing :obj:`LidarReader` parsers, keyed by the
        :obj:`FileType` they are associated with.
    """
    
    # Import known modules here to avoid circular imports:
    from obiwan.data.readers.licel import LicelReader
    from obiwan.data.readers.licel_v2 import LicelV2Reader
    
    # We store the global directory of available reader plugins in the local
    # scope of this method just to avoid wrong interactions from other Python modules:
    return OrderedDict({
        # Licel V2 must come before Licel V1 because of backwards compatibility.
        # We want to test for Licel V2 compliancy before testing for Licel V1.
        FileType.LICEL_V2: LicelV2Reader, 
        FileType.LICEL_V1: LicelReader
    })

def get_reader_for_type ( file_type : FileType ) -> 'LidarReader':
    """
    Retrieve the correct reader class for the specified file type.
    
    Note:
        This method raises ValueError if the specified file type is not valid, or
        NotImplementedError if there is no reader class available for the specified
        file type.
    
    Args:
        file_type (:obj:`FileType`): The file type you need to get the reader class for.
        
    Returns:
        :obj:`LidarReader` subclass for the specified file type.
    """
    if type(file_type) is not FileType:
        raise ValueError ( "Invalid file type provided." )
        
    try:
        return available_raw_file_readers()[ file_type ]
    except Exception:
        raise NotImplementedError ( f"No parser available for file type {type}." )
        