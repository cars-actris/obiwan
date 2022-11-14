import datetime

from pathlib import Path
from typing import Union, List, Dict, Tuple

from obiwan.config import Config
from obiwan.data.types import FileInfo
from obiwan.repository import MeasurementSet

class LidarReader:
    """
    Abstract class for raw lidar data files readers. The methods of this class should be implemented by
    children for their respective supported file formats.
    """
    
    @staticmethod
    def read_info ( file : Path ) -> Union[FileInfo, None]:
        """
        Read basic information from the specified file.
        
        Note:
            This method can raise any type of Exception if you try to read a file
            with the wrong type, as error handling is left to the various Python
            modules used to read the raw data files.
        
        Args:
            file (:obj:`Path`): File path to read.
            
        Returns:
            :obj:`FileInfo` object if the file was successfully read, None otherwise.
        """
        raise NotImplementedError ("Each parsing method should be implemented by a specific class for the file type")
        
    @staticmethod
    def has_identifier ( info : FileInfo, identifier : str ) -> bool:
        """
        Check if a measurement is is characterized by a certain identifier (e.g.: Location information for Licel files).
        
        Args:
            info (:obj:`FileInfo`): File information to check.
            identifier(str): Strings that will be tested against relevant fields in the file.
            
        Returns:
            True if the file and identifier match, False otherwise.
        """
        raise NotImplementedError ("Each parsing method should be implemented by a specific class for the file type")
        
    @staticmethod
    def has_identifier_in_list ( info : FileInfo, identifiers: List[str] ) -> bool:
        """
        Check if a measurement is is characterized by a certain identifier (e.g.: Location information for Licel files).
        This is provided as an additional method besides `has_identifier()` in order to allow various reader plugins
        to optimize the process of checking the identifier list.
        
        Args:
            info (:obj:`FileInfo`): File information to check.
            identifiers(:obj:`list` of str): List of strings which will be tested against relevant fields in the file.
            
        Returns:
            True if the file and identifier match, False otherwise.
        """
        raise NotImplementedError ("Each parsing method should be implemented by a specific class for the file type")
        
    @staticmethod
    def convert_to_scc ( measurement_set : MeasurementSet, system_id : int, output_folder : Path, app_config : Config ) -> Tuple[Path, str]:
        """
        Convert a set of raw lidar data files to an SCC NetCDF input file.
        
        Args:
            measurement_set (:obj:`MeasurementSet`): The data set to convert.
            system_id (int): The SCC System ID for this lidar system.
            output_folder (:obj:`Path`): Path to the folder where to store the resulting NetCDF file.
            app_config (:obj:`Config`): Application configuration parameters.
                
        Returns:
            :obj:`Path` of the final SCC NetCDF input file. If the conversion cannot be done, it will return None.
            
        Returns:
            A :obj:`Tuple` containing the :obj:`Path` to the resulting NetCDF and the SCC Measurement ID.
        """
        raise NotImplementedError ("Each converstion method should be implemented by a specific class for the file type")