import os

from obiwan.repository import MeasurementFile
from obiwan.log import logger

from pathlib import Path

import traceback

class System:
    """
    Class to describe a lidar system.
    
    Attributes:
        id (int): SCC System ID for this lidar system
        extra (:obj:`Dict`): Dictionary to hold any extra information about the lidar system.
        measurement (:obj:`MeasurementFile`): The sample measurement file to be used
            for comparisons with other files.
    """
    def __init__ (self, file : Path):
        """
        Args:
            file (:obj:`Path`): Path of the raw lidar data file.
        """
        self.id = None
        self.extra = None
        self.measurement = None
        
        self.ReadFromFile (file)
        
    def ReadFromFile (self, file : Path) -> None:
        """
        Read a sample file to determine the lidar system configuration.
        
        Args:
            file (:obj:`Path`): Path of the raw lidar data file.
        """
        
        # Syntax checking:
        # - Remove any file extension (that can be used for specifying multiple sample files for the same System ID)
        # - Make sure the remaining string can be converted to an int. Will throw a ValueError otherwise.
        
        parts = os.path.basename (file).rsplit('.', maxsplit=1)
        self.id = int ( parts[0] )
        
        if len(parts) > 1:
            self.extra = parts[1]
            
        self.measurement = MeasurementFile(file)
        
    def Equivalent (self, measurement : MeasurementFile) -> bool:
        """
        Compare two lidar systems by comparing their channels.
        
        Args:
            system (:obj:`MeasurementFile`): The system used for comparison.
            
        Returns:
            True if the systems are practically equivalent, False otherwise.
        """
        return self.measurement.HasSameChannelsAs (measurement)
        
    @property
    def name ( self ) -> str:
        """
        Get a user-friendly name of this system.
        
        Returns:
            The name as a string, computed from the ID and any extra information about the lidar system.
        """
        if type(self.extra) is str:
            return f"{self.id}.{self.extra}"
            
        return str( self.id )
        
class SystemIndex:
    """
    Class used to store information about known lidar systems, to quickly identify the right
    SCC System ID for raw measurement sets later on.
    
    Attributes:
        systems (:obj:`List` of :obj:`System`): List of identified lidar systems from the sample files
    """
    def __init__ (self, folder : Path = None):
        """
        Args:
            
        """
        self.systems = []
        
        if folder is not None:
            self.ReadFolder (folder)
        
    def ReadFolder (self, folder : Path) -> None:
        """
        Read an entire folder and identifies distinct lidar systems inside that folder.
        
        Args:
            folder (:obj:`Path`): Path of the folder holding the sample data files.
        """
        files = [os.path.join(folder, f) for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
        
        for file in files:
            try:
                self.systems.append (System (file))
            except Exception:
                logger.warning (f"File {file} is not a valid sample file.")
                pass
                
        logger.debug(f"Can use System IDs {', '.join([s.name for s in self.systems])}")
        
    def GetSystemId (self, measurement : MeasurementFile) -> int:
        """
        Retrieve the system ID for a specific data file.
        
        Args
        system (:obj:`MeasurementFile`): The data file to get the system ID for.
        """
        compatible_ids = []
        
        for s in self.systems:
            if s.Equivalent (measurement):
                compatible_ids.append (s.id)
                
        if len(compatible_ids) == 0:
            raise ValueError ( "Couldn't find a matching configuration." )
        
        if len(compatible_ids) > 1:
            raise ValueError ( "More than one configuration matches: %s" % compatible_ids )
            
        return int(compatible_ids[0])
