import os

from obiwan.log import logger

from obiwan.lidarchive.lidarchive import Lidarchive

class System:
    '''
    Helper class to describe a lidar system.
    '''
    def __init__ (self, file):
        self.file = None
        self.id = None
        self.channels = []
        
        self.ReadFromFile (file)
        
    def ReadFromFile (self, file):
        '''
        Reads a sample file to determine the lidar
        system configuration.
        
        Parameters
        ----------
        file : str
            Path of the raw lidar data file.
        '''
        self.file = file
        self.id = os.path.basename (file).rsplit('.', maxsplit=1)[0]
        self.measurement = Lidarchive.MeasurementFile(file)
        
    def Equivalent (self, measurement):
        '''
        Compares two lidar systems by comparing their channels.
        
        Parameters
        ----------
        system : System
            The system used for comparison.
        '''
        return self.measurement.HasSameChannelsAs (measurement)
        
class SystemIndex:
    '''
    Holds an index of lidar systems.
    '''
    def __init__ (self, folder = None):
        self.systems = []
        
        if folder is not None:
            self.ReadFolder (folder)
        
    def ReadFolder (self, folder):
        '''
        Reads an entire folder and identifies distinct lidar
        systems inside that folder by looking into the raw
        lidar data files.
        
        Parameters
        ----------
        folder : str
            Path of the folder holding the sample data files.
        '''
        files = [os.path.join(folder, f) for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
        
        for file in files:
            try:
                self.systems.append (System (file))
            except Exception:
                logger.warning ("File %s is not a valid sample file" % file)
                pass
                
        logger.debug(f"Can use System IDs {[s.id for s in self.systems]}")
        
    def GetSystemId (self, measurement):
        '''
        Retrieves the system ID for a
        specific measurement.
        
        Parameters
        ----------
        system : System
            The system you need to retrieve the ID for.
        '''
        compatible_ids = []
        
        for s in self.systems:
            if s.Equivalent (measurement):
                compatible_ids.append (s.id)
                
        if len(compatible_ids) == 0:
            raise ValueError ( "Couldn't find a matching configuration." )
        
        if len(compatible_ids) > 1:
            raise ValueError ( "More than one configuration matches." )
            
        return compatible_ids[0]
        
system_index = SystemIndex()
