import os

from obiwan.log import logger

from atmospheric_lidar.licel import LicelLidarMeasurement

class Channel:
    '''
    Helper class used to describe a lidar system channel.
    '''
    def __init__ (self, licel_channel):
        self.name = licel_channel.name
        self.resolution = licel_channel.resolution
        self.wavelength = licel_channel.wavelength
        self.laser_used = licel_channel.laser_used
        self.adcbits = licel_channel.adcbits
        self.analog = licel_channel.is_analog
        self.active = licel_channel.active
        
    def Equals (self, channel):
        '''
        Compares two lidar channels.
        
        Parameters
        ----------
        channel : Channel
            The channel used for comparison.
        '''
        if  (self.name == channel.name and
                self.resolution == channel.resolution and
                self.laser_used == channel.laser_used and
                self.adcbits == channel.adcbits and
                self.analog == channel.analog and
                self.active == channel.active):
            
            return True
            
        return False

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
        self.id = os.path.basename (file)
        measurement = LicelLidarMeasurement ([file])
        
        for channel_name, channel in measurement.channels.items():
            self.channels.append (Channel (channel))
            
        del measurement
        
    def Equals (self, system):
        '''
        Compares two lidar systems by comparing their channels.
        
        Parameters
        ----------
        system : System
            The system used for comparison.
        '''
        # Make a copy of the other system's channel list:
        other_channels = system.channels[:]
        
        if len(self.channels) != len(other_channels):
            return False
        
        for channel in self.channels:
            found = False
            for other_channel in other_channels:
                if channel.Equals (other_channel):
                    found = True
                    other_channels.remove (other_channel)
                    break
                    
            if found == False:
                return False
           
        if len (other_channels) > 0:
            return False
            
        return True
        
class SystemIndex:
    '''
    Holds an index of lidar systems.
    '''
    def __init__ (self, folder):
        self.systems = []
        
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
        
    def GetSystemId (self, system):
        '''
        Retrieves the system ID stored in the SystemIndex for a
        given lidar system. Used to determine the system ID for a
        specific measurement.
        
        Parameters
        ----------
        system : System
            The system you need to retrieve the ID for.
        '''
        compatible_ids = []
        system_obj = System (system)
        
        for s in self.systems:
            if system_obj.Equals (s):
                compatible_ids.append (s.id)
                
        if len(compatible_ids) == 0:
            raise ValueError ( "Couldn't find a matching configuration." )
        
        if len(compatible_ids) > 1:
            raise ValueError ( "More than one configuration matches." )
            
        return compatible_ids[0]