from enum import Enum

from datetime import datetime

from typing import List, Dict, Union, Optional
from pathlib import Path

class FileType(Enum):
    """
    Measurement types which Lidarchive knows about. This can be used in the processing chain.
    """
    UNKNOWN = 1
    LICEL_V1 = 2
    LICEL_V2 = 3

class ChannelInfo:
    """
    Helper class used to describe a lidar system channel.
    
    Attributes:
        name (str): String used to internally identify the channel.
        resolution (float): Height resolution of this channel in metres.
        wavelength (int): Detection wavelength of this channel in nm.
        laser_used (str): ID of the laser used in this channel.
        adcbits (int): ADC resolution in number of bits.
        analog (bool): True if the channel has analog detection, False if it uses photon counting.
        active (bool): True if the channel is actively used, False otherwise.
        number_of_shots (int, Optional): The number of shots taken in this channel. Defaults to 0.
    """

    def __init__(
        self, name : str,
        resolution : float,
        wavelength : int,
        laser_used : str,
        adcbits : int,
        analog : bool,
        active : bool,
        number_of_shots : Optional[int] = 0
    ):
        """
        Construct a `ChannelInfo` object.
        
        Args:
            name (str): String used to internally identify the channel.
            resolution (float): Height resolution of this channel in metres.
            wavelength (int): Detection wavelength of this channel in nm.
            laser_used (str): ID of the laser used in this channel.
            adcbits (int): ADC resolution in number of bits.
            analog (bool): True if the channel has analog detection, False if it uses photon counting.
            active (bool): True if the channel is actively used, False otherwise.
            number_of_shots (int, Optional): The number of shots taken in this channel. Defaults to 0.
        """
        self.name = name
        self.resolution = resolution
        self.wavelength = wavelength
        self.laser_used = laser_used
        self.adcbits = adcbits
        self.analog = analog
        self.active = active
        self.number_of_shots = number_of_shots,
        self.id = id

    def Equals(self, channel : 'ChannelInfo') -> bool:
        """
        Check if this channel is equivalent to another.
        
        Args:
            channel: The channel used for comparison.
            
        Returns:
            True if the two channels are identical, False otherwise.
        """
        if (self.name == channel.name and
                self.resolution == channel.resolution and
                self.laser_used == channel.laser_used and
                self.adcbits == channel.adcbits and
                self.analog == channel.analog and
                self.active == channel.active):
            return True

        return False
        
    @property
    def description (self) -> str:
        """
        Get the channel properties as a human-readable string.
        
        Returns:
            A string representing the main channel parameters.
        """
        return f"{self.name}: resolution={self.resolution} laser_used={self.laser_used} adcbits={self.adcbits} analog={self.analog} active={self.active}"
        
class FileInfo:
    """
    Data structure to hold information about a measurement file.
    Used an abstraction layer for all the supported file types.
    """
    def __init__ (
        self,
        start_time : datetime,
        end_time : datetime,
        location : str,
        channels : List[ChannelInfo],
        extra_info : Dict = {}
    ):
        """
        Build a MeasurementInfo data structure.
        
        Args:
            start_time: datetime object representing the start time of the measurement this file contains.
            end_time: datetime object representing the end time of the measurement this file contains.
            location: Arbitrary string which should identify the location this measurement was taken in.
            channels: List of ChannelInfo containing information about the channels present in the file.
            extra_info: Optional dictionary containing extra information that might be present in the file.
        """
        self.start_time = start_time
        self.end_time = end_time
        self.location = location
        self.channels = channels
        self.extra = extra_info
        