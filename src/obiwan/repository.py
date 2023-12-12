import glob
import os
import shutil

from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path

from typing import Tuple, List, Dict, Optional, Union

from obiwan.data import available_raw_file_readers, get_reader_for_type
from obiwan.data.types import FileType
from obiwan.log import logger

import traceback

# from pollyxt_pipelines.polly_to_scc.pollyxt import PollyXTFile

licel_file_header_format = ['Filename',
                            'StartDate StartTime EndDate EndTime Altitude Longtitude Latitude ZenithAngle',
                            # Appart from Site that is read manually
                            'LS1 Rate1 LS2 Rate2 DataSets', ]
                            
class AlignmentType(Enum):
    """
    Continuous measurement sets split methods:
        NONE will split them based only on measurement length. When the maximum length is reached, the set will be split.
        SHARP_HOUR will center the measurement sets at sharp (xx:00) hours.
            First and last measurements from a set are being excepted from this rule (depending on when the measurement started/stopped).
        SHARP_HOUR_STRICT behaves like SHARP_HOUR but will not glue any data before or after the aligned segments.
        HALF_HOUR will center the measurement sets at half hours (xx:30).
            First and last measurements from a set are being excepted from this rule (depending on when the measurement started/stopped).
        HALF_HOUR_STRICT behaves like HALF_HOUR but will not glue any data before or after the aligned segments.
    """
    NONE = -1
    SHARP_HOUR = 0
    SHARP_HOUR_STRICT = 1
    HALF_HOUR = 2
    HALF_HOUR_STRICT = 3
    
    @property
    def minute ( self ):
        """
        Get what minute of the hour this should align to as a number.
        
        Returns:
            :obj:`int` between 0 and 59
        """
        if self == AlignmentType.HALF_HOUR or self == AlignmentType.HALF_HOUR_STRICT:
            return 30
        
        return 0
        
    @property
    def is_strict ( self ):
        """
        Check if this type is strict. Strict alignment types do not allow for leading or trailing data gluing.
        
        Returns:
            True if this alignment is strict, False otherwise.
        """
        if self == AlignmentType.SHARP_HOUR_STRICT or self == AlignmentType.HALF_HOUR_STRICT:
            return True
            
        return False
    
class SplitStart(Enum):
    """
    Where to start from when performing a measurement set split.
    START will start from the first measurement and will go forward in time, splitting the measurement set appropriately.
    END will start from the last measurement and go back in time.
    """
    START = -1
    END = 1
    
class MeasurementFile:
    """
    Class representing a single lidar data file. This represents an abstraction layer over the various
    file formats that the data could be stored in.
    
    Attributes:
        info (:obj:`FileInfo`): Information that was read from the file. None if file is not a supported file type.
        type (:obj:`FileType`): The identified file format, based on which parses could successfully read it.
        path (:obj:`Path`): The absolute path of this data file.
        parser (:obj:`LidarReader`): The parser that read the information from this data file.
    """
    def __init__ ( self, path : Path ):
        """
        Read a lidar measurement file.
        
        Note:
            This method will try to automatically identify the format of the file
            by successively calling all the available file parsers until the file is properly read.
        
        Args:
            path: Path to the file you want to read.
        """
        
        self.path = path
        self.type = FileType.UNKNOWN
        self.info = None
        self.parser = None
        
        for type, reader in available_raw_file_readers().items():
            # Try each available parser in succession. If any parser fails, we know this is not the right
            # file type.
            try:
                # Save information read from the file, the type of the file format
                # as well as the parser that succeeded reading the file - in order to not always
                # search for it.
                self.info = reader.read_info ( path )
                self.type = type
                self.parser = reader
            
                break
            except Exception:
                # Not the right parser perhaps? We'll try other ones.
                continue
        
        if self.type == FileType.UNKNOWN:
            # If no parser could successfully read the file, it means this is an unsupported file type.
            raise ValueError (f"Could not read file {path}")

    def IsDark(self, dark_identifiers: List[str] = [ "Dark" ]) -> bool:
        """
        Check if a given measurement represents a dark measurement.
        
        Args:
            dark_identifiers: List of strings which will be tested against relevant fields in the file.
            
        Returns:
            True if the file is identified as a dark measurement, False otherwise.
        """
        try:
            return self.parser.has_identifier_in_list ( self.info, dark_identifiers )
        except Exception:
            return False

    def Path(self) -> Path:
        """
        Retrieve the path of this measurement file.
        
        Returns:
            Path to the file.
        """
        return self.path
        
    def Filename(self) -> str:
        """
        Retrieve the name of this measurement file.
        
        Note:
            The filename is obtained from the file path.
        
        Returns:
            The file name.
        """
        return os.path.basename(self.path)

    def StartDateTime(self) -> datetime:
        """
        Retrieve the start date of measurements this file contains.
        
        Returns:
            datetime object representing the start date of measurement.
        """
        return self.info.start_time

    def EndDateTime(self) -> datetime:
        """
        Retrieve the end date of measurements this file contains.
        
        Returns:
            datetime object representing the end date of measurement.
        """
        return self.info.end_time

    def Site(self) -> str:
        """
        Retrieve the location where the measurement this file belongs to was taken.
        
        Returns:
            String representing the location.
        """
        return self.info.location

    def Type(self) -> FileType:
        """
        Retrieve the identified file format type.
        
        Returns:
            File type.
        """
        return self.type

    def HasSameChannelsAs(self, measurement : 'MeasurementFile') -> bool:
        """
        Check if this measurement file contains same channels as another measurement file.
        
        Args:
            measurement (:obj:`MeasurementFile`): The measurement file to compare the channels to.
        
        Returns:
            True if both measurement files have the same channels. False otherwise.
        """
        # Make a copy of the other system's channel list:
        other_channels = measurement.info.channels[:]

        # Check if both files have the same number of channels:
        if len(self.info.channels) != len(other_channels):
            return False
            
        # Check if we can find every channel in both files:
        for channel in self.info.channels:
            found = False
            for other_channel in other_channels:
                if channel.Equals(other_channel):
                    found = True
                    other_channels.remove(other_channel)
                    break

            if found == False:
                return False

        # If there's any extra channel that somehow we missed
        # it means the two files do not contain the exact same
        # channels.
        if len(other_channels) > 0:
            return False

        return True
        
    def NumberOfShotsSimilarTo ( self, measurement : 'MeasurementFile', max_relative_diff : Optional[float] = .0 ) -> bool:
        """
        Check if the channels in this measurement file have the same number of shots
        as the channels in another measurement file.
        
        Args:
            measurement (:obj:`MeasurementFile`): The measurement file to compare the channels to.
            max_relative_diff (float, Optional): Maximum accepted relative difference between the number of shots.
        
        Returns:
            True if the channels have the same number of shots, False otherwise.
        """
        for channel in self.info.channels:
            same_n_shots = False
            
            for other_channel in measurement.info.channels:
                number_of_shots = channel.number_of_shots
                if type(number_of_shots) is tuple:
                    number_of_shots = number_of_shots[0]
                    
                other_number_of_shots = other_channel.number_of_shots
                if type(other_number_of_shots) is tuple:
                    other_number_of_shots = other_number_of_shots[0]
                    
                if channel.Equals (other_channel):
                    try:
                        relative_diff = abs((number_of_shots - other_number_of_shots) / other_number_of_shots) * 100.0
                    except ZeroDivisionError:
                        relative_diff = float('inf')
                        
                    same_n_shots = relative_diff <= max_relative_diff
                    break
                    
            if not same_n_shots:
                # We found different number of shots for one of the channels.
                # No need to look further
                logger.debug (f"{self.Filename()} vs. {measurement.Filename()}: Different number of shots in channel {channel.name} ({channel.number_of_shots} vs {other_channel.number_of_shots})")
                return False
                
        # Every channel turned out to have the same number of shots,
        # because we didn't return False from inside the loop.
        return True

class MeasurementSet:
    """
    Class used to store a continuous lidar measurement. A Measurement contains any number of MeasurementFiles.
    """
    def __init__(self, dark : List[MeasurementFile], data : List[MeasurementFile], number : int):
        """
        Construct a Measurement object.
        
        Args:
            dark: List of dark measurement files
            data: List of atmosphere measurement files
            number: Sequence number used to differentiate between measurements taken during the same time.
        """
        self.dark_files = []
        self.data_files = []
        
        start_times_seen = set()
        for dark_file in dark:
            # Make sure we don't accidentally have two data files
            # with the same start date. This would actually mean that file is duplicate,
            # and that will cause problems with certain converters.
            start_time = dark_file.StartDateTime()
            
            if start_time not in start_times_seen and not start_times_seen.add(start_time):
                self.dark_files.append ( dark_file )
        
        start_times_seen = set()
        for data_file in data:
            # Do the same for atmosphere measurement files.
            start_time = data_file.StartDateTime()
            
            if start_time not in start_times_seen and not start_times_seen.add(start_time):
                self.data_files.append ( data_file )

        self.number = number

    def DarkFiles(self) -> List[MeasurementFile]:
        """
        Retrieve the dark measurement files in this measurement set.
        
        Returns:
            List of MeasurementFile objects representing dark measurements.
        """
        return self.dark_files

    def DataFiles(self) -> List[MeasurementFile]:
        """
        Retrieve the atmosphere measurement files in this measurement set.
        
        Returns:
            List of MeasurementFile objects representing atmosphere measurements.
        """
        return self.data_files

    def Number(self) -> int:
        """
        Retrieve the sequence number of this data set.
        
        Returns:
            Integer representing the sequence number.
        """
        return self.number

    def NumberAsString(self) -> str:
        """
        Retrieve the sequence number of this data set as a string.
        
        Returns:
            Four characters zero-padded string representing the squence number.
        """
        return "%04d" % (self.number)
        
    def Id(self) -> str:
        """
        Retrieve unique identifier of this measurement set based on the start time and sequence number.
        
        Returns:
            Four characters zero-padded string representing the squence number.
        """
        try:
            date = self.DataFiles()[0].StartDateTime().strftime("%Y%m%d")
            
            return f"{date}_{self.NumberAsString()}"
        except Exception:
            return "UNKNOWN_MEASUREMENT"
            
    def Type(self) -> FileType:
        """
        Retrieve the file type used in this measurement set.
        
        Note:
            This method checks that all measurement files in this set have the same type.
            If this is not the case, by convention the set will have an unknown type.
        
        Returns:
            FileType object representing the file format used in this measurement set.
        """
        try:
            # Check if all measurement files are of the same type.
            all_measurements_same_type = len (
                set (
                    [ m.Type() for m in self.data_files ]
                )
            ) == 1
            
            if all_measurements_same_type:
                # If all measurements are of the same type, we can return the first file's type:
                return self.data_files[0].Type()
                
            # Otherwise we can't say this measurement set has type:
            return FileType.UNKNOWN
        except Exception:
            # If we don't have any data files, this measurement set does not have any type:
            return FileType.UNKNOWN
            
    def StartDateTime (self) -> Union[datetime, None]:
        """
        Retrieve the start time of this measurement set if available.
            
        Returns:
            Start :obj:`datetime` of the first data file in this set. If no data files
                are present in this set, it will return None.
        """
        try:
            return self.data_files[0].StartDateTime()
        except Exception:
            return None
            
    def EndDateTime (self) -> Union[datetime, None]:
        """
        Retrieve the end time of this measurement set if available.
            
        Returns:
            End :obj:`datetime` of the last data file in this set. If no data files
                are present in this set, it will return None.
        """
        try:
            return self.data_files[-1].EndDateTime()
        except Exception:
            return None
            
class LidarTest:
    """
    Class to hold information about a lidar test procedure. This is mainly used to compare measurements against it
    in order to verify if they are real or test measurements.
    """
    
    def __init__ ( self, name : str, test_identifiers : List[str] ):
        """
        Args:
            name (str): Name of the test.
            test_identifiers (:obj:`list` of :obj:`str`): List of strings representing test identifiers (e.g. location used in Licel header)
        """
        self.name = name
        self.test_identifiers = test_identifiers
        
    def MeasurementValid ( self, measurement : 'MeasurementFile' ) -> bool:
        """
        Check if a measurement file is considered valid for this test.
        
        Args:
            measurement (:obj:`MeasurementFile`): The MeasurementFile object you want to check.
            
        Returns:
            True if measurement is part of this lidar test, False otherwise.
        """
        try:
            return measurement.parser.has_identifier_in_list ( measurement.info, self.test_identifiers )
        except Exception:
            return False
        
    def CheckTest (
        self,
        test_files : List['MeasurementFile'],
        strict : bool = True
    ) -> bool:
        """
        Check if the test is complete from the given files.
        
        Args:
            test_files (:obj:`list` of :obj:`MeasurementFile`): Files to test if they form a complete test
            strict (bool): If True, all file types describing this test need to be present. If False,
                the presence of a single file from this test means the test is done. Defaults to True.
                
        Returns:
            True if the test is considered to be done, False otherwise.
        """
        # No need to check any further:
        if len(test_files) < len(self.test_identifiers) and strict:
            return False
            
        # If we're running in non-strict mode we basically want
        # at least one file. If we get that, test is considered valid.
        if len(test_files) > 0 and not strict:
            return True
            
        # Check all required types of files are present:
        for test_type in self.test_identifiers:
            test_valid = False
            for file in test_files:
                if self.MeasurementValid ( file ):
                    test_valid = True
                    break
                    
            if not test_valid:
                # Test type was not found. Can stop looking.
                return False
                
        return True

class Lidarchive:
    """
    Class used to store a repository of lidar measurements from a specific folder. The repository can contain
    different types of lidar measurement raw files, in any folder structure. The entire folder tree will be scanned
    and measurement sets will be identified, with the possibility of finding continuous measurements (with acceptable
    time gaps), as well as splitting the datasets based on length.
    
    Continous measurement identification is cached in order to improve performance for multiple calls. The parameters
    used for identifying said continuous measurements are available as instance attributes.
    
    Attributes:
        folder (:obj:`Path`): Folder where the measurement files are located
        measurements (:obj:`list` of :obj:`MeasurementFile`): List of measurement files identified in the folder.
        accepted_gap (int): Maximum acceptable time gap (in seconds) between two measurement files in order to
            treat them as being part of the same continuous measurement.
        accepted_min_length (int): Minimum acceptable measurement set length (in seconds). Measurement sets shorter
            than this will be discarded.
        accepted_max_length (int): Maximum acceptable measurement set length (in seconds). Measurement sets longer
            than this will be automatically split.
        accepted_alignment_type (:obj:`SplitType`): The ype of split done on continuous measurement sets.
        continuous_measurements (:obj:`list` of :obj:`MeasurementSet`): The cached continuous measurement sets,
            split based on the parameters above.
        tests (:obj:`list` of :obj:`LidarTest`): Information about lidar test files that should be identified in the
            repository.
        dark_identifiers (:obj:`list` of :obj:`str`): List of dark identifiers, as strings,
            used to filter dark measurement files from other types.
        measurement_identifiers (:obj:`list` of :obj:`str`): List of dark identifiers, as strings,
            used to filter atmosphere measurement files from other types.
    """

    def __init__(
        self,
        folder : Optional[Path] = None,
        tests : List[LidarTest] = [],
        dark_identifiers = List[str],
        measurement_identifiers = List[str]
    ):
        """
        Args:
            folder (Path, optional): Path to the folder which you want to scan for lidar measurements.
            tests (:obj:`list` of :obj:`LidarTest`): List of lidar system tests that will be identified
                and will not be considered actual atmosphere measurements.
            dark_identifiers (:obj:`list` of :obj:`str`): List of dark identifiers, as strings,
                used to filter dark measurement files from other types.
            measurement_identifiers (:obj:`list` of :obj:`str`): List of dark identifiers, as strings,
                used to filter atmosphere measurement files from other types.
        """
        
        self.SetFolder ( folder )
        
        self.tests = tests
        self.dark_identifiers = dark_identifiers
        self.measurement_identifiers = measurement_identifiers

    def SetFolder(self, folder : Path) -> None:
        """
        Sets the repository root folder which can be scanned later.

        Args:
            folder (Path, optional): Path to the folder which you want to scan for lidar measurements.
        """
        if folder is not None:
            new_folder = os.path.abspath(folder)
        else:
            new_folder = None
            self.folder = None
            
        if new_folder != self.folder or new_folder is None:
            self.folder = folder
            self.measurements = []
            self.ResetCache()
        
    def ResetCache (self) -> None:
        """
        Reset the continuous measurement set list cache.
        """
        self.accepted_gap = 0
        self.accepted_min_length = 0
        self.accepted_max_length = 0
        self.accepted_alignment_type = AlignmentType.NONE
        self.continuous_measurements = []

    def MeasurementWasSent(self, last_end, min_length, max_length):
        """
        Check if current received measurements are meeting the length requirements

        Parameters
        -----------

        last_end: str
            the end time of last sent measurement

        min_length: int
            Minimum accepted length, measured in seconds, of a set of measurements

        max_length: int
            Maximum accepted length, measured in seconds, of a set of measurements

        Returned values
        -----------------
        Boolean which tells if the current set of measurements was sent

        last_end: datetime
            Updated variable of the last set of measurements sent
        """

        index = 0
        for measurement in self.measurements:
            if measurement.IsDark( self.dark_identifiers ):
                index += 1
        #ignore dark files

        current_end = self.measurements[-1].EndDateTime()
        current_start = self.measurements[index].StartDateTime()


        if str(current_end) == last_end:
            return (True, last_end)

        last_end = current_end

        if (current_end - current_start).total_seconds() >= max_length + min_length:
            return (True, last_end)

        return (False, last_end)

    @staticmethod
    def SplitMeasurements(
        measurements : List[MeasurementFile],
        max_gap : int,
        min_length : int,
        max_length : int,
        alignment_type : AlignmentType,
        same_location : bool = True,
        same_type : bool = True,
        same_system : bool = True,
        same_folder : bool = True,
    ) -> List[List[MeasurementFile]]:
        """
        Split measurement sets based on the selected criteria. This is especially useful for creating hour-long
        measurement sets for further conversion or processing using the Single Calculus Chain.

        Args:
            measurements (:obj:`list` of `MeasurementFile`): Lidar measurement files.
            max_gap (int): Maximum acceptable time gap in seconds between two measurements.
            min_length (int): Minimum accepted length, measured in seconds, of a set of measurements
            max_length (int): Maximum accepted length, measured in seconds, of a set of measurements
            alignment_type (:obj:`AlignmentType`): The type of alignment to be performed on the measurement sets.
            same_location (bool): If True, all measurements in a set will need to have the same location identifier. Defaults to True.
            same_type (bool): If True, all measurements in a set will need to be of the same file type. Defaults to True.
            same_system (bool): If True, all measurements in a set will need to be originate from the same lidar system.
                This check is done by comparing the channels present in each file. Defaults to True.
            same_folder (bool): If True, it requires all files to be in the same folder, otherwise they won't be considered to
                be part of the measurement set. Defaults to True
                
        Returns:
            :obj:`list` :obj:`list` of :obj:`Measurement` after being split on the specified criteria.
        """
        if len(measurements) < 1:
            return []

        distinct_sets = []
        
        split_measurements = []
        folder_split_measurements = []
        
        if same_folder:
            # First split measurement sets by folder if required
            measurements_by_folder = {}
            
            for measurement in measurements:
                folder = os.path.abspath ( os.path.dirname ( measurement.Path() ) )
                
                if folder not in measurements_by_folder.keys():
                    measurements_by_folder[ folder ] = []
                    
                measurements_by_folder[ folder ].append ( measurement )
                
            for folder_data in measurements_by_folder.values():
                folder_split_measurements.append ( folder_data )
        else:
            folder_split_measurements = [ measurements ]
        
        if same_type:
            # Split measurement sets by file type if required
            for split in folder_split_measurements:
                test = set( [os.path.dirname(m.Path()) for m in split] )
                
                measurements_by_type = {}
                
                for type in FileType:
                    measurements_by_type[ type ] = []
                    
                for measurement in split:
                    measurements_by_type[ measurement.Type() ].append ( measurement )
                    
                for type in measurements_by_type.keys():
                    split_measurements.append ( measurements_by_type[type] )
            else:
                split_measurements = folder_split_measurements

        for split in split_measurements:
            if len(split) < 1:
                # We have no measurement in this split
                continue
                
            cset = [split[0]]
            last_measurement = split[0]
            
            # Split by gap, location and system if requested:
            for measurement_index in range(1, len(split)):
                previous_end = split[measurement_index - 1].EndDateTime()
                current_start = split[measurement_index].StartDateTime()

                gap_trigger = (current_start - previous_end).total_seconds() > max_gap
                system_trigger = False
                
                if same_system:
                    system_trigger = split[measurement_index].HasSameChannelsAs(last_measurement) == False

                location_trigger = False
                if same_location:
                    if split[measurement_index - 1].Site() != split[measurement_index].Site():
                        location_trigger = True

                if gap_trigger or location_trigger or system_trigger:
                    distinct_sets.append(cset)

                    cset = []
                    last_measurement = split[measurement_index]

                cset.append(split[measurement_index])

            if len(cset) > 0:
                distinct_sets.append(cset)
                
        final_sets = []
        
        # Split by length and perform time alignment on the measurement sets if required
        for measurement_set in distinct_sets:
            final_sets.extend ( Lidarchive.SplitByTime ( measurement_set, min_length = min_length, max_length = max_length, alignment_type = alignment_type ) )

        return final_sets

    @staticmethod
    def SplitByTime(
        measurements : List[MeasurementFile],
        min_length : int,
        max_length : int,
        alignment_type : AlignmentType
        ) -> List[List[MeasurementFile]]:
        """
        Retrieve sets of measurements split into groups based on minimum and maximum allowed
        length of the set.
        
        Note:
            This method will always try to result in 1-hour long measurement sets.

        Args:
            measurements (:obj:`list` of :obj:`Measurement`): A list of measurements
            min_length (int): Minimum accepted length, measured in seconds, of a set of measurements
            max_length (int): Maximum accepted length, measured in seconds, of a set of measurements
            alignment_type (:obj:`AlignmentType`): The type of alignment to be performed on the measurement sets.

        Returns:
            :obj:`list` :obj:`list` of :obj:`MeasurementFile` after being split on the specified criteria.
        """
        remaining_hours = int(max_length / 3600)

        if len(measurements) < 1:
            return []
            
        if alignment_type == AlignmentType.NONE:
            return Lidarchive.SplitByLength ( measurements = measurements, min_length = min_length, max_length = max_length )
        
        start_index = 0
        last_time_difference = 60
        minute_marker = alignment_type.minute
        
        # First data file marks the beginning of a set.
        # If doesn't satisfy any criteria it will be filtered out later.
        split_indexes = []
        splits = []
        
        # Go through every measurement and store every local minimum
        # in time difference compared to the minute marker:
        for index, measurement in enumerate ( measurements ):
            start_minute = measurement.StartDateTime().minute
            
            # Look both ahead and behind in time for the closest minute marker:
            time_difference = minute_marker - start_minute
            
            # Split measurement sets whenever we pass the minute marker:
            should_split = time_difference <= 0 and last_time_difference > 0
            
            if should_split:
                # Time difference to the required minute mark has started
                # rising, so we store the index where to perform the split
                # and reset the time difference.
                last_time_difference = -99
                split_indexes.append (index)
            else:
                # We are getting closer to the required minute mark
                # but we still need to check further measurements
                last_time_difference = time_difference
                
        # Did we actually do any alignment split?
        # If not, we should just copy the data as it is
        # without splitting it.
        if len(split_indexes) < 1:
            return Lidarchive.SplitByLength ( measurements = measurements, min_length = min_length, max_length = max_length )
            
        # We do have some splits to perform:
        for i in range ( 0, len (split_indexes) - 1 ):
            # Copy measurements from each split index, right before the following split index.
            start_index = split_indexes[i]
            end_index = split_indexes[i+1]
            
            splits.append(measurements[ start_index : end_index ])
            
        segments = []
        # Split measurements by length:
        for split in splits:
            segments.extend ( Lidarchive.SplitByLength ( measurements = split, min_length = min_length, max_length = max_length ) )
            
        # Check if we have data before the first split:
        if split_indexes[0] != 0:
            # Compute the time length of the data before the first split
            leading_data = measurements[ : split_indexes[0] ]
            
            if len(leading_data):
                leading_length = (leading_data[-1].EndDateTime() - leading_data[0].StartDateTime()).total_seconds()
                
                if leading_length < min_length:
                    # If the data before the first split is too short to be a standalone measurement
                    # we can glue it to the first segment:
                    if not alignment_type.is_strict:
                        # Glue the data to the first split if we are not in a strict mode:
                        if len (segments) > 0:
                            segments[0] = leading_data + segments[0]
                        else:
                            segments = Lidarchive.SplitByLength ( measurements = leading_data, min_length = min_length, max_length = max_length )
                        
                    # If we are in a strict mode we simply discard the data.
                else:
                    # If data before the first split is long enough, we treat it as a standalone measurement:
                    segments = Lidarchive.SplitByLength ( measurements = leading_data, min_length = min_length, max_length = max_length, allow_glue = not alignment_type.is_strict ) + segments
                
        # Finally, do the same thing for trailing data:
        trailing_data = measurements [ split_indexes[-1] : ]
        
        if len (trailing_data):
            trailing_length = (trailing_data[-1].EndDateTime() - trailing_data[0].StartDateTime()).total_seconds()
            
            if trailing_length < min_length:
                if not alignment_type.is_strict:
                    if len(segments) > 0:
                        segments[-1].extend(trailing_data)
                    else:
                        segments = Lidarchive.SplitByLength ( measurements = trailing_data, min_length = min_length, max_length = max_length )
            else:
                segments.extend( Lidarchive.SplitByLength ( measurements = trailing_data, min_length = min_length, max_length = max_length, start = SplitStart.END, allow_glue = not alignment_type.is_strict ) )
                
            for index, set in enumerate (segments):
                set_length = (set[-1].EndDateTime() - set[0].StartDateTime()).total_seconds()
                
        return segments

    @staticmethod
    def SplitByLength(
        measurements : List[MeasurementFile],
        min_length : int,
        max_length : int,
        start : SplitStart = SplitStart.START,
        allow_glue: bool = True
    ) -> List[List[MeasurementFile]]:
        """
        Retrieve sets of measurement files split into groups based on minimum and maximum allowed
        length of the set.

        Args:
            measurements (:obj:`list` of :obj:`Measurement`): A list of measurements
            min_length (int): Minimum accepted length, measured in seconds, of a set of measurements
            max_length (int): Maximum accepted length, measured in seconds, of a set of measurements
            start (:obj:`SplitStart`, optional): Where to start parsing the measurement list.

        Returns:
            :obj:`list` :obj:`list` of :obj:`MeasurementFile` after being split on the specified criteria.
        """
        if len(measurements) < 1:
            return []

        last_end = measurements[-1].EndDateTime()
        segment_start = measurements[0].StartDateTime()
        segments = []
        segment = [measurements[0]]
        
        if start == SplitStart.END:
            index_generator = reversed ( range(0, len(measurements)-1) )
        else:
            index_generator = range(1, len(measurements))

        for index in range(1, len(measurements)):
            current_start = measurements[index].StartDateTime()
            current_end = measurements[index].EndDateTime()

            if (last_end - current_start).total_seconds() < min_length and (last_end - segment_start).total_seconds() > min_length and allow_glue:
                segment.extend(measurements[index:])
                segments.append(segment)
                
                # We must reset the segment variable, otherwise
                # we will have residual values which will duplicate
                # real data files.
                segment = []
                break

            if (current_end - segment_start).total_seconds() > max_length:
                segments.append(segment)
                segment = [measurements[index]]
                segment_start = measurements[index].StartDateTime()
                continue
            
            segment.append(measurements[index])
            
        # We might have a residual open segment.
        # Check if it satisfies the length criteria before adding it
        # to the final array.
        if len(segment) > 0:
            segment_length = (segment[-1].EndDateTime() - segment[0].StartDateTime()).total_seconds()
            if segment_length > min_length:
                segments.append(segment)

        return segments
        
    def CopyTestFiles (
        self,
        out_folder : Path,
        date_format : str = "%Y-%m-%d-%H-%M",
        strict : bool = True
    ) -> bool:
        """
        Copy test files to the specified folder. It will split them into subfolders based on test date.

        Args:
            out_folder (:obj:`Path`): Path to the folder where we want to copy the files. If it does not exist, it will be created.
            date_format (str): Date format used to create subfolders for each of the tests. Defaults to "%Y-%m-%d-%H-%M".
            strict (bool): If True, all test files must be present or else the test will be ignored. Defaults to True
            
        Returns:
            True if the function succeeded, False otherwise. The type of alignment to be performed on the measurement sets.
        """
        if len ( self.measurements ) < 1:
            return False
            
        potential_tests = dict([ (test.name, []) for test in self.tests ])
        valid_tests = []
            
        # Run through all measurements to see if any test was done:
        for measurement in self.measurements:
            for test in self.tests:
                if test.MeasurementValid ( measurement ):
                    # Measurement could be part of this test:
                    potential_tests[ test.name ].append (measurement)
                else:
                    # This measurement is not part of the test, so let's see
                    # if the test has completed until now:
                    test_files = potential_tests.get( test.name, [] )
                    potential_tests[ test.name ] = []
                    
                    if test.CheckTest ( test_files, strict ):
                        subfolder_name = test_files[0].StartDateTime().strftime(date_format)
                        test_subfolder = os.path.join ( out_folder, test_name, subfolder_name )
                        
                        if not os.path.isdir ( test_subfolder ):
                            os.makedirs ( test_subfolder )
                        
                        for file in test_files:
                            shutil.copy2(file.Path(), test_subfolder)
                            
        # Do a final check on remaining data:
        for test in self.tests:
            test_files = potential_tests.get( test.name, [] )
            potential_tests[ test.name ] = []
            
            if test.CheckTest ( test_files, strict ):
                subfolder_name = test_files[0].StartDateTime().strftime(date_format)
                test_subfolder = os.path.join ( out_folder, test_name, subfolder_name )
                
                if not os.path.isdir ( test_subfolder ):
                    os.makedirs ( test_subfolder )
                
                for file in test_files:
                    shutil.copy2(file.Path(), test_subfolder)


    def ContinuousMeasurements(
        self,
        max_gap : int = 300,
        min_length : int = 1800,
        max_length : int = 3600,
        alignment_type : AlignmentType = 0
    ) -> List[MeasurementSet]:
        """
        Retrieve the continuous measurement sets based on the given criteria.

        Args:
            max_gap (int): Maximum acceptable time gap between two measurement files in seconds.
            min_length (int): Minimum acceptable length for a measurement in seconds.
            max_length (int): Maximum acceptable length for a measurement in seconds. Measurements longer
                than this value will be split.
            alignment_type (:obj:`AlignmentType`): The type of alignment to be performed on the measurement sets.

        Returns:
            :obj:`list` of :obj:`MeasurementSet`
        """
        if len(self.continuous_measurements) == 0:
            self.ComputeContinuousMeasurements(max_gap, min_length, max_length, alignment_type)

        if self.accepted_gap != max_gap:
            self.ComputeContinuousMeasurements(max_gap, min_length, max_length, alignment_type)

        if self.accepted_min_length != min_length:
            self.ComputeContinuousMeasurements(max_gap, min_length, max_length, alignment_type)

        if self.accepted_max_length != max_length:
            self.ComputeContinuousMeasurements(max_gap, min_length, max_length, alignment_type)

        if self.accepted_alignment_type != alignment_type:
            self.ComputeContinuousMeasurements(max_gap, min_length, max_length, alignment_type)

        return self.continuous_measurements
        
    def ContinuousDarkMeasurements(self, max_gap : int, min_length : int, max_length : int, alignment_type : AlignmentType) -> List[List[MeasurementFile]]:
        """
        Retrieve continuous sets of dark measurements.
        
        Args:
            max_gap (int): Maximum acceptable time gap between two measurement files in seconds.
        
        Returns:
            :obj:`list` of :obj:`list` of :obj:`MeasurementFile`
        """
        dark_measurements = [ m for m in self.measurements if m.IsDark ( self.dark_identifiers ) ]
        
        dark_segments = self.SplitMeasurements ( dark_measurements, max_gap, min_length, max_length, alignment_type, same_location = True, same_type = True, same_system = True )
        
        return dark_segments
        
    def ContinuousDataMeasurements(self, max_gap : int, min_length : int, max_length : int, alignment_type : AlignmentType) -> List[List[MeasurementFile]]:
        """
        Retrieve continuous sets of atmosphere measurements.
        
        Args:
            max_gap (int): Maximum acceptable time gap between two measurement files in seconds.
        
        Returns:
            :obj:`list` of :obj:`list` of :obj:`MeasurementFile`
        """
        data_measurements = [ m for m in self.measurements if not m.IsDark ( self.dark_identifiers ) and m.Site() in self.measurement_identifiers ]
        
        data_segments = self.SplitMeasurements ( data_measurements, max_gap, min_length, max_length, alignment_type, same_location = True, same_type = True, same_system = True )
        
        return data_segments
        
    @staticmethod
    def ClosestDarkSegment (
        data_segment : List[MeasurementFile],
        dark_segments : List[List[MeasurementFile]],
        type : FileType = None
    ) -> List[MeasurementFile]:
        """
        Find the closest dark measurement set to a given atmosphere measurement set.
        
        Args:
            data_segment (:obj:`list` of :obj:`MeasurementFile`): Atmosphere measurement set as a list of :obj:`MeasurementFile`
            dark_segments (:obj:`list` of :obj:`list` of :obj:`MeasurementFile`): List of dark measurement sets to search in.
            type (:obj:`FileType`, optional): Required file format. If not specified, any file format will be considered valid.
            
        Returns:
            :obj:`list` of :obj:`MeasurementFile`
        """
        data_start = data_segment[0].StartDateTime()
        data_end = data_segment[0].EndDateTime()
        
        closest = None
        best_dark_segment = None
        
        for index, dark_segment in enumerate(dark_segments):
            if dark_segment[0].Type() != type and type is not None:
                # This is not the type of data we are looking for.
                continue
                
            if not dark_segment[0].HasSameChannelsAs ( data_segment[0] ):
                # This dark file does not correspond to this data segment
                continue
                
            dark_start = data_segment[0].StartDateTime()
            dark_end = data_end = data_segment[0].EndDateTime()
            
            if dark_end < data_start:
                # Dark measurements were taken before data measurements.
                time_gap = data_start - dark_end
            elif dark_start > data_end:
                # Dark measurements were taken after data measurements.
                time_gap = dark_start - data_end
            else:
                # Dark measurements and data measurements overlap so set
                # the time gap to zero.
                #
                # We can also safely assume no other dark segment will come
                # close to this performance. :P
                time_gap = timedelta(seconds=0)
                return dark_segment
                
            if closest is None:
                closest = time_gap
                best_dark_segment = index
            elif time_gap < closest:
                closest = time_gap
                best_dark_segment = index
                
        if best_dark_segment is None:
            return []
            
        return dark_segments[index]

    def ComputeContinuousMeasurements(
        self,
        max_gap : int,
        min_length : int,
        max_length : int,
        alignment_type : AlignmentType
    ):
        """
        Compute the continuous measurement sets based on given criteria.
        
        Args:
            max_gap (int): Maximum acceptable time gap between two measurement files in seconds.
            min_length (int): Minimum acceptable length for a measurement in seconds.
            max_length (int): Maximum acceptable length for a measurement in seconds. Measurements longer
                than this value will be split.
            alignment_type (:obj:`AlignmentType`): The type of alignment to be performed on the measurement sets.
        """
        self.continuous_measurements = []

        if len(self.measurements) < 1:
            self.accepted_gap = max_gap
            self.accepted_min_length = min_length
            self.accepted_max_length = max_length
            self.accepted_alignment_type = alignment_type
            return
            
        gapped_dark_segments = self.ContinuousDarkMeasurements ( max_gap, min_length, max_length, alignment_type )
        gapped_data_segments = self.ContinuousDataMeasurements ( max_gap, min_length, max_length, alignment_type )

        measurement_number = 0
        last_start = None
        
        # Sort the segments by start date because we will compare
        # the dates in order to set the sequence number.
        gapped_data_segments.sort ( key=lambda x: x[0].StartDateTime() )
                
        for segment in gapped_data_segments:
            # This is already filtered
            real_measurements = segment
            # Need to find closest continuous dark segment:
            dark_measurements = Lidarchive.ClosestDarkSegment(data_segment = segment, dark_segments = gapped_dark_segments, type = real_measurements[0].Type())

            # Apply measurement number if necessary:
            if last_start != None:
                if segment[0].StartDateTime().date() != last_start.date():
                    measurement_number = 0
                else:
                    measurement_number += 1
            else:
                measurement_number = 0

            last_start = segment[0].StartDateTime()

            # Do not add last segment if new data files might appear just in case it's a recent dataset:
            if (datetime.now() - segment[-1].EndDateTime()).total_seconds() >= max_gap:
                measurement_set = MeasurementSet(
                    dark=dark_measurements,
                    data=real_measurements,
                    number=measurement_number
                )
                
                self.continuous_measurements.append( measurement_set )

        self.accepted_gap = max_gap
        self.accepted_min_length = min_length
        self.accepted_max_length = max_length
        self.accepted_alignment_type = alignment_type

    def Measurements(self) -> List[MeasurementFile]:
        """
        Retrieve all lidar data files inside the folder.

        Returns:
            :obj:`list` of :obj:`MeasurementFile`
        """
        return self.measurements

    def ReadFolder(self, start_date : Optional[datetime] = None, end_date : Optional[datetime] = None) -> None:
        """
        Read the folder and identify all lidar data files in the folder and its subdirectories.

        Args:
        start_date (:obj:`datetime`, optional): The earliest date a measurement could have been taken at.
            If set to None, this criteria will not be used.
        end_date (:obj:`datetime`, optional): The earliest date a measurement could have been taken at.
            If set to None, this criteria will not be used.
        """
        # Reset measurements set:
        self.measurements = []

        # Walk the folder tree:
        for root, dirs, files in os.walk(self.folder):
            for file in files:
                path = os.path.join(root, file)
                
                try:
                    file = MeasurementFile( path = path )
                    
                    # Only read the files that are between specified dates:
                    good_file = False
                    
                    date = file.StartDateTime()
                    
                    if start_date == None:
                        if end_date == None:
                            good_file = True
                        elif date <= end_date:
                            good_file = True
                    elif end_date == None:
                        if date >= start_date:
                            good_file = True
                    elif date >= start_date and date <= end_date:
                        good_file = True
                
                    if good_file:
                        self.measurements.append(file)
                        
                except Exception as e:
                    # This was most likely not a valid measurement file.
                    #
                    # Continue silently. Shhhh.
                    continue
                    
        # Make sure we get a unique list of files!
        # Since we're walking down the folder tree, it might just so happen
        # that some files can be stored multiple times in different folders.
        #
        # When that happens, atmospheric-lidar is confused and throws errors,
        # so it's better to take care of it here.
        seen = set()
        self.measurements = [ m for m in self.measurements if m.Filename() not in seen and not seen.add(m.Filename()) ]

        self.measurements.sort(key=lambda x: x.StartDateTime())
