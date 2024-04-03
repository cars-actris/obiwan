from obiwan.data.types import FileType
from obiwan.log import logger
from obiwan.repository import AlignmentType, LidarTest

from pathlib import Path
from typing import Optional

import datetime
import os
import yaml

import traceback

DEFAULT_TESTS_FOLDER = "data/tests"

class ExtraNCParameters:
    """
    Helper class to store information regarding extra parameters that should be written to the SCC NetCDF input files.
    
    Attributes:
        configurations (:obj:`list` of :obj:`ExtraNCParametersFile`): Stores the list of known NetCDF parameters files.
        root_folder (:obj:`Path`, Optional): Root folder from where to compute relative paths. If None, it defaults
                to the user's home directory.
    """
    
    UNKNOWN_SYSTEM = -1
    
    class ExtraNCParametersFile:
        """
        Data structure to hold information about a specific Licel extra NetCDF parameters file, as used in the
        atmospheric-lidar module.
        
        Attributes:
            system_id (int): SCC System ID
            file_path (:obj:`Path`): Path to the file containing the extra NetCDF parameters that will be used
                by atmospheric-lidar conversion routines.
            file_type (:obj:`FileType`): Type of file type this parameters file should be used for. If this is
                set to FileType.UNKNOWN, it will be considered valid for every type of Licel file.
            start_date (:obj:`datetime`, Optional): Date from which this configuration parameters became valid.
                If set to None, configuration is believed to be valid from the beginning of humanity.
            end_date (:obj:`datetime`, Optional): Date until which this configuration parameters is valid.
                If set to None, configuration is believed to be valid until total human extinction.
        """
        def __init__ (self,
            system_id : int,
            file_path : Path,
            file_type : FileType,
            start_date : Optional[datetime.datetime] = None,
            end_date : Optional[datetime.datetime] = None
        ):
            """
            Args:
                system_id (int): SCC System ID
                file_path (:obj:`Path`): Path to the file containing the extra NetCDF parameters that will be used
                    by atmospheric-lidar conversion routines.
                file_type (:obj:`FileType`): Type of file type this parameters file should be used for. If this is
                    set to FileType.UNKNOWN, it will be considered valid for every type of Licel file.
                start_date (:obj:`datetime`, Optional): Date from which this configuration parameters became valid.
                    If set to None, configuration is believed to be valid from the beginning of humanity.
                end_date (:obj:`datetime`, Optional): Date until which this configuration parameters is valid.
                    If set to None, configuration is believed to be valid until total human extinction.
            """
            self.system_id = system_id
            self.file_path = file_path
            self.file_type = file_type
            self.start_date = start_date
            self.end_date = end_date
            
        def valid_for ( self, system_id : int, file_type : FileType, date : Optional[datetime.datetime] = None ) -> bool:
            """
            Checks if the information stored about this parameters file is valid for a certain system, measurement
            and, optionally, a measurement date.
            
            Args:
                system_id (int): SCC ID of the system
                file_type (:obj:`FileType`): File format used in this measurement. Useful as different Licel formats might
                    need different parameters or IDs.
                date (:obj:`datetime`, Optional): The measurement date for which to check extra parameters availability.
                    
            Returns:
                True if this parameters file can be used using the specified measurement information, False otherwise.
            """
            start_date_valid = True
            end_date_valid = True
            system_valid = (system_id == self.system_id)
            type_valid = (file_type == self.file_type) or self.file_type == FileType.UNKNOWN
            
            if date is not None:
                # Check start and end date if need be, otherwise the criteria will be
                # automatically satisfied.
                if self.start_date is not None:
                    start_date_valid = self.start_date <= date
                    
                if self.end_date is not None:
                    end_date_valid = self.end_date >= date
                    
            # print ( date, system_id, file_type )
            # print ( self.start_date, self.end_date, self.system_id, self.file_type )
            # print ( start_date_valid, end_date_valid, system_valid, type_valid )
                    
            return start_date_valid and end_date_valid and system_valid and type_valid
    
    def __init__ ( self, raw_value : Optional[object] = None, root_folder : Optional[Path] = None ):
        """
        Args:
            value (:obj:`object`, Optional): The raw value that should be parsed. It can be a string, a list of strings,
                or a list of dictionaries.
            root_folder (:obj:`Path`, Optional): Root folder from where to compute relative paths. If None, it defaults
                to the user's home directory.
        """
        self.configurations = []
        
        if root_folder is None:
            self.root_folder = Path.home()
        else:
            self.root_folder = root_folder
        
        if raw_value is not None:
            self.parse_raw_value ( raw_value )
        
    @staticmethod
    def parse_format_code ( code : int ) -> FileType:
        available_formats = {
            0: FileType.UNKNOWN,
            1: FileType.LICEL_V1,
            2: FileType.LICEL_V2
        }
        
        try:
            return available_formats[ code ]
        except Exception:
            raise TypeError ( f"Format {code} is not a valid file format type." )
            
    def parse_raw_value ( self, raw_value : object ) -> None:
        """
        Parse the corresponding licel_netcdf_parameters field from the YAML configuration file.
        This method can handle the raw value as a string or as a list of dictionaries.
        
        Args:
            raw_value (:obj:`object`): The raw value that should be parsed. It can be a string or a list of dictionaries.
        """
        self.systems = []
        
        if type(raw_value) is str:
            # User set a simple path in the configuration file, so we will use this parameters file
            # for every lidar system.
            self.configurations.append (ExtraNCParameters.ExtraNCParametersFile(
                system_id = ExtraNCParameters.UNKNOWN_SYSTEM,
                file_path = Config.compute_path(value, root_folder = self.root_folder),
                file_type = FileType.UNKNOWN,
                start_date = None,
                end_date = None
            ))
            
            return
            
        if type(raw_value) is not dict:
            raise ValueError ( "Could not parse the value of licel_netcdf_parameters configuration." )
            
        for system_id, files in raw_value.items():
            # First check the system ID is an integer number:
            if type(system_id) is not int:
                logger.error ( f"Invalid system ID in licel_netcdf_parameters: {system_id}" )
                continue
                
            # First check if we got a simple string or a full dictionary for this system ID
            if type(files) is str:
                # We got a simple path, so let's store that and move on
                    try:
                        file_path = Config.compute_path(files, root_folder = self.root_folder)
                        
                        self.configurations.append (ExtraNCParameters.ExtraNCParametersFile(
                            system_id = system_id,
                            file_path = Config.compute_path ( files, root_folder = self.root_folder ),
                            file_type = FileType.UNKNOWN,
                            start_date = None,
                            end_date = None
                        ))
                    except KeyError:
                        logger.error (f"Missing licel_netcdf_parameters \"file\" field for system ID {system_id}")
            elif type(files) is list:
                for system_value in files:
                # Read configuration from dictionary
                    try:
                        file_path = Config.compute_path(system_value["file"], root_folder = self.root_folder)
                    except KeyError:
                        logger.error (f"Missing licel_netcdf_parameters \"file\" field for system ID {system_id}")
                        continue
                        
                    file_type = ExtraNCParameters.parse_format_code ( system_value.get("version", 0) )
                    start_date = system_value.get("from", None)
                    end_date = system_value.get("until", None)
                
                    # Perform basic data validation:
                    if start_date is not None:
                        try:
                            start_date = datetime.datetime(start_date.year, start_date.month, start_date.day)
                        except Exception:
                            logger.error (f"Could not parse date {start_date} in licel_netcdf_parameters for system ID {system_id}")
                            traceback.print_exc()
                            continue
                        
                    if end_date is not None:
                        try:
                            end_date = datetime.datetime(end_date.year, end_date.month, end_date.day)
                        except Exception:
                            logger.error (f"Could not parse date {end_date} in licel_netcdf_parameters for system ID {system_id}")
                            traceback.print_exc()
                            continue
                            
                    if not os.path.isfile ( file_path ):
                        logger.error (f"Invalid path for extra parameters file: {file_path}.")
                        continue
                        
                    self.configurations.append (ExtraNCParameters.ExtraNCParametersFile(
                        system_id = system_id,
                        file_path = file_path,
                        file_type = file_type,
                        start_date = start_date,
                        end_date = end_date
                    ))
        
    def get_parameter_file ( self, system_id : int, file_type : FileType, date : Optional[datetime.datetime] = None ) -> Path:
        """
        Retrieve the file path to the extra parameters file for a given system and measurement type.
        
        Args:
            system_id (int): SCC ID of the system
            file_type (:obj:`FileType`): File format used in this measurement. Useful as different Licel formats might
                need different parameters or IDs.
            date (:obj:`datetime`, Optional): The measurement date for which to check extra parameters availability.
                
        Returns:
            :obj:`Path` representing the file path to the required file.
        """
        
        # Filter known configurations for the system we're using
        valid_files = [ file for file in self.configurations if file.valid_for ( system_id, file_type, date ) ]
        
        if len(valid_files) < 1:
            # We could not find any suitable extra parameters file
            raise KeyError ( f"Could not find extra parameters file for System ID {system_id} and file format {measurement_type}" )
            
        if len(valid_files) > 1:
            # We found multiple suitable files so we must check which is the best one.
            # We're going to sort first by file type, and then by file creation date.
            type_sort = lambda x: 0 if x.file_type != file_type else 1
            date_sort = lambda x: x.start_date if x.start_date is not None else datetime.min
            valid_files.sort(key = type_sort, reversed = True)
            valid_files.sort(key = date_sort, reversed = True)
        
        # We found the right file! Let's go!
        return valid_files[0].file_path

class Config:
    """
    Class to read and store configuration parameters. This will do basic type and value checking on the provided YAML file.
    
    Attributes:
        file_path (:obj:`Path`): File path of the YAML configuration file that is being used.
        scc_configurations_folder (:obj:`Path`): Path to the folder holding sample files used to identify different lidar systems.
        netcdf_out_dir (:obj:`Path`): Path to the folder where SCC NetCDF files will be stored after conversion.
        scc_output_dir (:obj:`Path`): Path to the folder where SCC Products will be stored after downloading.
        system_netcdf_parameters (:obj:`ExtraNCParameters`): Extra parameters that should be included in the SCC NetCDF input files at conversion time.
        scc_basic_credentials (:obj:`tuple` of :obj:`str`): HTTP credentials for the SCC web server.
        scc_website_credentials (:obj:`tuple` of :obj:`str`): User credentials for the SCC platform.
        scc_base_url (:obj:`str`): HTTP URL of the SCC website.
        maximum_upload_retry_count (int): Maximum number of retries to perform in case of upload errors.
        measurement_identifiers (:obj:`list` of :obj:`str`): List of identifiers for real atmosphere measurements.
        dark_identifiers (:obj:`list` of :obj:`str`): List of identifiers for dark measurements.
        max_acceptable_gap (int): Maximum acceptable time gap, in seconds, between two measurement files in order to consider them as being
            part of the same continuous measurement.
        min_acceptable_length (int): Minimum length for a measurement set, in seconds. Measurements shorter than this will be discarded, depending
            on the `measurement_alignment_type`. In some cases, short measurements could be glued to normal length measurements.
        max_acceptable_length (int): Maximum length for a measurement set, in seconds. This value is used when splitting continuous measurements in
            reasonably sized chunks during conversion (common value is 3600 for 1 hour long measurement sets).
        alignment_type (:obj:`AlignmentType`): Type of alignment to perform on the identified measurements. Defaults to `AlignmentType.NONE`.
        measurements_debug_dir (:obj:`Path`): Path to the folder where to copy raw and NetCDF files when debugging measurement identification and splitting.
        tests_dir (:obj:`Path`): Path to the folder where to copy raw files identified as test files.
        test_lists (:obj:`list` of :obj:`LidarTest`): List of tests to search for in the measurement files.
    """
    def __init__ ( self, file_path : Path = None ):
        if file_path is not None:
            self.read_file ( file_path )
            
    def read_file ( self, file_path : Path ) -> None:
        """
        Read specified configuration file.
        
        Args:
            file_path (:obj:`Path`): Path to the configuration file.
        """
        fp = os.path.abspath ( file_path )
            
        try:
            with open (fp, 'r') as yaml_file:
                try:
                    config = yaml.safe_load (yaml_file)
                except Exception as e:
                    raise IOError(f"Could not parse YAML configuration file ({fp})! {str(e)}")
        except Exception as e:
            raise IOError(f"Could not read configuration file ({fp})! {str(e)}")
                
        self.file_path = fp
        
        config_dir = os.path.abspath ( os.path.dirname ( self.file_path ) )
        
        # Folders:
        self.scc_configurations_folder = Config.compute_path ( config['scc_configurations_folder'], root_folder = config_dir )
        self.netcdf_out_dir = Config.compute_path ( config['netcdf_out_folder'], root_folder = config_dir )
        self.scc_output_dir = Config.compute_path ( config['scc_output_dir'], root_folder = config_dir )
        
        # Licel system NetCDF parameter files:
        self.system_netcdf_parameters = ExtraNCParameters (
            raw_value = config.get('licel_netcdf_parameters', None),
            root_folder = config_dir
        )
        
        # SCC Configuration:
        self.scc_basic_credentials = tuple ( config['scc_basic_credentials'] )
        self.scc_website_credentials = tuple ( config['scc_website_credentials'] )
        self.scc_base_url = config['scc_base_url']
        self.maximum_upload_retry_count = config['scc_maximum_upload_retries']
        
        # Licel header location types:
        if type(config['measurement_identifiers']) is str:
            self.measurement_identifiers = [ config['measurement_identifiers'] ]
        elif type(config['measurement_identifiers']) is list:
            self.measurement_identifiers = config['measurement_identifiers']
        else:
            raise ValueError ( "CONFIG ERROR: Measurement locations is not a list." )
        
        if type(config['dark_identifiers']) is str:
            self.dark_identifiers = [ config['dark_identifiers'] ]
        elif type(config['dark_identifiers']) is list:
            self.dark_identifiers = config['dark_identifiers']
        else:
            raise ValueError ( "CONFIG ERROR: Dark locations is not a list." )
        
        # Lidarchive parameters:
        self.max_acceptable_gap = config['maximum_measurement_gap']
        self.min_acceptable_length = config['minimum_measurement_length']
        self.max_acceptable_length = config['maximum_measurement_length']
        
        self.min_acceptable_dark_length = config.get('minimum_dark_measurement_length', 60)
        
        try:
            self.alignment_type = AlignmentType ( config['measurement_alignment_type'] )
        except ValueError:
            self.alignment_type = AlignmentType.NONE
        except KeyError:
            self.alignment_type = AlignmentType.NONE
        
        # Measurements debug:
        self.measurements_debug_dir = Config.compute_path ( config['measurements_debug_dir'], root_folder = config_dir )
        
        # Test folder:
        self.tests_dir = Config.compute_path (DEFAULT_TESTS_FOLDER, root_folder = config_dir)
        
        self.test_lists = []
        
        for key in config.keys():
            if key.startswith("test_") and len(key) > 5:
                self.test_lists.append ( LidarTest ( name = key[5:], test_identifiers = config[key] ) )
                
        self.makedirs()
                
    def makedirs ( self ) -> None:
        """
        Create folders that will be used later.
        """
        if not os.path.isdir (self.netcdf_out_dir):
            os.makedirs ( self.netcdf_out_dir )
            
        if not os.path.isdir (self.scc_output_dir):
            os.makedirs ( self.scc_output_dir )
            
        if not os.path.isdir (self.measurements_debug_dir):
            os.makedirs ( self.measurements_debug_dir )
            
        if not os.path.isdir (self.scc_output_dir):
            os.makedirs ( self.scc_output_dir )
            
        if not os.path.isdir (self.tests_dir):
            os.makedirs ( self.tests_dir )
                
    @staticmethod
    def compute_path ( path : str, root_folder : Path = None ) -> Path:
        """
        Get an absolute path from a relative or absolute path. If using a relative path,
        you can specify the root folder from where to build the path tree.
        
        Args:
            path (str): The path string. Can be either a relative or an absolute path.
            root_folder (:obj:`Path`): Root folder from where to start resolving relative paths.
            
        Returns:
            :obj:`Path` representing the final absolute path.
        """
        if ( os.path.isabs ( path ) ):
            # If the user configured an absolute path
            # use that without any questions asked:
            return Path ( path )
            
        root = root_folder
        if root is None:
            root = Path.home()
            
        # Otherwise, if a relative path was provided,
        # get path relative to the parent folder of this file:
        
        relpath = os.path.join ( root, path )
        abspath = os.path.abspath ( os.path.normpath ( relpath ) )
        
        return Path ( abspath )