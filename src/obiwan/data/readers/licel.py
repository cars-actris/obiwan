from obiwan.config import Config
from obiwan.data.types import FileInfo, ChannelInfo
from obiwan.log import Datalog, logger
from obiwan.repository import MeasurementSet

from .generic import LidarReader

from atmospheric_lidar.licel import LicelFile, LicelLidarMeasurement

from pathlib import Path

import importlib
import os
import sys
import traceback

from typing import Union, List, Tuple

class LicelReader(LidarReader):
    """
    Reader for raw Licel data files in the older specification format.
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
        info = {}
        extra_info = {}
        channels = []
        
        licel_file = LicelFile(file, import_now = False)
        
        for channel in licel_file.channel_info:
            channels.append(
                ChannelInfo(
                    name = channel["ID"],
                    resolution = channel["bin_width"],
                    wavelength = int ( channel["wavelength"].split('.')[0] ),
                    laser_used = channel["laser_used"],
                    adcbits = channel["ADCbits"],
                    analog = channel["analog_photon"] == 0,
                    active = channel["active"],
                    number_of_shots = int(channel["number_of_shots"])
                )
            )
            
        info = FileInfo (
            start_time = licel_file.start_time.replace(tzinfo=None),
            end_time = licel_file.stop_time.replace(tzinfo=None),
            location = licel_file.site,
            channels = channels
        )
        
        return info
        
    @staticmethod
    def has_identifier ( info : FileInfo, identifier : str ) -> bool:
        """
        Check if a measurement is is characterized by a certain identifier (e.g.: Location information for Licel files)
        
        Args:
            info (:obj:`FileInfo`): File information to check.
            identifier(str): Strings that will be tested against relevant fields in the file.
            
        Returns:
            True if the file and identifier match, False otherwise.
        """
        return info.location == identifier
        
    @staticmethod
    def has_identifier_in_list ( info : FileInfo, identifiers: List[str] ) -> bool:
        """
        Check if a measurement is is characterized by a certain identifier (e.g.: Location information for Licel files)
        
        Args:
            info (:obj:`FileInfo`): File information to check.
            identifiers(:obj:`list` of str): List of strings which will be tested against relevant fields in the file.
            
        Returns:
            True if the file and identifier match, False otherwise.
        """
        return info.location in identifiers
        
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
        try:
            netcdf_parameters_path = app_config.system_netcdf_parameters.get_parameter_file ( system_id = system_id, file_type = measurement_set.Type(), date = measurement_set.StartDateTime() )
        except Exception:
            # No netcdf parameters file found for this system!
            logger.error ( f"Could not find netcdf parameters file for system ID {system_id}" )
            return None, None
            
        try:
            sys.path.append ( os.path.dirname (netcdf_parameters_path) )
            netcdf_parameters_filename = os.path.basename ( netcdf_parameters_path )
            if netcdf_parameters_filename.endswith ('.py'):
                netcdf_parameters_filename = netcdf_parameters_filename[:-3]

            nc_parameters_module = importlib.import_module ( netcdf_parameters_filename )

            class CustomLidarMeasurement(LicelLidarMeasurement):
                extra_netcdf_parameters = nc_parameters_module
                
            earlinet_station_id = nc_parameters_module.general_parameters['Call sign']
            date_str = measurement_set.DataFiles()[0].StartDateTime().strftime('%Y%m%d')
            measurement_number = measurement_set.NumberAsString()
            measurement_id = "{0}{1}{2}".format(date_str, earlinet_station_id, measurement_number)
        except Exception as e:
            logger.error ( "Could not determine measurement ID." )
            traceback.print_exc()
            return None, None
            
        logger.info ( "Converting %d Licel files to SCC NetCDF format." % len(measurement_set.DataFiles()), extra={'scope': measurement_id} )
            
        # In the case that the system was shut down whilst writing the last data file,
        # the latter will have a smaller number of laser shots. If this happens, the
        # HiRELPP module of the SCC will throw error 87 - `Please check the variables
        # "Raw_Data_Start_Time" and "Raw_Data_Stop_Time" (and eventually the variables
        # "Raw_Bck_Start_Time" and "Raw_Bck_Stop_Time") in the submitted input file`
        #
        # While we cannot fix the SCC, we can work around it by removing the last data file.
        data_files = measurement_set.DataFiles()
        dark_files = measurement_set.DarkFiles()
        
        if not data_files[-1].NumberOfShotsSimilarTo ( data_files[0], max_relative_diff = .05 ):
            logger.warning ("Found different number of laser shots in the last recorded data file. Removing it to avoid SCC processing errors.", extra={'scope': measurement_id})
            data_files = data_files[ : -1]
            
        if len(dark_files) > 0:
            if not dark_files[-1].NumberOfShotsSimilarTo ( dark_files[0], max_relative_diff = .05 ):
                logger.warning ("Found different number of laser shots in the last recorded dark file. Removing it to avoid SCC processing errors.", extra={'scope': measurement_id})

        try:
            try:
                custom_measurement = CustomLidarMeasurement ( file_list = [file.Path() for file in data_files], use_id_as_name=False )
                
                if len(measurement_set.DarkFiles()) > 0:
                    custom_measurement.dark_measurement = CustomLidarMeasurement ( file_list = [file.Path() for file in dark_files], use_id_as_name=False )
                    
                custom_measurement = custom_measurement.subset_by_scc_channels ()
            except (IOError, ValueError):
                # This could happen because the system has two telescopes so channel names get duplicated
                # inside atmospheric-lidar which throws an IOError.
                #
                # A ValueError can be thrown if there are no common channels between the Licel file
                # and the extra_netcdf_parameters configuration file. In this case, it might be that the configuration file
                # uses digitizer IDs as channel identifiers.
                custom_measurement = CustomLidarMeasurement ( file_list = [file.Path() for file in data_files], use_id_as_name=True )
                
                if len(measurement_set.DarkFiles()) > 0:
                    custom_measurement.dark_measurement = CustomLidarMeasurement ( file_list = [file.Path() for file in dark_files], use_id_as_name=True )
                    
                custom_measurement = custom_measurement.subset_by_scc_channels ()
                
            channel_ids = ", ".join(custom_measurement.channels.keys())
            logger.debug(f"Measurement channels: {channel_ids}", extra = { 'scope': 'converter' })
                
            try:
                if len(custom_measurement.channels) != len(data_files[0].info.channels):
                    logger.warning (f"Could not find all measurement channels in extra NetCDF parameters file", extra={'scope': measurement_id})
            except Exception:
                pass

            custom_measurement.set_measurement_id(measurement_number=measurement_set.NumberAsString())
            
            file_path = os.path.join(output_folder, f'{measurement_id}.nc')
            
            custom_measurement.save_as_SCC_netcdf (filename=file_path)
        except Exception as e:
            logger.error ( f"Could not convert measurement. {traceback.format_exc()}", extra={'scope': measurement_id} )
            return None, measurement_id
        
        return Path ( file_path ), measurement_id