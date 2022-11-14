from obiwan.log import logger, datalog, Datalog
from obiwan.lidar import system_index
from obiwan.config import ExtraNCParameters

from obiwan.lidarchive.lidarchive import MeasurementSet

import importlib
import os
import sys

from pathlib import Path
from typing import Union

from atmospheric_lidar.licel import LicelLidarMeasurement
from atmospheric_lidar.licelv2 import LicelLidarMeasurementV2

import traceback

def LicelToSCC ( measurement : MeasurementSet, out_folder : Path, system_netcdf_parameters : ExtraNCParameters ) -> Union[Path, None]:
    """
    Convert a measurement set consisting of older specification Licel raw files
    to SCC NetCDF input file format.
    
    Args:
        measurement (:obj:`MeasurementSet`): Measurement set to be converted.
        out_folder (:obj:`Path`): Path to the folder where to store the resulting NetCDF file.
        system_netcdf_parameters (:obj:`ExtraNCParameters`): Repository of extra NetCDF parameters
            used by `atmospheric-lidar` to perform the conversion.
            
    Returns:
        :obj:`Path` of the final SCC NetCDF input file. If the conversion cannot be done, it will return None.
    """
    logger.info ( "Converting %d Licel files to SCC NetCDF format." % len(measurement.DataFiles()) )
    
    try:
        system_id = system_index.GetSystemId (measurement.DataFiles()[0])
    except ValueError as e:
        logger.error ("Couldn't determine system ID for measurement '%s': %s." % (measurement.DataFiles()[0].Path(), str(e)))
        return None
    except IndexError as e:
        logger.error ( "Could not find any data files for this measurement." )
        return None
        
    datalog.update_task ( measurement.Id(), (Datalog.Field.SYSTEM_ID, system_id) )
    
    try:
        netcdf_parameters_path = system_netcdf_parameters.get_parameter_file ( system_id = system_id, measurement_type = measurement.Type() )
    except:
        traceback.print_exc()
        # No netcdf parameters file found for this system!
        logger.error ( f"Could not find netcdf parameters file for system ID {system_id}" )
        return None
        
    try:
        sys.path.append ( os.path.dirname (netcdf_parameters_path) )
        netcdf_parameters_filename = os.path.basename ( netcdf_parameters_path )
        if netcdf_parameters_filename.endswith ('.py'):
            netcdf_parameters_filename = netcdf_parameters_filename[:-3]

        nc_parameters_module = importlib.import_module ( netcdf_parameters_filename )

        class CustomLidarMeasurement(LicelLidarMeasurement):
            extra_netcdf_parameters = nc_parameters_module
            
        earlinet_station_id = nc_parameters_module.general_parameters['Call sign']
        date_str = measurement.DataFiles()[0].StartDateTime().strftime('%Y%m%d')
        measurement_number = measurement.NumberAsString()
        measurement_id = "{0}{1}{2}".format(date_str, earlinet_station_id, measurement_number)
    except Exception as e:
        logger.error ( "Could not determine measurement ID." )
        return None
        
    datalog.update_task ( measurement.Id(), (Datalog.Field.SCC_MEASUREMENT_ID, measurement_id) )

    try:
        try:
            custom_measurement = CustomLidarMeasurement ( file_list = [file.Path() for file in measurement.DataFiles()], use_id_as_name=False )
            
            if len(measurement.DarkFiles()) > 0:
                custom_measurement.dark_measurement = CustomLidarMeasurement ( file_list = [file.Path() for file in measurement.DarkFiles()], use_id_as_name=False )
                
            custom_measurement = custom_measurement.subset_by_scc_channels ()
        except (IOError, ValueError):
            # This could happen because the system has two telescopes so channel names get duplicated
            # inside atmospheric-lidar which throws an IOError.
            #
            # A ValueError can be thrown if there are no common channels between the Licel file
            # and the extra_netcdf_parameters configuration file. In this case, it might be that the configuration file
            # uses digitizer IDs as channel identifiers.
            custom_measurement = CustomLidarMeasurement ( file_list = [file.Path() for file in measurement.DataFiles()], use_id_as_name=True )
            
            if len(measurement.DarkFiles()) > 0:
                custom_measurement.dark_measurement = CustomLidarMeasurement ( file_list = [file.Path() for file in measurement.DarkFiles()], use_id_as_name=True )
                
            custom_measurement = custom_measurement.subset_by_scc_channels ()
            
        channel_ids = ", ".join(custom_measurement.channels.keys())
        logger.debug(f"Measurement channels: {channel_ids}", extra = { 'scope': 'converter' })

        custom_measurement.set_measurement_id(measurement_number=measurement.NumberAsString())
        
        file_path = os.path.join(out_folder, f'{measurement_id}.nc')
        
        custom_measurement.save_as_SCC_netcdf (filename=file_path)
    except Exception as e:
        logger.error ( f"Could not convert measurement. {traceback.format_exc()}" )
        return None
    
    return Path ( file_path )
    
def LicelToSCCV2 ( measurement : MeasurementSet, out_folder : Path, system_netcdf_parameters : ExtraNCParameters ) -> Union[Path, None]:
    """
    Convert a measurement set consisting of older specification Licel raw files
    to SCC NetCDF input file format.
    
    Args:
        measurement (:obj:`MeasurementSet`): Measurement set to be converted.
        out_folder (:obj:`Path`): Path to the folder where to store the resulting NetCDF file.
        system_netcdf_parameters (:obj:`ExtraNCParameters`): Repository of extra NetCDF parameters
            used by `atmospheric-lidar` to perform the conversion.
            
    Returns:
        :obj:`Path` of the final SCC NetCDF input file. If the conversion cannot be done, it will return None.
    """
    logger.info ( "Converting %d Licel V2 files to SCC NetCDF format." % len(measurement.DataFiles()) )
    
    try:
        system_id = system_index.GetSystemId (measurement.DataFiles()[0])
    except ValueError as e:
        logger.error ("Couldn't determine system ID for measurement '%s': %s." % (measurement.DataFiles()[0].Path(), str(e)))
        return None
    except IndexError as e:
        logger.error ( "Could not find any data files for this measurement." )
        return None
        
    datalog.update_task ( measurement.Id(), (Datalog.Field.SYSTEM_ID, system_id) )
    
    try:
        netcdf_parameters_path = system_netcdf_parameters.get_parameter_file ( system_id = system_id, measurement_type = measurement.Type() )
    except:
        traceback.print_exc()
        # No netcdf parameters file found for this system!
        logger.error ( f"Could not find netcdf parameters file for system ID {system_id}" )
        return None
        
    try:
        sys.path.append ( os.path.dirname (netcdf_parameters_path) )
        netcdf_parameters_filename = os.path.basename ( netcdf_parameters_path )
        if netcdf_parameters_filename.endswith ('.py'):
            netcdf_parameters_filename = netcdf_parameters_filename[:-3]

        nc_parameters_module = importlib.import_module ( netcdf_parameters_filename )

        class CustomLidarMeasurement(LicelLidarMeasurementV2):
            extra_netcdf_parameters = nc_parameters_module
            
        earlinet_station_id = nc_parameters_module.general_parameters['Call sign']
        date_str = measurement.DataFiles()[0].StartDateTime().strftime('%Y%m%d')
        measurement_number = measurement.NumberAsString()
        measurement_id = "{0}{1}{2}".format(date_str, earlinet_station_id, measurement_number)
    except Exception as e:
        logger.error ( "Could not determine measurement ID." )
        return None
        
    datalog.update_task ( measurement.Id(), (Datalog.Field.SCC_MEASUREMENT_ID, measurement_id) )

    try:
        custom_measurement = CustomLidarMeasurement ( [file.Path() for file in measurement.DataFiles()] )
        
        if len(measurement.DarkFiles()) > 0:
            custom_measurement.dark_measurement = CustomLidarMeasurement ( [file.Path() for file in measurement.DarkFiles()] )

        custom_measurement = custom_measurement.subset_by_scc_channels ()
        
        custom_measurement.set_measurement_id(measurement_number=measurement.NumberAsString())
        
        file_path = os.path.join(out_folder, f'{measurement_id}.nc')
        
        custom_measurement.save_as_SCC_netcdf (filename=file_path)
    except Exception as e:
        logger.error ( f"Could not convert measurement. {traceback.format_exc()}" )
        return None
    
    return Path ( file_path )