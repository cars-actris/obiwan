from obiwan.log import logger, datalog
from obiwan.lidar import system_index

import importlib
import os
import sys

from atmospheric_lidar.licel import LicelLidarMeasurement
from atmospheric_lidar.licelv2 import LicelLidarMeasurementV2

import traceback

def LicelToSCC ( measurement, out_folder, netcdf_parameters_path ):
    logger.info ( "Converting %d Licel files to SCC NetCDF format." % len(measurement.DataFiles()) )
    
    try:
        system_id = system_index.GetSystemId (measurement.DataFiles()[0])
    except ValueError as e:
        logger.error ("Couldn't determine system ID for measurement '%s': %s." % (measurement.DataFiles()[0].Path(), str(e)))
        return None, None
    except IndexError as e:
        logger.error ( "Could not find any data files for this measurement. Skipping." )
        return None, None
        
    datalog.update_measurement ( measurement.Id(), ("system_id", system_id) )
        
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
        logger.error ( "Could not determine measurement ID. Skipping..." )
        return None, None
        
    datalog.update_measurement ( measurement.Id(), ("scc_measurement_id", measurement_id) )

    try:
        try:
            custom_measurement = CustomLidarMeasurement ( file_list = [file.Path() for file in measurement.DataFiles()], use_id_as_name=False )
            
            if len(measurement.DarkFiles()) > 0:
                measurement.dark_measurement = CustomLidarMeasurement ( file_list = [file.Path() for file in measurement.DarkFiles()], use_id_as_name=False )
                
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
                measurement.dark_measurement = CustomLidarMeasurement ( file_list = [file.Path() for file in measurement.DarkFiles()], use_id_as_name=True )
                
            custom_measurement = custom_measurement.subset_by_scc_channels ()

        custom_measurement.set_measurement_id(measurement_number=measurement.NumberAsString())
        
        file_path = os.path.join(out_folder, f'{measurement_id}.nc')
        
        custom_measurement.save_as_SCC_netcdf (filename=file_path)
    except Exception as e:
        logger.error ( f"Could not convert measurement. {traceback.format_exc()}" )
        return None, None
    
    return file_path, measurement_id
    
def LicelToSCCV2 ( measurement, out_folder, netcdf_parameters_path ):
    logger.info ( "Converting %d Licel V2 files to SCC NetCDF format." % len(measurement.DataFiles()) )
    
    try:
        system_id = system_index.GetSystemId (measurement.DataFiles()[0])
    except ValueError as e:
        logger.error ("Couldn't determine system ID for measurement '%s': %s. Skipping measurement." % (measurement.DataFiles()[0].Path(), str(e)))
        return None, None
    except IndexError as e:
        logger.error ( "Could not find any data files for this measurement. Skipping." )
        return None, None
        
    datalog.update_measurement ( measurement.Id(), ("system_id", system_id) )
        
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
        logger.error ( "Could not determine measurement ID. Skipping..." )
        return None, None
        
    datalog.update_measurement ( measurement.Id(), ("scc_measurement_id", measurement_id) )

    try:
        custom_measurement = CustomLidarMeasurement ( [file.Path() for file in measurement.DataFiles()] )
        
        if len(measurement.DarkFiles()) > 0:
            measurement.dark_measurement = CustomLidarMeasurement ( [file.Path() for file in measurement.DarkFiles()] )

        custom_measurement = custom_measurement.subset_by_scc_channels ()
        custom_measurement.set_measurement_id(measurement_number=measurement.NumberAsString())
        
        file_path = os.path.join(out_folder, f'{measurement_id}.nc')
        
        custom_measurement.save_as_SCC_netcdf (filename=file_path)
    except Exception as e:
        logger.error ( f"Could not convert measurement. {traceback.format_exc()}" )
        return None, None
    
    return file_path, measurement_id