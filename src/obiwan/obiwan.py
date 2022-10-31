from .lidarchive import lidarchive

from obiwan.config import Config
from obiwan.log import logger, datalog, SetLogLevel, UseSwapFile, UseCsvDatalog
from obiwan.lidar import SystemIndex
from obiwan.scc import scc

import argparse
import datetime
import importlib
import os
import sys
import shutil
import time

from atmospheric_lidar.licel import LicelLidarMeasurement

SWAP_FILE_NAME = "obiwan.swp"
convert_resumed = []
upload_resumed = []

def Convert ( config, licel_measurement ):
    logger.info ( "Converting %d licel files to SCC NetCDF format." % len(licel_measurement.DataFiles()) )
    
    try:
        system_id = system_index.GetSystemId (licel_measurement.DataFiles()[0].Path())
    except ValueError as e:
        logger.error ("Couldn't determine system ID for measurement '%s': %s. Skipping measurement." % (licel_measurement.DataFiles()[0].Path(), str(e)))
        return None, None
    except IndexError as e:
        logger.error ( "Could not find any data files for this measurement. Skipping." )
        return None, None
        
    datalog.update_measurement ( licel_measurement.Id(), ("system_id", system_id) )
        
    try:
        earlinet_station_id = nc_parameters_module.general_parameters['Call sign']
        date_str = licel_measurement.DataFiles()[0].StartDateTime().strftime('%Y%m%d')
        measurement_number = licel_measurement.NumberAsString()
        measurement_id = "{0}{1}{2}".format(date_str, earlinet_station_id, measurement_number)
    except Exception as e:
        logger.error ( "Could not determine measurement ID. Skipping..." )
        return None, None
        
    datalog.update_measurement ( licel_measurement.Id(), ("scc_measurement_id", measurement_id) )

    try:
        measurement = CustomLidarMeasurement ( [file.Path() for file in licel_measurement.DataFiles()] )
    except:
        logger.error ( "Could not convert measurement." )
        return None, None
    
    if len(licel_measurement.DarkFiles()) > 0:
        measurement.dark_measurement = CustomLidarMeasurement ( [file.Path() for file in licel_measurement.DarkFiles()] )

    measurement = measurement.subset_by_scc_channels ()
    measurement.set_measurement_id(measurement_number=licel_measurement.NumberAsString())
    
    file_path = os.path.join(config.netcdf_out_dir, f'{measurement_id}.nc')
    
    measurement.save_as_SCC_netcdf (filename=file_path)
    
    datalog.update_measurement_by_scc_id ( measurement_id, ("converted", True) )
    datalog.update_measurement_by_scc_id ( measurement_id, ("scc_netcdf_path", file_path) )
    datalog.update_measurement_by_scc_id ( measurement_id, ("result", "Converted to SCC NetCDF") )
    
    return file_path, measurement_id
    
def DebugMeasurement ( licel_measurement, measurement_path, measurements_debug_dir ):
    if measurements_debug_dir:
        debug_date_str = licel_measurement.DataFiles()[0].StartDateTime().strftime('%Y-%m-%d-%H-%M')
        debug_dir = os.path.join ( measurements_debug_dir, debug_date_str )

        if os.path.exists (debug_dir):
            i = 2
            temp_debug_dir = "%s_%d" % (debug_dir, i)
            
            while os.path.exists ( temp_debug_dir ):
                i += 1
                temp_debug_dir = "%s_%d" % (debug_dir, i)
                
            debug_dir = temp_debug_dir
        
        debug_dark_dir = os.path.join ( debug_dir, "D" )

    os.makedirs ( debug_dir )
    os.makedirs ( debug_dark_dir )
    
    # logger.debug ("Raw data files:")
    for file in licel_measurement.DataFiles():
        # logger.debug ( os.path.basename(file.Path()) )
        if measurements_debug_dir:
            shutil.copy2 ( file.Path(), debug_dir )
        
    # logger.debug ("Raw dark files:")
    for file in licel_measurement.DarkFiles():
        # logger.debug ( os.path.basename(file.Path()) )
        if measurements_debug_dir:
            shutil.copy2 ( file.Path(), debug_dark_dir )
            
    # logger.debug ("SCC NetCDF file: %s" % ( os.path.basename(measurement_path) ))
    if measurements_debug_dir:
        shutil.copy2 ( measurement_path, debug_dir )
            
def Upload (config, measurement_id, measurement_date, file_path, **kwargs):
    reprocess = kwargs.get("reprocess", True)
    replace = kwargs.get("replace", True)
    
    system_id = datalog.get_measurement_by_scc_id ( measurement_id )["system_id"]
    
    measurement_exists = False
    existing_measurement, _ = scc.client.get_measurement( measurement_id )
    
    datalog.update_measurement_by_scc_id ( measurement_id, ("already_on_scc", measurement_exists) )
    
    if existing_measurement is not None:
        measurement_exists = True
    
    if measurement_exists and reprocess:
        # Reprocess the measurement and mark it for download
        logger.debug ( "Measurement already exists in the SCC, triggering reprocessing." )
        scc.client.rerun_all ( measurement_id, False )
        datalog.update_measurement_by_scc_id ( measurement_id, ("uploaded", True) )
        
        if datalog.config["last_processed_date"] is None:
            datalog.update_config ( ("last_processed_date", measurement_date) )
        elif measurement_date > datalog.config["last_processed_date"]:
            datalog.update_config ( ("last_processed_date", measurement_date) )
            
        return measurement_id
    elif measurement_exists and not replace:
        # Simply mark the measurement for download without reuploading or reprocessing
        logger.debug ( "Measurement already exists in the SCC, skipping reprocessing." )
        datalog.update_measurement_by_scc_id ( measurement_id, ("uploaded", True) )
        
        if datalog.config["last_processed_date"] is None:
            datalog.update_config ( ("last_processed_date", measurement_date) )
        elif measurement_date > datalog.config["last_processed_date"]:
            datalog.update_config ( ("last_processed_date", measurement_date) )
            
        return measurement_id
    
    measurement_id = os.path.splitext ( os.path.basename(file_path) ) [0]
    
    can_download = scc.UploadMeasurement ( file_path, system_id, config.maximum_upload_retry_count, replace )

    if can_download == True:
        logger.debug ( "Successfully uploaded to SCC", extra={'scope': measurement_id})
        if datalog.config["last_processed_date"] is None:
            datalog.update_config ( ("last_processed_date", measurement_date) )
        elif measurement_date > datalog.config["last_processed_date"]:
            datalog.update_config ( ("last_processed_date", measurement_date) )
            
        datalog.update_measurement_by_scc_id ( measurement_id, ("uploaded", True) )
        return measurement_id
    else:
        datalog.update_measurement_by_scc_id ( measurement_id, ( "result", "Error uploading to SCC" ) )
        
    return None
    
def ResumePastWork ():
    if not datalog.load():
        return []
        
    if len(datalog.measurements.keys()) < 1:
        return []
        
    logger.warning ("obiwan was interrupted during previous task. Resuming...")
    
    if not datalog.config["convert"] and not scc.logged_in:
        scc.Login()

    resume_download = [
        measurement["scc_measurement_id"] for measurement in datalog.measurements.values()
        if not measurement["downloaded"] and datalog.config["download"] and measurement["uploaded"]
    ]

    resume_convert = [
        measurement for measurement in datalog.measurements.values()
        if not measurement["converted"]
    ]

    resume_upload = [
        measurement for measurement in datalog.measurements.values()
        if not datalog.config["convert"] and measurement["converted"] and not measurement["uploaded"]
    ]
    
    logger.warning (f"Not converted: {len(resume_convert)}, not uploaded: {len(resume_upload)}, not downloaded: {len(resume_download)} ")

    if len(resume_convert):
        logger.info(f"Retrying to convert {len(resume_convert)} measurements...")
        
    for measurement in resume_convert:
        try:
            file_path, measurement_id = Convert ( datalog.config["yaml"], measurement["licel_measurement"] )
            convert_resumed.append ( measurement["licel_measurement"] )
            
            if not measurement_id:
                continue
            
            if datalog.config["debug"]:
                if datalog.config["yaml"].measurements_debug_dir:
                    DebugMeasurement (measurement["licel_measurement"], file_path, datalog.config["yaml"].measurements_debug_dir)
                    
            logger.info(f"{measurement_id} added to list of measurements to be uploaded.")
            resume_upload.append(measurement)
        except Exception as e:
            logger.error ( "Error processing measurement: %s" % (str(e)) )
            
    if len(resume_upload):
        logger.info(f"Retrying to upload {len(resume_upload)} measurements...")
        
    for measurement in resume_upload:
        try:
            if not datalog.config["convert"]:
                measurement_id = Upload (
                    datalog.config["yaml"],
                    measurement["scc_measurement_id"],
                    measurement["licel_measurement"].DataFiles()[-1].EndDateTime(),
                    measurement["scc_netcdf_path"],
                    reprocess = datalog.config["reprocess"],
                    replace = datalog.config["replace"]
                )
                
                upload_resumed.append ( measurement["licel_measurement"] )
                
                if measurement_id:
                    logger.info(f'{measurement["scc_measurement_id"]} added to list of measurements to be downloaded.')
                    resume_download.append(measurement["scc_measurement_id"])
                    
        except Exception as e:
            logger.error ( "Error processing measurement: %s" % (str(e)) )
            
    return resume_download

parser = argparse.ArgumentParser(description="Tool for processing Licel lidar measurements using the Single Calculus Chain.")
parser.add_argument("folder", help="The path to the folder you want to scan.")
parser.add_argument("--datalog", help="Path of the Datalog CSV you want to save the processing log in.", default="datalog.csv")
parser.add_argument("--startdate", help="The path to the folder you want to scan.")
parser.add_argument("--enddate", help="The path to the folder you want to scan.")
parser.add_argument("--cfg", help="Configuration file for this script.", default=None)
parser.add_argument("--verbose", "-v", help="Verbose output level.", action="count")
parser.add_argument("--replace", "-r", help="Replace measurements that already exist in the SCC database.", action="store_true")
parser.add_argument("--reprocess", "-p", help="Reprocess measurements that already exist in the SCC database, skipping the reupload.", action="store_true")
parser.add_argument("--download", "-d", help="Download SCC products after processing", action="store_true")
parser.add_argument("--wait", "-w", help="Wait for SCC", action="store_true",dest="wait")
parser.add_argument("--convert", "-c", help="Convert files to SCC NetCDF without submitting", action="store_true")
parser.add_argument("--continuous", help="Use for continuous measuring systems", action="store_true")
parser.add_argument("--resume", help="Tries to resume past, interrupted, processing if possible.", action="store_true")
parser.add_argument("--test-files", help="Copies any raw test files to tests folder.", action="store_true", dest="test_files")
parser.add_argument("--debug", help="Copies raw measurement files and resulting NetCDF files in the debug folder.", action="store_true")

args = parser.parse_args ()

if args.verbose is not None:
    SetLogLevel ( args.verbose )

if args.folder is None:
    logger.error ( "You must specify the data folder. Exiting..." )
    parser.print_help()
    sys.exit (1)
        
try:
    config = Config ( args.cfg )
except Exception as e:
    logger.error ( "Error loading configuration file. Exiting..." )
    sys.exit (1)
    
scc.Initialize(
    config.scc_basic_credentials,
    config.scc_output_dir,
    config.scc_base_url,
    config.scc_website_credentials
)

datalog_path = os.path.join ( config.netcdf_out_dir, SWAP_FILE_NAME )
UseSwapFile ( datalog_path )
UseCsvDatalog ( args.datalog )

test_lists = config.test_lists

sys.path.append ( os.path.dirname (config.netcdf_parameters_path) )
netcdf_parameters_filename = os.path.basename ( config.netcdf_parameters_path )
if netcdf_parameters_filename.endswith ('.py'):
    netcdf_parameters_filename = netcdf_parameters_filename[:-3]

nc_parameters_module = importlib.import_module ( netcdf_parameters_filename )

class CustomLidarMeasurement(LicelLidarMeasurement):
    extra_netcdf_parameters = nc_parameters_module

lidarchive = lidarchive.Lidarchive ( measurement_location = config.measurement_location, dark_location = config.dark_location, tests = config.test_lists )
lidarchive.SetFolder (args.folder)

if args.startdate is None:
    start_date = None
else:
    start_date = datetime.datetime.strptime( args.startdate, '%Y%m%d%H%M%S' )
    
if args.enddate is None:
    end_date = None
else:
    end_date = datetime.datetime.strptime( args.enddate, '%Y%m%d%H%M%S' )
    
to_download = []

if args.continuous:
    logger.debug(datalog.config)
    
    last_processed_date = datalog.config.get("last_processed_date", None)
    
    if last_processed_date is not None:
        if start_date is not None and start_date < last_processed_date:
            start_date = last_processed_date
        if start_date is None:
            start_date = last_processed_date

system_index = SystemIndex (config.scc_configurations_folder)

if not args.convert:
    scc.Login()

if args.resume:
    resume_download = ResumePastWork ()
    to_download += resume_download
    # datalog.reset()

log_header_run_time = "Run started at %s" % ( datetime.datetime.now().strftime ( "%Y-%m-%d %H:%M:%S" ) )
logger.info ( log_header_run_time, extra={'scope': 'start'} )

log_header_cfg = "Configuration file = %s" % ( config.file_path )
logger.info ( log_header_cfg )

log_header_folder = "Data folder = %s" % os.path.abspath ( args.folder )
logger.info ( log_header_folder )

start_time_text = "N/A" if start_date is None else start_date.strftime ( "%Y-%m-%d %H:%M:%S" )
log_header_start_time = "Minimum start time = %s" % ( start_time_text )
logger.info ( log_header_start_time )

end_time_text = "N/A" if end_date is None else end_date.strftime ( "%Y-%m-%d %H:%M:%S" )
log_header_end_time = "Maximum end time = %s" % (end_time_text)
logger.info ( log_header_end_time )

log_header_gap = "Maximum gap between measurements (seconds) = %d" % config.max_acceptable_gap
logger.info ( log_header_gap )

logger.info ( "Identifying measurements. This can take a few minutes...")

lidarchive.ReadFolder (start_date, end_date)
logger.debug ( "Found %d files" % len (lidarchive.Measurements()) )

licel_measurements = lidarchive.ContinuousMeasurements (config.max_acceptable_gap, config.min_acceptable_length, config.max_acceptable_length, config.center_type)
logger.info ( "Identified %d different continuous measurements with a maximum acceptable gap of %ds" % (len (licel_measurements), config.max_acceptable_gap) )

if args.test_files:
    logger.info("Copying test files...")
    lidarchive.CopyTestFiles ( config.tests_dir )

for licel_measurement in licel_measurements:
    datalog.update_measurement ( licel_measurement.Id(), ("licel_measurement", licel_measurement), save=False )
    datalog.update_measurement ( licel_measurement.Id(), ("scc_netcdf_path", ""), save=False )
    datalog.update_measurement ( licel_measurement.Id(), ("converted", False), save=False )
    datalog.update_measurement ( licel_measurement.Id(), ("uploaded", False) )
    datalog.update_measurement ( licel_measurement.Id(), ("downloaded", False), save=False )
    datalog.update_measurement ( licel_measurement.Id(), ("system_id", None), save=False )
    datalog.update_measurement ( licel_measurement.Id(), ("scc_measurement_id", None), save=False )
    datalog.update_measurement ( licel_measurement.Id(), ("already_on_scc", False), save=False )
    datalog.update_measurement ( licel_measurement.Id(), ("result", ""), save=False )
    datalog.update_measurement ( licel_measurement.Id(), ("scc_version", ""), save=False )
    datalog.update_measurement ( licel_measurement.Id(), ("process_start", datetime.datetime.now()), save=False )
    
datalog.update_config(("convert", args.convert), save=False)
datalog.update_config(("reprocess", args.reprocess), save=False)
datalog.update_config(("replace", args.replace), save=False)
datalog.update_config(("download", args.download), save=False)
datalog.update_config(("folder", os.path.abspath(args.folder)), save=False)
datalog.update_config(("last_processed_date", None), save=False)
datalog.update_config(("debug", args.debug), save=False)
datalog.update_config(("yaml", config), save=True)

logger.info ( "Starting processing" )

for index, licel_measurement in enumerate(licel_measurements):
    # try:
    logger.info ( f"Started processing measurement {index+1}/{len(licel_measurements)}" )
    
    if licel_measurement.Id() in [m.Id() for m in convert_resumed]:
        logger.warning ( "This measurement measurement {licel_measurement.Id()} was processed already because of --convert. Skipping it." )
        continue
    
    file_path, measurement_id = Convert ( config, licel_measurement )

    if not measurement_id:
        continue

    if args.debug:
        if config.measurements_debug_dir:
            DebugMeasurement (licel_measurement, file_path, config.measurements_debug_dir)
            
    if not args.convert:
        if licel_measurement.Id() in [m.Id() for m in upload_resumed]:
            logger.warning ( "This measurement measurement {licel_measurement.Id()} was processed already because of --convert. Skipping it." )
            continue
            
        measurement_id = Upload (
            config,
            measurement_id,
            licel_measurement.DataFiles()[-1].EndDateTime(),
            file_path,
            reprocess = args.reprocess,
            replace = args.replace
        )
        
        if measurement_id:
            to_download.append(measurement_id)
            
    # except Exception as e:
        # logger.error ( "Error processing measurement: %s" % (str(e)) )

if args.download:
    logger.info ( "Downloading SCC products" )
    
    to_download = list(set(to_download))
    
    for measurement_id in to_download:
        if args.wait:
            result = scc.client.monitor_processing ( measurement_id, exit_if_missing = not args.wait )
            logger.debug ( "Waiting for processing to finish and downloading files...", extra={'scope': measurement_id} )
        else:
            result, _ = scc.client.get_measurement(measurement_id)
            
        try:
            if result is not None:
                logger.debug ( "Processing finished", extra={'scope': measurement_id} )
                
                try:
                    scc_version = scc.GetSCCVersion ( scc.client.output_dir, measurement_id )
                except Exception as e:
                    if result.elpp != 127:
                        logger.error ( "No SCC products found", extra={'scope': measurement_id} )
                        datalog.update_measurement_by_scc_id( measurement_id, ("result", "No SCC products found") )
                    else:
                        logger.error ( "Unknown error in SCC products", extra={'scope': measurement_id} )
                        datalog.update_measurement_by_scc_id( measurement_id, ("result", "Unknown error in SCC products") )
                    
                    scc_version = "Unknown SCC Version! Check preprocessed NetCDF files."
                    logger.error ( e )
                    continue
                    
                logger.info ( scc_version, extra={'scope': measurement_id} )
                datalog.update_measurement_by_scc_id( measurement_id, ("downloaded", True) )
                datalog.update_measurement_by_scc_id( measurement_id, ("result", scc.client.output_dir) )
                datalog.update_measurement_by_scc_id( measurement_id, ("scc_version", scc_version) )
            elif args.wait:
                logger.error ( "Download failed", extra={'scope': measurement_id} )
                datalog.update_measurement_by_scc_id( measurement_id, ("result", "Error downloading SCC products") )
            else:
                logger.info ( "Measurement was not yet processed by the SCC, will not wait for it.", extra={'scope': measurement_id} )
                datalog.update_measurement_by_scc_id( measurement_id, ("result", "SCC did not finish processing in due time.") )
        except Exception as e:
            logger.error ( f"Error downloading SCC products: {str(e)}" )
            datalog.update_measurement_by_scc_id( measurement_id, ("result", "Error downloading SCC products") )
            
if args.datalog is not None:
    datalog.write_csv()

# Delete swap file
datalog.reset_measurements()
datalog.save()
sys.exit (0)
