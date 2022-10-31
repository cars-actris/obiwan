from .lidarchive import lidarchive
from .lidarchive.lidarchive import MeasurementType

from obiwan.config import Config
from obiwan.log import logger, datalog, SetLogLevel, UseSwapFile, UseCsvDatalog
from obiwan.lidar import system_index
from obiwan.scc import scc

import obiwan.converters as converters

import argparse
import datetime
import os
import sys
import shutil
import time

SWAP_FILE_NAME = "obiwan.swp"
convert_resumed = []
upload_resumed = []

def Convert ( config, measurement ):
    # We can use this opportunity to set a more appropriate process_start information:
    datalog.update_measurement ( measurement.Id(), ("process_start", datetime.datetime.now()) )
    
    if measurement.Type() == MeasurementType.LICEL_V1:
        file_path, measurement_id = converters.LicelToSCC ( measurement, config.netcdf_out_dir, config.netcdf_parameters_path )
    elif measurement.Type() == MeasurementType.LICEL_V2:
        file_path, measurement_id = converters.LicelToSCCV2 ( measurement, config.netcdf_out_dir, config.netcdf_parameters_path )
    else:
        logger.error (f"Unknown measurement type.")
        return None, None
        
    if file_path is None or measurement_id is None:
        # Conversion has failed
        return None, None
        
    logger.debug (f"Identified raw data format: {measurement.Type()}")
    
    datalog.update_measurement_by_scc_id ( measurement_id, ("converted", True) )
    datalog.update_measurement_by_scc_id ( measurement_id, ("scc_netcdf_path", file_path) )
    datalog.update_measurement_by_scc_id ( measurement_id, ("result", "Converted to SCC NetCDF") )
    
    return file_path, measurement_id
    
def DebugMeasurement ( measurement, measurement_path, measurements_debug_dir ):
    if measurements_debug_dir:
        debug_date_str = measurement.DataFiles()[0].StartDateTime().strftime('%Y-%m-%d-%H-%M')
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
    for file in measurement.DataFiles():
        # logger.debug ( os.path.basename(file.Path()) )
        if measurements_debug_dir:
            shutil.copy2 ( file.Path(), debug_dir )
        
    # logger.debug ("Raw dark files:")
    for file in measurement.DarkFiles():
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

system_index.ReadFolder (config.scc_configurations_folder)

if not args.convert:
    scc.Login()

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

if args.resume:
    if datalog.load() is not None:
        to_download = [
            measurement["scc_measurement_id"] for measurement in datalog.measurements.values()
            if not measurement["downloaded"] and datalog.config["download"] and measurement["uploaded"]
        ]

        to_convert = [
            measurement for measurement in datalog.measurements.values()
            if not measurement["converted"]
        ]

        to_upload = [
            measurement for measurement in datalog.measurements.values()
            if not datalog.config["convert"] and measurement["converted"] and not measurement["uploaded"]
        ]
        
        if len(to_download) > 0 or len(to_convert) > 0 or len(to_upload) > 0:
            logger.warning ("Found previous unfinished tasks")
            logger.warning (f"Not converted: {len(to_convert)}, not uploaded: {len(to_upload)}, not downloaded: {len(to_download)} ")

new_measurements = lidarchive.ContinuousMeasurements (config.max_acceptable_gap, config.min_acceptable_length, config.max_acceptable_length, config.center_type)
logger.info ( "Identified %d different continuous measurements with a maximum acceptable gap of %ds" % (len (new_measurements), config.max_acceptable_gap) )

if args.test_files:
    logger.info("Copying test files...")
    lidarchive.CopyTestFiles ( config.tests_dir )

logger.info ( "Starting processing" )

datalog.update_config(("convert", args.convert), save=False)
datalog.update_config(("reprocess", args.reprocess), save=False)
datalog.update_config(("replace", args.replace), save=False)
datalog.update_config(("download", args.download), save=False)
datalog.update_config(("wait", args.wait), save=False)
datalog.update_config(("folder", os.path.abspath(args.folder)), save=False)
datalog.update_config(("last_processed_date", None), save=False)
datalog.update_config(("debug", args.debug), save=False)
datalog.update_config(("folder", args.folder), save = False)
datalog.update_config(("yaml", config), save=True)

for measurement in new_measurements:
    datalog.initialize_measurement ( measurement )

for index, measurement in enumerate(datalog.measurements.values()):
    # try:
    logger.info ( f"Started processing measurement {index+1}/{len(datalog.measurements)}" )
    
    needs_convert = not measurement["converted"]
    needs_upload = measurement["want_upload"] and not measurement["uploaded"]
    needs_download = measurement["want_download"] and not measurement["downloaded"]
    needs_debug = measurement["want_debug"]
    
    if needs_convert:
        file_path, measurement_id = Convert ( config, measurement["measurement"] )
    else:
        file_path = measurement["scc_netcdf_path"]
        measurement_id = measurement["scc_measurement_id"]
        
    if not measurement_id or not file_path:
        logger.error ("Measurement could not be converted. Skipping...")
        continue
    
    if needs_debug:
        if config.measurements_debug_dir:
            DebugMeasurement(measurement["measurement"], file_path, config.measurements_debug_dir)
            
    if needs_upload:
        measurement_id = Upload (
            config,
            measurement_id,
            measurement["measurement"].DataFiles[-1].EndDateTime(),
            file_path,
            reprocess = measurement["reprocess_enabled"],
            replace = measurement["replace_enabled"]
        )

to_download = list ( set ([
    measurement for measurement in datalog.measurements.values()
    if measurement["want_download"] and not measurement["downloaded"]
]))

if args.download:
    logger.info ( "Downloading SCC products" )
    
    for measurement in to_download:
        if measurement["wait_enabled"]:
            result = scc.client.monitor_processing ( measurement["scc_measurement_id"], exit_if_missing = not measurement["wait_enabled"] )
            logger.debug ( "Waiting for processing to finish and downloading files...", extra={'scope': measurement["scc_measurement_id"]} )
        else:
            result, _ = scc.client.get_measurement(measurement["scc_measurement_id"])
            
        try:
            if result is not None:
                logger.debug ( "Processing finished", extra={'scope': measurement["scc_measurement_id"]} )
                
                try:
                    scc_version = scc.GetSCCVersion ( scc.client.output_dir, measurement["scc_measurement_id"] )
                except Exception as e:
                    if result.elpp != 127:
                        print (measurement)
                        logger.error ( "No SCC products found", extra={'scope': measurement["scc_measurement_id"]} )
                        datalog.update_measurement_by_scc_id( measurement["scc_measurement_id"], ("result", "No SCC products found") )
                    else:
                        logger.error ( "Unknown error in SCC products", extra={'scope': measurement["scc_measurement_id"]} )
                        datalog.update_measurement_by_scc_id( measurement["scc_measurement_id"], ("result", "Unknown error in SCC products") )
                    
                    scc_version = "Unknown SCC Version! Check preprocessed NetCDF files."
                    logger.error ( e )
                    continue
                    
                logger.info ( scc_version, extra={'scope': measurement["scc_measurement_id"]} )
                datalog.update_measurement_by_scc_id( measurement["scc_measurement_id"], ("downloaded", True) )
                datalog.update_measurement_by_scc_id( measurement["scc_measurement_id"], ("result", scc.client.output_dir) )
                datalog.update_measurement_by_scc_id( measurement["scc_measurement_id"], ("scc_version", scc_version) )
            elif args.wait:
                logger.error ( "Download failed", extra={'scope': measurement["scc_measurement_id"]} )
                datalog.update_measurement_by_scc_id( measurement["scc_measurement_id"], ("result", "Error downloading SCC products") )
            else:
                logger.info ( "Measurement was not yet processed by the SCC, will not wait for it.", extra={'scope': measurement["scc_measurement_id"]} )
                datalog.update_measurement_by_scc_id( measurement["scc_measurement_id"], ("result", "SCC did not finish processing in due time.") )
        except Exception as e:
            logger.error ( f"Error downloading SCC products: {str(e)}" )
            datalog.update_measurement_by_scc_id( measurement["scc_measurement_id"], ("result", "Error downloading SCC products") )
else:
    logger.info("SCC products download is not enabled. You can enable it with --download.")
            
if args.datalog is not None:
    datalog.write_csv()

# Delete swap file
datalog.reset_measurements()
datalog.save()
sys.exit (0)
