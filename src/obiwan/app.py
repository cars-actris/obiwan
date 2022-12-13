from obiwan.repository import Lidarchive, MeasurementSet

from obiwan import obiwan
from obiwan.data import get_reader_for_type
from obiwan.log import Datalog

from obiwan.config import Config

from pathlib import Path
from typing import Optional, Union

import argparse
import datetime
import os
import sys
import shutil
import time

import traceback

def Convert ( config : Config, measurement : MeasurementSet ) -> Union[Path, None]:
    """
    Convert a measurement (data file set) to an SCC NetCDF input file.
    
    Note:
        This function will also update the obiwan.datalog as the conversion is done.
        `converted`, `scc_netcdf_path` and `result` fields are all being set in this method.
    
    Args:
        config (:obj:`Config`): Configuration used for processing.
        measurement (:obj:`MeasurementSet`): Data set to convert.
        
    Returns:
        :obj:`Path` of the written NetCDF file if successful, None otherwise.
    """
    # Find the right SCC System ID for this measurement
    try:
        system_id = obiwan.system_index.GetSystemId (measurement.DataFiles()[0])
    except ValueError as e:
        logger.error ("Couldn't determine system ID for measurement '%s': %s." % (measurement.DataFiles()[0].Path(), str(e)))
        return None
    except IndexError as e:
        logger.error ( "Could not find any data files for this measurement." )
        return None
        
    obiwan.datalog.update_task ( measurement.Id(), (Datalog.Field.SYSTEM_ID, system_id) )
    
    # Find the right handler for this file type and convert to SCC NetCDF input file
    try:
        reader = get_reader_for_type ( measurement.Type() )
        
        # We can also use this opportunity to set a more appropriate process_start information:
        obiwan.datalog.update_task ( measurement.Id(), (Datalog.Field.PROCESS_START, datetime.datetime.now()) )
        
        file_path, measurement_id = reader.convert_to_scc (
            measurement_set = measurement,
            system_id = system_id,
            output_folder = config.netcdf_out_dir,
            app_config = obiwan.config
        )
    except Exception as e:
        obiwan.logger.error (f"Unknown measurement type.")
        traceback.print_exc()
        return None
        
    if file_path is None:
        # Conversion has failed
        return None
    
    obiwan.datalog.update_task ( measurement.Id(), (Datalog.Field.CONVERTED, True) )
    obiwan.datalog.update_task ( measurement.Id(), (Datalog.Field.SCC_MEASUREMENT_ID, measurement_id) )
    obiwan.datalog.update_task ( measurement.Id(), (Datalog.Field.SCC_NETCDF_PATH, file_path) )
    obiwan.datalog.update_task ( measurement.Id(), (Datalog.Field.RESULT, "Converted to SCC NetCDF") )
    
    return file_path
    
def DebugMeasurement ( measurement : MeasurementSet, measurements_debug_dir : Path ) -> None:
    """
    Copy raw data files and, if available, the SCC NetCDF file to the specified folder
    for debugging purposes regarding which files were converted.
    
    Note:
        Each measurement set will be copied inside its own subfolder, inside the specified
        `measurements_debug_dir`.
    
    Args:
        measurement (:obj:`MeasurementSet`): Measurement set to debug.
        measurements_debug_dir (:obj:`Path`): Path to the folder where the files should be copied to.
    """
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
    
    # obiwan.logger.debug ("Raw data files:")
    for file in measurement.DataFiles():
        # obiwan.logger.debug ( os.path.basename(file.Path()) )
        if measurements_debug_dir:
            shutil.copy2 ( file.Path(), debug_dir )
        
    # obiwan.logger.debug ("Raw dark files:")
    for file in measurement.DarkFiles():
        # obiwan.logger.debug ( os.path.basename(file.Path()) )
        if measurements_debug_dir:
            shutil.copy2 ( file.Path(), debug_dark_dir )
            
    # obiwan.logger.debug ("SCC NetCDF file: %s" % ( os.path.basename(measurement_path) ))
    if measurements_debug_dir:
        measurement_path = obiwan.datalog.task_info ( measurement.Id(), Datalog.Field.SCC_NETCDF_PATH )
        shutil.copy2 ( measurement_path, debug_dir )
            
def Upload (config : Config, measurement : MeasurementSet, **kwargs) -> Union[str, None]:
    """
    Upload a measurement to the SCC. This method checks if the SCC NetCDF file is available or not.
    
    Args:
        config (:obj:`Config`): Configuration used for processing.
        measurement (:obj:`MeasurementSet`): Data set to upload.
        **reprocess (bool): Flag whether to reprocess the measurement in case it already exists in the SCC database.
        **replace (bool): Flag whether to reupload the measurement in case it already exists in the SCC database.
        
    Returns:
        `str` containing the SCC measurement ID if the upload is successful, None otherwise.
    """
    measurement_id = obiwan.datalog.task_info ( measurement.Id(), Datalog.Field.SCC_MEASUREMENT_ID )
    file_path = obiwan.datalog.task_info ( measurement.Id(), Datalog.Field.SCC_NETCDF_PATH )
    measurement_date = obiwan.datalog.task_info ( measurement.Id(), Datalog.Field.MEASUREMENT ).DataFiles()[0].StartDateTime()
    
    if measurement_id is None:
        obiwan.logger.error ( "Could not determine SCC measurement ID" )
        return None
        
    if file_path is None:
        obiwan.logger.error ( "Could not read SCC NetCDF file" )
        return None
    
    reprocess = kwargs.get("reprocess", True)
    replace = kwargs.get("replace", True)
    
    system_id = obiwan.datalog.task_info ( measurement.Id(), Datalog.Field.SYSTEM_ID )
    
    if system_id is None:
        obiwan.logger.error ( "Measurement does not belong to any known SCC system." )
        return None
    
    measurement_exists = False
    existing_measurement, _ = obiwan.scc.client.get_measurement( measurement_id )
    
    obiwan.datalog.update_task ( measurement.Id(), (Datalog.Field.ALREADY_ON_SCC, measurement_exists) )
    
    if existing_measurement is not None:
        measurement_exists = True
    
    if measurement_exists and reprocess:
        # Reprocess the measurement and mark it for download
        obiwan.logger.debug ( "Measurement already exists in the SCC, triggering reprocessing." )
        obiwan.scc.client.rerun_all ( measurement_id, False )
        obiwan.datalog.update_task ( measurement.Id(), (Datalog.Field.UPLOADED, True) )
        
        if obiwan.datalog.config[Datalog.Field.LAST_PROCESSED_DATE] is None:
            obiwan.datalog.update_config ( (Datalog.Field.LAST_PROCESSED_DATE, measurement_date) )
        elif measurement_date > obiwan.datalog.config[Datalog.Field.LAST_PROCESSED_DATE]:
            obiwan.datalog.update_config ( (Datalog.Field.LAST_PROCESSED_DATE, measurement_date) )
            
        return measurement_id
    elif measurement_exists and not replace:
        # Simply mark the measurement for download without reuploading or reprocessing
        obiwan.logger.debug ( "Measurement already exists in the SCC, skipping reprocessing." )
        obiwan.datalog.update_task ( measurement.Id(), (Datalog.Field.UPLOADED, True) )
        
        if obiwan.datalog.config[Datalog.Field.LAST_PROCESSED_DATE] is None:
            obiwan.datalog.update_config ( (Datalog.Field.LAST_PROCESSED_DATE, measurement_date) )
        elif measurement_date > obiwan.datalog.config[Datalog.Field.LAST_PROCESSED_DATE]:
            obiwan.datalog.update_config ( (Datalog.Field.LAST_PROCESSED_DATE, measurement_date) )
            
        return measurement_id
    
    measurement_id = os.path.splitext ( os.path.basename(file_path) ) [0]
    
    can_download = obiwan.scc.UploadMeasurement ( file_path, system_id, config.maximum_upload_retry_count, replace )

    if can_download == True:
        obiwan.logger.info ( "Successfully uploaded to SCC", extra={'scope': measurement_id})
        if obiwan.datalog.config[Datalog.Field.LAST_PROCESSED_DATE] is None:
            obiwan.datalog.update_config ( (Datalog.Field.LAST_PROCESSED_DATE, measurement_date) )
        elif measurement_date > obiwan.datalog.config[Datalog.Field.LAST_PROCESSED_DATE]:
            obiwan.datalog.update_config ( (Datalog.Field.LAST_PROCESSED_DATE, measurement_date) )
            
        obiwan.datalog.update_task( measurement.Id(), (Datalog.Field.UPLOADED, True) )
        return measurement_id
    else:
        obiwan.datalog.update_task ( measurement.Id(), ( Datalog.Field.RESULT, "Error uploading to SCC" ) )
        
    return None
    
def LogProcessingParameters (
    config : Config,
    args : argparse.Namespace,
    start_date : Optional[datetime.datetime],
    end_date : Optional[datetime.datetime]
) -> None:
    """
    Log messages about the main processing parameters being used.
    
    Args:
        config (:obj:`Config`): Configuration being used for processing.
        args (:obj:`argparse.Namespace`): Parsed command line arguments.
        start_date (:obj:`datetime`, Optional): Earliest date for data files to be considered.
        end_date (:obj:`datetime`, Optional): Latest date for data files to be considered.
    """
    log_header_run_time = "Run started at %s" % ( datetime.datetime.now().strftime ( "%Y-%m-%d %H:%M:%S" ) )
    obiwan.logger.info ( log_header_run_time, extra={'scope': 'start'} )

    log_header_cfg = "Configuration file = %s" % ( config.file_path )
    obiwan.logger.info ( log_header_cfg )

    log_header_folder = "Data folder = %s" % os.path.abspath ( args.folder )
    obiwan.logger.info ( log_header_folder )

    start_time_text = "N/A" if start_date is None else start_date.strftime ( "%Y-%m-%d %H:%M:%S" )
    log_header_start_time = "Minimum start time = %s" % ( start_time_text )
    obiwan.logger.info ( log_header_start_time )

    end_time_text = "N/A" if end_date is None else end_date.strftime ( "%Y-%m-%d %H:%M:%S" )
    log_header_end_time = "Maximum end time = %s" % (end_time_text)
    obiwan.logger.info ( log_header_end_time )

    log_header_gap = "Maximum gap between measurements (seconds) = %d" % config.max_acceptable_gap
    obiwan.logger.info ( log_header_gap )

    log_header_minlength = "Minimum measurement set length (seconds) = %d" % config.min_acceptable_length
    obiwan.logger.info ( log_header_minlength )

    log_header_maxlength = "Maximum measurement set length (seconds) = %d" % config.max_acceptable_length
    obiwan.logger.info ( log_header_maxlength )

    log_header_alignment = "Measurement set alignment = %s" % config.alignment_type
    obiwan.logger.info ( log_header_alignment )
    
def LogInterruptedWork () -> None:
    """
    Log messages about unfinised work from the past. This will identify how many measurements
    still need converting, how many still need uploading and finally how many still need downloading.
    """
    if obiwan.datalog.load() is not None:
        to_download = [
            task[Datalog.Field.SCC_MEASUREMENT_ID] for task in obiwan.datalog.tasks.values()
            if not task[Datalog.Field.DOWNLOADED] and task[Datalog.Field.WANT_DOWNLOAD] and task[Datalog.Field.UPLOADED]
        ]

        to_convert = [
            task for task in obiwan.datalog.tasks.values()
            if not task[Datalog.Field.CONVERTED]
        ]

        to_upload = [
            task for task in obiwan.datalog.tasks.values()
            if not obiwan.datalog.config[Datalog.Field.CONVERT] and task[Datalog.Field.CONVERTED] and not task[Datalog.Field.UPLOADED]
        ]
        
        if len(to_download) > 0 or len(to_convert) > 0 or len(to_upload) > 0:
            obiwan.logger.warning ("Found previous unfinished tasks")
            obiwan.logger.warning (f"Not converted: {len(to_convert)}, not uploaded: {len(to_upload)}, not downloaded: {len(to_download)} ")
            
def DownloadMeasurements ( wait : bool = True ) -> None:
    """
    Download measurements from the SCC.
    
    Note:
        The list of measurements that need to be downloaded is computed from the
        internal obiwan.datalog of obiwan.
        
    Args:
        wait (bool): If True, the method will wait for each measurement to finish processing
            on the SCC. If False, the method will only download measurements that already
            have been processed and will not wait for others.
    """
    seen = set()
    to_download= [
        task for task in obiwan.datalog.tasks.values()
        if task[Datalog.Field.SCC_MEASUREMENT_ID] not in seen and not seen.add(task[Datalog.Field.SCC_MEASUREMENT_ID]) and
        task[Datalog.Field.WANT_DOWNLOAD] and not task[Datalog.Field.DOWNLOADED]
    ]

    for task in to_download:
        measurement = task[Datalog.Field.MEASUREMENT]
        
        if task[Datalog.Field.WAIT_ENABLED]:
            result = obiwan.scc.client.monitor_processing ( task[Datalog.Field.SCC_MEASUREMENT_ID], exit_if_missing = not task[Datalog.Field.WAIT_ENABLED] )
            obiwan.logger.debug ( "Waiting for processing to finish and downloading files...", extra={'scope': task[Datalog.Field.SCC_MEASUREMENT_ID]} )
        else:
            result, _ = obiwan.scc.client.get_measurement(task[Datalog.Field.SCC_MEASUREMENT_ID])
            
        try:
            if result is not None:
                try:
                    scc_version = obiwan.scc.GetSCCVersion ( obiwan.scc.client.output_dir, task[Datalog.Field.SCC_MEASUREMENT_ID] )
                except Exception as e:
                    if result.elpp != 127:
                        obiwan.logger.error ( "No SCC products found", extra={'scope': task[Datalog.Field.SCC_MEASUREMENT_ID]} )
                        obiwan.datalog.update_task( measurement.Id(), (Datalog.Field.RESULT, "No SCC products found") )
                    else:
                        obiwan.logger.error ( "Unknown error in SCC products", extra={'scope': task[Datalog.Field.SCC_MEASUREMENT_ID]} )
                        obiwan.datalog.update_task( measurement.Id(), (Datalog.Field.RESULT, "Unknown error in SCC products") )
                    
                    scc_version = "Unknown SCC Version! Check preprocessed NetCDF files."
                    obiwan.logger.error ( e )
                    continue
                    
                obiwan.datalog.update_task( measurement.Id(), (Datalog.Field.DOWNLOADED, True) )
                obiwan.datalog.update_task( measurement.Id(), (Datalog.Field.RESULT, obiwan.scc.client.output_dir) )
                obiwan.datalog.update_task( measurement.Id(), (Datalog.Field.SCC_VERSION, scc_version) )
            elif task[Datalog.Field.WAIT_ENABLED]:
                obiwan.logger.error ( "Download failed", extra={'scope': task[Datalog.Field.SCC_MEASUREMENT_ID]} )
                obiwan.datalog.update_task( measurement.Id(), (Datalog.Field.RESULT, "Error downloading SCC products") )
            else:
                obiwan.logger.info ( "Measurement was not yet processed by the SCC, will not wait for it.", extra={'scope': task[Datalog.Field.SCC_MEASUREMENT_ID]} )
                obiwan.datalog.update_task( measurement.Id(), (Datalog.Field.RESULT, "SCC did not finish processing in due time.") )
        except Exception as e:
            obiwan.logger.error ( f"Error downloading SCC products: {str(e)}" )
            obiwan.datalog.update_task( measurement.Id(), (Datalog.Field.RESULT, "Error downloading SCC products") )
    
def main ():
    # Initialize data repository for the required folder
    lidarchive = Lidarchive (
        measurement_identifiers = obiwan.config.measurement_identifiers,
        dark_identifiers = obiwan.config.dark_identifiers,
        tests = obiwan.config.test_lists
    )
    lidarchive.SetFolder (obiwan.args.folder)
    
    # Print a nice header when starting processing
    LogProcessingParameters ( config = obiwan.config, args = obiwan.args, start_date = obiwan.args.startdate, end_date = obiwan.args.enddate)
    
    obiwan.logger.info ( "Identifying measurements. This can take a few minutes...")

    lidarchive.ReadFolder (obiwan.args.startdate, obiwan.args.enddate)
    obiwan.logger.debug ( "Found %d files" % len(lidarchive.Measurements()) )
    
    # Check if we have any interrupted work from past runs and print a message if so
    if obiwan.args.resume:
        LogInterruptedWork ()
        
    if obiwan.args.test_files:
        obiwan.logger.info("Copying test files...")
        lidarchive.CopyTestFiles ( obiwan.config.tests_dir )

    obiwan.datalog.update_config((Datalog.Field.CONVERT, obiwan.args.convert), save=False)
    obiwan.datalog.update_config((Datalog.Field.REPROCESS, obiwan.args.reprocess), save=False)
    obiwan.datalog.update_config((Datalog.Field.REPLACE, obiwan.args.replace), save=False)
    obiwan.datalog.update_config((Datalog.Field.DOWNLOAD, obiwan.args.download), save=False)
    obiwan.datalog.update_config((Datalog.Field.WAIT, obiwan.args.wait), save=False)
    obiwan.datalog.update_config((Datalog.Field.FOLDER, os.path.abspath(obiwan.args.folder)), save=False)
    obiwan.datalog.update_config((Datalog.Field.LAST_PROCESSED_DATE, None), save=False)
    obiwan.datalog.update_config((Datalog.Field.DEBUG, obiwan.args.debug), save=False)
    obiwan.datalog.update_config((Datalog.Field.FOLDER, obiwan.args.folder), save = False)
    obiwan.datalog.update_config((Datalog.Field.CONFIGURATION_FILE, obiwan.config), save=True)
        
    scanned_measurements = lidarchive.ContinuousMeasurements (
        obiwan.config.max_acceptable_gap,
        obiwan.config.min_acceptable_length,
        obiwan.config.max_acceptable_length,
        obiwan.config.alignment_type
    )
    obiwan.logger.info ( "Identified %d different continuous measurements" % (len (scanned_measurements)) )

    for measurement in scanned_measurements:
        inserted = obiwan.datalog.initialize_task ( measurement )
        
        if not inserted:
            obiwan.logger.debug ( f"Measurement {measurement.Id()} needed resuming, but was scanned again this time. Will reprocess entirely." )
        
        
    obiwan.logger.info ( f"Starting processing {len(obiwan.datalog.tasks)} tasks" )
    
    # Main loop:
    for index, task in enumerate(obiwan.datalog.tasks.values()):
        # try:
        obiwan.logger.info ( f"Started task {index+1}/{len(obiwan.datalog.tasks)}" )
        
        needs_convert = not task[Datalog.Field.CONVERTED]
        needs_upload = task[Datalog.Field.WANT_UPLOAD] and not task[Datalog.Field.UPLOADED]
        needs_download = task[Datalog.Field.WANT_DOWNLOAD] and not task[Datalog.Field.DOWNLOADED]
        needs_debug = task[Datalog.Field.WANT_DEBUG]
        
        if needs_convert:
            Convert ( obiwan.config, task[Datalog.Field.MEASUREMENT] )
            
        file_path = task[Datalog.Field.SCC_NETCDF_PATH]
        measurement_id = task[Datalog.Field.SCC_MEASUREMENT_ID]
            
        if not measurement_id or not file_path:
            obiwan.logger.error ("Measurement could not be converted.")
            continue
            
        obiwan.logger.info ( f"Successfully converted measurement to SCC NetCDF format: {os.path.basename(file_path)}" )
        
        if needs_debug:
            if obiwan.config.measurements_debug_dir:
                DebugMeasurement(task[Datalog.Field.MEASUREMENT], obiwan.config.measurements_debug_dir)
                
        if needs_upload:
            Upload (
                config = obiwan.config,
                measurement = task[Datalog.Field.MEASUREMENT],
                reprocess = task[Datalog.Field.REPROCESS_ENABLED],
                replace = task[Datalog.Field.REPLACE_ENABLED]
            )
            
    if obiwan.args.download:
        obiwan.logger.info ( "Downloading SCC products" )
        DownloadMeasurements ()
    else:
        obiwan.logger.info("SCC products download is not enabled. You can enable it with --download.")
        
    if obiwan.args.datalog is not None:
        obiwan.datalog.write_csv()

    # Delete swap file
    obiwan.datalog.reset_tasks()
    obiwan.datalog.save()
    sys.exit (0)

if __name__ == "__main__":
    main ()