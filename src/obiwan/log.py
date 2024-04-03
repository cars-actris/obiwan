import logging
import pickle
import os
import datetime

from enum import Enum
from pathlib import Path
from typing import Union, Tuple

from logging import Logger

# Module export
logger = None

def set_log_level ( level : int, logger : Logger ) -> None:
    """
    Set the log level for the specified logger object.
    
    Args:
        level (int): The logging level that should be set.
        logger (:obj:`Logger`): The logger to which to apply the selected output verbosity.
        
    Note:
        Three distinct levels of verbosity (level) are available:
            * A value of 0 will output INFO or more important messages from `obiwan`, and only ERROR
              messages from the various Python modules it uses.
            * A value of 1 will output INFO or more important messages from `obiwan`, as well as
              from the various Python modules it uses.
            * A value of 2 or greater will output DEBUG or more important messages from `obiwan`,
              as well as from the various Python modules it uses.
    """
    if level == 0 or level is None:
        for handler in logger.handlers:
            handler.setLevel(logging.INFO)
            
        for handler in logging.getLogger().handlers:
            handler.setLevel(logging.INFO)
            
        logging.getLogger ( 'scc_access.scc_access' ).setLevel ( logging.ERROR )
        logging.getLogger ( 'atmospheric_lidar.generic' ).setLevel ( logging.ERROR )
    elif level == 1:
        logging.getLogger ( 'scc_access.scc_access' ).setLevel ( logging.INFO )
        logging.getLogger ( 'atmospheric_lidar.generic' ).setLevel ( logging.INFO )
    elif level > 1:
        logger.setLevel (logging.DEBUG)
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
            
        for handler in logging.getLogger().handlers:
            handler.setLevel(logging.DEBUG)
        
        logging.getLogger ( 'scc_access.scc_access' ).setLevel ( logging.DEBUG )
        logging.getLogger ( 'atmospheric_lidar.generic' ).setLevel ( logging.DEBUG )
        
    logger.debug("Debug enabled")

class Datalog:
    """
    Class used to store the current processing state of the application. This is used to instantly
    save to the disk any change in measurement processing progress in a specific swap file,
    which can then be loaded in order to resume interrupted work.
    
    It is also used to transfer data between various modules of the `obiwan` application.
    
    Attributes:
        tasks (:obj:`Dict` of `object` keyed by `str`): The running tasks of obiwan are stored here,
            and each one of them gets a specific ID based on the measurement it refers to.
        config (:obj:`Dict` of `object` keyed by `str`): The running configuration of obiwan is stored here.
        file_path (:obj:`Path`): The path to the swap file being used.
        csv_path (:obj:`Path`): Path to a CSV log of all the processed measurements.
    """
    class Field(Enum):
        """
        Available keys in the datalog dictionaries.
        """
        # Raw data fields
        FOLDER = "folder"
        MEASUREMENT = "measurement"
        
        # Processing status fields
        PROCESS_START = "process_start"
        RESULT = "result"
        CONVERTED = "converted"
        UPLOADED = "uploaded"
        DOWNLOADED = "downloaded"
        
        # Processing parameters fields
        WANT_CONVERT = "want_convert"
        WANT_UPLOAD = "want_upload"
        WANT_DOWNLOAD = "want_download"
        WANT_DEBUG = "want_debug"
        REPROCESS_ENABLED = "reprocess_enabled"
        REPLACE_ENABLED = "replace_enabled"
        WAIT_ENABLED = "wait_enabled"
        
        # SCC information fields
        SCC_NETCDF_PATH = "scc_netcdf_path"
        SYSTEM_ID = "system_id"
        SCC_MEASUREMENT_ID = "scc_measurement_id"
        ALREADY_ON_SCC = "already_on_scc"
        SCC_VERSION = "scc_version"
        
        # Configuration file
        CONFIGURATION_FILE = "configuration_file"
        
        # Global configuration fields
        LAST_PROCESSED_DATE = "last_processed_date"
        CONVERT = "convert"
        REPROCESS = "reprocess"
        REPLACE = "replace"
        DOWNLOAD = "download"
        WAIT = "wait"
        DEBUG = "debug"
        
    def __init__ ( self, file_path : Path = None ):
        """
        Args:
            file_path (:obj:`Path`): Path to the swap file where the datalog will be stored.
        """
        self.tasks = {}
        self.config = {}
        self.file_path = file_path
        self.csv_path = None
        
    def set_file_path ( self, file_path : Path ) -> None:
        """
        Set the file path for the swap file where the datalog will be stored.
        Args:
            file_path (:obj:`Path`): Path to the swap file where the datalog will be stored.
        """
        self.file_path = file_path
        
    def load ( self ) -> None:
        """
        Load last saved state from the swap file.
        """
        try:
            with open ( self.file_path, 'rb' ) as file:
                info = pickle.load(file)
                
            self.config = info["config"]
            self.tasks = info["tasks"]
            
            return len(self.config.keys()) > 0
        except Exception:
            self.reset()
            
        return False
        
    def save ( self ) -> None:
        """
        Write the swap file with the most up-to-date processing state.
        """
        with open ( self.file_path, 'wb' ) as file:
            pickle.dump({
                "config": self.config,
                "tasks": self.tasks
            }, file)
            
    def reset ( self ) -> None:
        """
        Reset the state stored in the datalog, by resetting all tasks and configurations.
        """
        self.reset_tasks ()
        self.config = {}
        
    def reset_tasks ( self ) -> None:
        """
        Reset tasks that are being tracked in the datalog.
        """
        self.tasks = {}
        
    def initialize_task ( self, measurement : 'obiwan.repository.MeasurementSet', force_restart : bool = False ) -> bool:
        """
        Initialize a datalog task entry for a given measurement.
        
        Args:
            measurement (:obj:`obiwan.repository.MeasurementSet`): The lidar measurement set to track.
            force_restart (bool): If True, if the measurement is already being tracked or was
                loaded from the swap file, its state will be reset completely which means
                it will get reprocessed entirely.
                
        Returns:
            True if the task was initialized, False otherwise.
        """
        already_exists = measurement.Id() in self.tasks.keys()
        
        if already_exists and not force_restart:
            # Do not restart task from the beginning if it already exists and
            # we don't want to reprocess it entirely.
            return False
        
        self.update_task ( measurement.Id(), (Datalog.Field.FOLDER, self.config.get("folder", None)), save = False )
        self.update_task ( measurement.Id(), (Datalog.Field.MEASUREMENT, measurement), save=False )
        
        self.update_task ( measurement.Id(), (Datalog.Field.PROCESS_START, datetime.datetime.now()), save=False )
        self.update_task ( measurement.Id(), (Datalog.Field.RESULT, ""), save=False )
        self.update_task ( measurement.Id(), (Datalog.Field.CONVERTED, False), save=False )
        self.update_task ( measurement.Id(), (Datalog.Field.UPLOADED, False) )
        self.update_task ( measurement.Id(), (Datalog.Field.DOWNLOADED, False), save=False )
        
        upload_enabled = not self.config.get(Datalog.Field.CONVERT, False)
        download_enabled = not self.config.get(Datalog.Field.CONVERT, False) and self.config.get(Datalog.Field.DOWNLOAD, True)
        debug_enabled = self.config.get(Datalog.Field.DEBUG, False)
        reprocess_enabled = self.config.get(Datalog.Field.REPROCESS, False)
        replace_enabled = self.config.get(Datalog.Field.REPLACE, False)
        wait_enabled = self.config.get(Datalog.Field.WAIT, False)
        
        self.update_task ( measurement.Id(), (Datalog.Field.WANT_CONVERT, True), save = False )
        self.update_task ( measurement.Id(), (Datalog.Field.WANT_UPLOAD, upload_enabled), save = False )
        self.update_task ( measurement.Id(), (Datalog.Field.WANT_DOWNLOAD, download_enabled), save = False )
        self.update_task ( measurement.Id(), (Datalog.Field.WANT_DEBUG, debug_enabled), save = False )
        self.update_task ( measurement.Id(), (Datalog.Field.REPROCESS_ENABLED, reprocess_enabled), save = False )
        self.update_task ( measurement.Id(), (Datalog.Field.REPLACE_ENABLED, replace_enabled), save = False )
        self.update_task ( measurement.Id(), (Datalog.Field.WAIT_ENABLED, wait_enabled), save = False )
        
        self.update_task ( measurement.Id(), (Datalog.Field.SCC_NETCDF_PATH, ""), save=False )
        self.update_task ( measurement.Id(), (Datalog.Field.SYSTEM_ID, None), save=False )
        self.update_task ( measurement.Id(), (Datalog.Field.SCC_MEASUREMENT_ID, None), save=False )
        self.update_task ( measurement.Id(), (Datalog.Field.ALREADY_ON_SCC, False), save=False )
        self.update_task ( measurement.Id(), (Datalog.Field.SCC_VERSION, ""), save=False )
        
        return True

    def update_config ( self, kvp : Tuple[str, object], save : bool = True ) -> None:
        """
        Update the configuration parameters in the datalog and, optionally, save the datalog
        to the swap file.
        
        Args:
            kvp (:obj:`Tuple` of :obj:`str` and :obj:`object`): Tuple representing key-value pair to set
                in the configuration stored inside the datalog.
            save (bool): If True, the swap file will be immediately written.
        """
        self.config[ kvp[0] ] = kvp[1]
        
        if save:
            self.save()
            
    def update_task ( self, task_id : str, kvp : Tuple[str, object], save = True ):
        """
        Update the task state in the datalog and, optionally, save the datalog to the swap file.
        
        Args:
            kvp (:obj:`Tuple` of :obj:`str` and :obj:`object`): Tuple representing key-value pair to set
                in the task stored inside the datalog.
            save (bool): If True, the swap file will be immediately written.
        """
        if task_id not in self.tasks.keys():
            self.tasks[ task_id ] = {}
            
        self.tasks[ task_id ][ kvp[0] ] = kvp[1]
        
        if save:
            self.save()
                
    def task ( self, id : str ) -> object:
        """
        Get a specific task state from the datalog by task ID.
        
        Args:
            id (str): ID of the task.
        """
        return self.tasks.get(id, None)
        
    def task_info ( self, id, field ) -> Union[object, None]:
        """
        Get specific info from a task state stored in the datalog by task ID.
        
        Args:
            id (str): ID of the task.
            field (:obj:`Field`): Field to retrieve information from.
            
        Returns:
            Object from the specified field of the specified task, or None
            if the required information could not be retrieved.
        """
        task = self.task ( id )
        
        if task is not None:
            return task.get(field, None)
            
        return None
                
    def task_by_scc_id ( self, scc_id : str ) -> object:
        """
        Get a specific task state from the datalog by SCC measurement ID corresponding
        to this task.
        
        Args:
            scc_id (str): SCC measurement ID corresponding to the task.
        """
        for key in self.tasks.keys():
            if self.tasks[ key ][ Datalog.Field.SCC_MEASUREMENT_ID ] == scc_id:
                return self.tasks[ key ]
                
    def set_csv_path ( self, file_path : Path ) -> None:
        """
        Set the path of the processing log CSV file.
        
        Args:
            file_path (:obj:`Path`): Path where to save the CSV file.
        """
        self.csv_path = os.path.abspath ( file_path )
                
    def write_csv ( self ) -> None:
        """
        Write the processing log CSV file.
        """
        if self.csv_path is None:
            return
            
        logger.info (f"Saving datalog to {self.csv_path}")
            
        if not os.path.isfile ( self.csv_path ):
            with open ( self.csv_path, 'w' ) as csvfile:
                csvfile.write ( "Process Start,Obiwan ID,Data Folder,NC Folder,NC File,SCC System ID,Measurement ID,Uploaded,Downloaded,SCC Version,Result" )
                
        with open (self.csv_path, 'a') as csvfile:
            for id, task in self.tasks.items():
                try:
                    # Path must be valid!
                    assert (len(task[Datalog.Field.SCC_NETCDF_PATH]) > 0)
                    
                    path = task[Datalog.Field.SCC_NETCDF_PATH]
                    netcdf_folder = os.path.dirname ( path )
                    netcdf_file = os.path.basename ( path )
                except Exception:
                    netcdf_folder = "N/A"
                    netcdf_file = "N/A"
                    
                try:
                    # Path must be valid!
                    assert (len(task[Datalog.Field.FOLDER]) > 0)
                    
                    data_folder = os.path.abspath ( task [ Datalog.Field.FOLDER ] )
                except Exception:
                    data_folder = "N/A"
                    
                csvfile.write ("\n%s,%s,%s,%s,%s,%s,%s,%s,%s,\"%s\",%s" % (
                    task.get(Datalog.Field.PROCESS_START, "N/A"),
                    id,
                    data_folder,
                    netcdf_folder,
                    netcdf_file,
                    task.get(Datalog.Field.SYSTEM_ID, "N/A"),
                    task.get(Datalog.Field.SCC_MEASUREMENT_ID, "N/A"),
                    task.get(Datalog.Field.UPLOADED, "N/A"),
                    task.get(Datalog.Field.DOWNLOADED, "N/A"),
                    task.get(Datalog.Field.SCC_VERSION, "N/A"),
                    task.get(Datalog.Field.RESULT, "N/A")
                ))
        
class LoggerFactory:
    """
    Helper class used to build a :obj:`logging.Logger` for the obiwan application.
    """
    class SystemLogFilter ( logging.Filter ):
        """
        Helper class used to set a custom logging scope for the main modules.
        """
        def filter ( self, record ):
            if not hasattr ( record, 'scope' ):
                record.scope = 'main'
                
            return True
            
    class SCCLogFilter ( logging.Filter ):
        """
        Helper class used to set a custom logging scope for the SCC module.
        """
        def filter ( self, record ):
            if not hasattr ( record, 'scope' ):
                record.scope = 'scc'
                
            return True
            
    class LidarLogFilter ( logging.Filter ):
        """
        Helper class used to set a custom logging scope for the converter modules.
        """
        def filter ( self, record ):
            if not hasattr ( record, 'scope' ):
                record.scope = 'converter'
                
            return True
            
    @staticmethod
    def get_logger ():
        """
        Construct a :obj:`logging.Logger` object and do basic settings on it.
        """
        obiwan_log_format = '%(asctime)s %(levelname)-8s %(scope)-12s %(message)s'
        log_format = '%(asctime)s %(levelname)-8s %(message)s'
        LOG_FILE = "obiwan.log"
        
        logging.basicConfig (
            level = logging.INFO,
            format = obiwan_log_format,
            datefmt = '%Y-%m-%d %H:%M',
            filename = LOG_FILE,
            filemode = 'w'
        )

        logging.getLogger ( 'scc_access.scc_access' ).setLevel ( logging.ERROR )
        logging.getLogger ( 'scc_access.scc_access' ).addFilter ( LoggerFactory.SCCLogFilter() )
        
        logging.getLogger ( 'scc_access' ).setLevel ( logging.ERROR )
        logging.getLogger ( 'scc_access' ).addFilter ( LoggerFactory.SCCLogFilter() )

        logging.getLogger ( 'atmospheric_lidar.generic' ).setLevel ( logging.ERROR )
        logging.getLogger ( 'atmospheric_lidar.generic' ).addFilter ( LoggerFactory.LidarLogFilter() )
        
        logging.getLogger ( 'atmospheric_lidar' ).setLevel ( logging.ERROR )
        logging.getLogger ( 'atmospheric_lidar' ).addFilter ( LoggerFactory.LidarLogFilter() )
        
        formatter = logging.Formatter ( log_format, '%Y-%m-%d %H:%M' )
        logger = logging.getLogger( 'obiwan' )

        logger.addFilter ( LoggerFactory.SystemLogFilter() )

        console = logging.StreamHandler()
        console.setLevel ( logging.INFO )
        console.setFormatter ( formatter )
        logging.getLogger().addHandler ( console )
        
        return logger
        
logger = LoggerFactory.get_logger()