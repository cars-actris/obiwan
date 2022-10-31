import logging
import pickle
import os
import datetime

'''
Module intended exports
'''
datalog = None
logger = None
console = None

# Set up logging
log_format = '%(asctime)s %(levelname)-8s %(scope)-12s %(message)s'
LOG_FILE = "obiwan.log"

def UseSwapFile ( file_path ):
    global datalog
    datalog.set_file_path ( file_path )
    
def UseCsvDatalog ( file_path ):
    global datalog
    datalog.set_csv_path ( file_path )
            
def SetLogLevel ( level ):
    global logger, console
    
    if level == 1:
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
    def __init__ ( self, file_path = None ):
        self.measurements = {}
        self.config = {}
        self.file_path = file_path
        self.csv_path = None
        
    def set_file_path ( self, file_path ):
        self.file_path = file_path
        
    def load ( self ):
        try:
            with open ( self.file_path, 'rb' ) as file:
                info = pickle.load(file)
                
            self.config = info["config"]
            self.measurements = info["measurements"]
            
            return len(self.config.keys()) > 0
        except:
            self.reset()
            
        return False
        
    def save ( self ):
        with open ( self.file_path, 'wb' ) as file:
            pickle.dump({
                "config": self.config,
                "measurements": self.measurements
            }, file)
            
    def reset ( self ):
        self.reset_measurements ()
        self.config = {}
        
    def reset_measurements ( self ):
        self.measurements = {}
        
    def initialize_measurement ( self, measurement ):
        self.update_measurement ( measurement.Id(), ("measurement", measurement), save=False )
        self.update_measurement ( measurement.Id(), ("scc_netcdf_path", ""), save=False )
        self.update_measurement ( measurement.Id(), ("converted", False), save=False )
        self.update_measurement ( measurement.Id(), ("uploaded", False) )
        self.update_measurement ( measurement.Id(), ("downloaded", False), save=False )
        self.update_measurement ( measurement.Id(), ("system_id", None), save=False )
        self.update_measurement ( measurement.Id(), ("scc_measurement_id", None), save=False )
        self.update_measurement ( measurement.Id(), ("already_on_scc", False), save=False )
        self.update_measurement ( measurement.Id(), ("result", ""), save=False )
        self.update_measurement ( measurement.Id(), ("scc_version", ""), save=False )
        self.update_measurement ( measurement.Id(), ("process_start", datetime.datetime.now()), save=False )
        self.update_measurement ( measurement.Id(), ("folder", self.config.get("folder", False)), save = False )
        
        upload_enabled = not self.config.get("convert", False)
        download_enabled = not self.config.get("convert", False) and self.config.get("download", True)
        debug_enabled = self.config.get("debug", False)
        reprocess_enabled = self.config.get("reprocess", False)
        replace_enabled = self.config.get("replace", False)
        wait_enabled = self.config.get("wait", False)
        
        self.update_measurement ( measurement.Id(), ("want_convert", True), save = False )
        self.update_measurement ( measurement.Id(), ("want_upload", upload_enabled), save = False )
        self.update_measurement ( measurement.Id(), ("want_download", download_enabled), save = False )
        self.update_measurement ( measurement.Id(), ("want_debug", debug_enabled), save = False )
        self.update_measurement ( measurement.Id(), ("reprocess_enabled", reprocess_enabled), save = False )
        self.update_measurement ( measurement.Id(), ("replace_enabled", replace_enabled), save = False )
        self.update_measurement ( measurement.Id(), ("wait_enabled", wait_enabled), save = False )

    def update_config ( self, kvp, save = True ):
        self.config[ kvp[0] ] = kvp[1]
        
        if save:
            self.save()
            
    def update_measurement ( self, measurement_id, kvp, save = True ):
        if measurement_id not in self.measurements.keys():
            self.measurements[ measurement_id ] = {}
            
        self.measurements[ measurement_id ][ kvp[0] ] = kvp[1]
        
        if save:
            self.save()
        
    def update_measurement_by_scc_id ( self, scc_id, kvp, save = True ):
        for key in self.measurements.keys():
            try:
                if self.measurements[ key ][ "scc_measurement_id" ] == scc_id:
                    self.measurements[ key ][ kvp[0] ] = kvp[1]
                    
                    if save:
                        self.save()
                        
                    return
            except:
                pass
                
    def get_measurement_by_scc_id ( self, scc_id ):
        for key in self.measurements.keys():
            if self.measurements[ key ][ "scc_measurement_id" ] == scc_id:
                return self.measurements[ key ]
                
    def set_csv_path ( self, file_path ):
        self.csv_path = os.path.abspath ( file_path )
                
    def write_csv ( self ):
        if self.csv_path is None:
            return
            
        logger.info (f"Saving datalog to {self.csv_path}")
            
        if not os.path.isfile ( self.csv_path ):
            with open ( self.csv_path, 'w' ) as csvfile:
                csvfile.write ( "Process Start,Obiwan ID,Data Folder,NC Folder,NC File,SCC System ID,Measurement ID,Uploaded,Downloaded,SCC Version,Result" )
                
        with open (self.csv_path, 'a') as csvfile:
            for id, measurement in self.measurements.items():
                process_start = measurement.get("process_start", "N/A")
                
                try:
                    # Path must be valid!
                    assert (len(measurement["scc_netcdf_path"]) > 0)
                    
                    path = measurement["scc_netcdf_path"]
                    netcdf_folder = os.path.dirname ( path )
                    netcdf_file = os.path.basename ( path )
                except:
                    netcdf_folder = "N/A"
                    netcdf_file = "N/A"
                    
                try:
                    # Path must be valid!
                    assert (len(measurement["folder"]) > 0)
                    
                    data_folder = os.path.abspath ( measurement["folder"] )
                except:
                    data_folder = "N/A"
                    
                csvfile.write ("\n%s,%s,%s,%s,%s,%s,%s,%s,%s,\"%s\",%s" % (
                    measurement.get("process_start", "N/A"),
                    id,
                    data_folder,
                    netcdf_folder,
                    netcdf_file,
                    measurement.get("system_id", "N/A"),
                    measurement.get("scc_measurement_id", "N/A"),
                    measurement.get("uploaded", "N/A"),
                    measurement.get("downloaded", "N/A"),
                    measurement.get("scc_version", "N/A"),
                    measurement.get("result", "N/A")
                ))
        
class LoggerFactory:
    class SystemLogFilter ( logging.Filter ):
        def filter ( self, record ):
            if not hasattr ( record, 'scope' ):
                record.scope = 'main'
                
            return True
            
    class SCCLogFilter ( logging.Filter ):
        def filter ( self, record ):
            if not hasattr ( record, 'scope' ):
                record.scope = 'scc'
                
            return True
            
    class LidarLogFilter ( logging.Filter ):
        def filter ( self, record ):
            if not hasattr ( record, 'scope' ):
                record.scope = 'converter'
                
            return True
            
    @staticmethod
    def getLogger ():
        logger = logging.getLogger( 'obiwan' )
        
        logging.basicConfig (
            level = logging.INFO,
            format = log_format,
            datefmt = '%Y-%m-%d %H:%M',
            filename = LOG_FILE,
            filemode = 'w'
        )

        logger.addFilter ( LoggerFactory.SystemLogFilter() )

        formatter = logging.Formatter ( log_format, '%Y-%m-%d %H:%M' )

        logger.setLevel (logging.DEBUG)
        logging.getLogger ( 'scc_access.scc_access' ).setLevel ( logging.ERROR )
        logging.getLogger ( 'scc_access.scc_access' ).addFilter ( LoggerFactory.SCCLogFilter() )

        logging.getLogger ( 'atmospheric_lidar.generic' ).setLevel ( logging.ERROR )
        logging.getLogger ( 'atmospheric_lidar.generic' ).addFilter ( LoggerFactory.LidarLogFilter() )

        console = logging.StreamHandler()
        console.setLevel ( logging.INFO )
        console.setFormatter ( formatter )
        logging.getLogger().addHandler ( console )
        
        return logger, console

datalog = Datalog ()
logger, console = LoggerFactory.getLogger()