import logging
import pickle
import os

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
    
def WriteProcessingLog ( file_path ):
    if not os.path.isfile ( os.path.abspath ( file_path) ):
        with open ( os.path.abspath ( file_path ), 'w' ) as csvfile:
            csvfile.write ( "Process Start,Data Folder,Data File,SCC System ID,Measurement ID,Uploaded,Downloaded,SCC Version,Result" )
            
    with open ( os.path.abspath ( file_path ), 'a' ) as csvfile:
        for measurement_id in processing_log.keys():
            csvfile.write ("\n%s,%s,%s,%s,%s,%s,%s,\"%s\",%s" % (
                processing_log[ measurement_id ][ "process_start" ],
                processing_log[ measurement_id ][ "data_folder" ],
                processing_log[ measurement_id ][ "data_file" ],
                processing_log[ measurement_id ][ "scc_system_id" ],
                measurement_id,
                processing_log[ measurement_id ][ "uploaded" ],
                processing_log[ measurement_id ][ "downloaded" ],
                processing_log[ measurement_id ][ "scc_version" ],
                processing_log[ measurement_id ][ "result" ]
            ))
            
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

class SwapFile:
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
                
    @staticmethod
    def compute_path ( path ):
        if ( os.path.isabs ( path ) ):
            # If the user configured an absolute path
            # use that without any questions asked:
            return path
            
        # Otherwise, if a relative path was provided,
        # get path relative to the parent folder of this file:
        relpath = os.path.join ( os.path.dirname ( os.path.realpath ( __file__ ) ), '..', '..', path )
        abspath = os.path.abspath ( os.path.normpath ( relpath ) )
        
        return abspath
                
    def set_csv_path ( self, file_path ):
        self.csv_path = SwapFile.compute_path ( file_path )
                
    def write_csv ( self ):
        if self.csv_path is None:
            return
            
        logger.info (f"Saving datalog to {self.csv_path}")
            
        if not os.path.isfile ( self.csv_path ):
            with open ( self.csv_path, 'w' ) as csvfile:
                csvfile.write ( "Process Start,Data Folder,Data File,SCC System ID,Measurement ID,Uploaded,Downloaded,SCC Version,Result" )
                
        with open (self.csv_path, 'a') as csvfile:
            for measurement in self.measurements.values():
                process_start = measurement.get("process_start", "N/A")
                
                try:
                    path = measurement["scc_netcdf_path"]
                    data_folder = os.path.dirname ( path )
                    data_file = os.path.basename ( path )
                except Exception as e:
                    logger.error ( str(e) )
                    data_folder = "N/A"
                    data_file = "N/A"
                    
                csvfile.write ("\n%s,%s,%s,%s,%s,%s,%s,\"%s\",%s" % (
                    measurement.get("process_start", "N/A"),
                    data_folder,
                    data_file,
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

datalog = SwapFile ()
logger, console = LoggerFactory.getLogger()