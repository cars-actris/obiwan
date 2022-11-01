from obiwan.log import logger
from pathlib import Path

import os
import yaml

DEFAULT_CFG_FILE = "obiwan/obiwan.config.yaml"
DEFAULT_TESTS_FOLDER = "data/tests"

class Config:
    def __init__ ( self, file_path = None ):
        if file_path is None:
            fp = Config.compute_path ( DEFAULT_CFG_FILE )
        else:
            fp = os.path.abspath ( file_path )
            
        try:
            logger.debug ( f"Trying to read configuration file {fp}" )
            with open (fp, 'r') as yaml_file:
                try:
                    config = yaml.safe_load (yaml_file)
                except Exception as e:
                    logger.error(f"Could not parse YAML configuration file ({file_path})!")
                    logger.error(f"Error: {str(e)}")
                    raise e
        except Exception as e:
            logger.error ( f"Could not read configuration file {fp}" )
            logger.error (f"{str(e)}")
            raise e
                
        self.file_path = fp
        
        config_dir = os.path.abspath ( os.path.dirname ( self.file_path ) )
        
        # Folders:
        self.scc_configurations_folder = Config.compute_path ( config['scc_configurations_folder'], root_folder = config_dir )
        self.netcdf_out_dir = Config.compute_path ( config['netcdf_out_folder'], root_folder = config_dir )
        self.scc_output_dir = Config.compute_path ( config['scc_output_dir'], root_folder = config_dir )
        
        # Lidar system parameter files:
        self.system_netcdf_parameters = config.get('system_netcdf_parameters', {})
        
        # Some users might still have an older configuration file:
        if type(self.system_netcdf_parameters) is not dict:
            self.system_netcdf_parameters = {}
        
        for system_id, file_path in self.system_netcdf_parameters.items():
            self.system_netcdf_parameters[ system_id ] = Config.compute_path ( file_path, root_folder = config_dir )
        
        # SCC Configuration:
        self.scc_basic_credentials = tuple ( config['scc_basic_credentials'] )
        self.scc_website_credentials = tuple ( config['scc_website_credentials'] )
        self.scc_base_url = config['scc_base_url']
        self.maximum_upload_retry_count = config['scc_maximum_upload_retries']
        
        # Licel header location types:
        self.measurement_location = config['measurement_location']
        self.dark_location = config['dark_location']
        
        # Lidarchive parameters:
        self.max_acceptable_gap = config['maximum_measurement_gap']
        self.min_acceptable_length = config['minimum_measurement_length']
        self.max_acceptable_length = config['maximum_measurement_length']
        self.center_type = config['measurement_center_type']
        
        # Measurements debug:
        self.measurements_debug_dir = Config.compute_path ( config['measurements_debug_dir'], root_folder = config_dir )
        
        # Test folder:
        self.tests_dir = Config.compute_path (DEFAULT_TESTS_FOLDER, root_folder = config_dir)
        
        self.test_lists = {}
        
        for key in config.keys():
            if key.startswith("test_") and len(key) > 5:
                test_list_name = key[5:]
                self.test_lists[test_list_name] = config[key]
                
        self.makedirs()
                
    def makedirs ( self ):
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
    def compute_path ( path, root_folder = None ):
        if ( os.path.isabs ( path ) ):
            # If the user configured an absolute path
            # use that without any questions asked:
            return path
            
        root = root_folder
        if root is None:
            root = Path.home()
            
        # Otherwise, if a relative path was provided,
        # get path relative to the parent folder of this file:
        
        relpath = os.path.join ( root, path )
        abspath = os.path.abspath ( os.path.normpath ( relpath ) )
        
        return abspath