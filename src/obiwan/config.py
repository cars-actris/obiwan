from obiwan.log import logger

import os
import yaml

DEFAULT_CFG_FILE = "conf/obiwan.config.yaml"
DEFAULT_TESTS_FOLDER = "data/tests"

class Config:
    def __init__ ( self, file_path = None ):
        if file_path is None:
            fp = Config.compute_path ( DEFAULT_CFG_FILE )
            
        with open (fp) as yaml_file:
            try:
                config = yaml.safe_load (yaml_file)
            except Exception as e:
                logger.error(f"Could not parse YAML configuration file ({file_path})!")
                logger.error(f"Error: {str(e)}")
                sys.exit (1)
                
        self.file_path = fp
        
        # Folders:
        self.scc_configurations_folder = Config.compute_path ( config['scc_configurations_folder'] )
        self.netcdf_out_dir = Config.compute_path ( config['netcdf_out_folder'] )
        self.scc_output_dir = Config.compute_path ( config['scc_output_dir'] )
        
        # Lidar system parameter file:
        self.netcdf_parameters_path = Config.compute_path ( config['system_netcdf_parameters'] )
        
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
        self.measurements_debug_dir = Config.compute_path ( config['measurements_debug_dir'] )
        
        # Test folder:
        self.tests_dir = Config.compute_path (DEFAULT_TESTS_FOLDER)
        
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