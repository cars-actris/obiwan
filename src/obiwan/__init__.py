__version__ = "0.1.0"

from obiwan.config import Config
from obiwan.data.system_index import SystemIndex
from obiwan.log import logger, Datalog, set_log_level
from obiwan.scc import OwScc

from pathlib import Path

import argparse
import datetime
import os
import sys
import traceback

class ObiwanApplication:
    """
    Class that defines the running obiwan application. It provides useful mechanisms to access
    the running configuration parameters, which will directly affect the behaviour of various
    application functions.
    
    Attributes:
        logger (:obj:`logging.logger`): Logger object used to output messages.
        args (:obj:`argparse.Namespace`): The parsed command line arguments.
        config (:obj:`Config`): Running configuration for this obiwan application.
        datalog (:obj:`Datalog`): The central datalog for storing processing state.
        system_index (:obj:`SystemIndex`): The system index of known lidar systems, used
            to retrieve SCC system IDs when converting raw measurements to SCC NetCDF
            input files.
        scc (:obj:`OwScc`): Helper object used to interact with the SCC API or website.
    """
    DEFAULT_CONFIGURATION_FILE = "obiwan/obiwan.config.yaml"
    SWAP_FILE_NAME = "obiwan.swp"
    
    def __init__(self):
        # Initialize logger
        self.logger = logger
        
        self.args = self.parse_args()
        
        # Set appropriate log level
        set_log_level ( self.args.verbose, self.logger )
            
        if self.args.folder is None:
            self.logger.error ( "You must specify the data folder. Exiting..." )
            parser.print_help()
            sys.exit (1)
        
        # Read configuration file
        try:
            configuration_file_path = self.args.cfg
            if configuration_file_path is None:
                default_configuration_file = os.path.join ( Path.home(), ObiwanApplication.DEFAULT_CONFIGURATION_FILE )
                configuration_file_path = os.path.abspath ( default_configuration_file )
                
            self.config = Config ( configuration_file_path )
        except Exception as e:
            self.logger.error ( "Error loading configuration file. Exiting..." )
            traceback.print_exc()
            sys.exit (1)
            
        # Initialize internal obiwan datalog for tracking processing state
        datalog_path = os.path.join ( self.config.netcdf_out_dir, ObiwanApplication.SWAP_FILE_NAME )
        self.datalog = Datalog()
        
        self.datalog.set_file_path ( datalog_path )
        self.datalog.set_csv_path ( self.args.datalog )
        
        # Initialize lidar system collection
        self.system_index = SystemIndex()
        self.system_index.ReadFolder (self.config.scc_configurations_folder)
        
        # Initialize SCC client
        self.scc = OwScc()
        self.scc.Initialize(
            self.config.scc_basic_credentials,
            self.config.scc_output_dir,
            self.config.scc_base_url,
            self.config.scc_website_credentials
        )
        
        if not self.args.convert:
            self.scc.Login()
            
    def parse_args(self) -> argparse.Namespace:
        """
        Parse command line arguments.
        
        Note:
            Some arguments, such as dates, get minimal processing work done.
        
        Returns:
            :obj:`argparse.Namespace` with the parsed arguments.
        """
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
        
        if args.startdate is None:
            args.startdate = None
        else:
            args.startdate = datetime.datetime.strptime( args.startdate, '%Y%m%d%H%M%S' )
            
        if args.enddate is None:
            args.enddate = None
        else:
            args.enddate = datetime.datetime.strptime( args.enddate, '%Y%m%d%H%M%S' )
            
        return args
        
obiwan = ObiwanApplication()