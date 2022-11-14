from obiwan.log import logger

import os

from scc_access import scc_access
from netCDF4 import Dataset

def SetSCCConfig ( basic_credentials, output_dir, scc_base_url, website_credentials = None ):
    global scc
    scc.Initialize( basic_credentials, output_dir, scc_base_url, website_credentials )

class OwScc:
    def __init__ ( self ):
        self.client = None
        self.basic_credentials = None
        self.output_dir = None
        self.client_base_url = None
        self.website_credentials = None
        self.logged_in = False
    
    def Initialize ( self, basic_credentials, output_dir, scc_base_url, website_credentials ):
        self.basic_credentials = basic_credentials
        self.output_dir = os.path.normpath ( output_dir )
        self.client_base_url = scc_base_url
        self.website_credentials = website_credentials
        
        self.client = scc_access.SCC(self.basic_credentials, self.output_dir, self.client_base_url)
        
    def Login ( self ):
        self.client.login(self.website_credentials)
        self.logged_in = True
    
    @staticmethod
    def GetPreProcessorVersion ( file ):
        '''
        Reads the SCC preprocessor version used to process a measurement.
        
        Parameters
        ----------
        file : str
            Name or path of the NetCDF file downloaded from the SCC (SCC preprocessed file).
        
        Return values
        -------------
        String representing the SCC preprocessor version.
        '''
        dataset = Dataset ( file )
        pp_version = dataset.SCCPreprocessingVersion
        dataset.close ()
        
        return pp_version
    
    @staticmethod
    def GetELPP_ELDAVersion ( file ):
        '''
        Reads the SCC processing software version used to process a measurement.
        
        Parameters
        ----------
        file : str
            Name or path of the NetCDF file downloaded from the SCC (SCC processed file).    
        Return values
        -------------
        Two strings, representing the ELPP version and the ELDA version.
        '''
        dataset = Dataset ( file )
        software_version = dataset.__AnalysisSoftwareVersion
        dataset.close ()
        
        elpp_regex = 'ELPP version: ([^;]*);'
        elda_regex = 'ELDA version: (.*)$'
        
        scc_elpp_version = re.findall ( elpp_regex, software_version )[0]
        scc_elda_version = re.findall ( elda_regex, software_version )[0]
        
        return scc_elpp_version, scc_elda_version

    @staticmethod
    def GetSCCVersion ( download_folder, measurement_id ):
        '''
        Retrieves version information about the SCC chain used to process a given measurement.
        
        Parameters
        ----------
        download_folder : str
            Path to the download folder as passed to the scc-access module.
        measurement_id : str
            ID of the measurement used to retrieve the information.
        
        Return values
        -------------
        String representing the SCC version and SCC processor versions description.
        '''
        # Try to read SCC version information from HiRelPP. If that fails, fallback to ELPP.
        elpp_folder = os.path.join ( download_folder, measurement_id, 'elpp' )
        hirelpp_folder = os.path.join ( download_folder, measurement_id, 'hirelpp' )
        
        folders_to_scan = [ hirelpp_folder, elpp_folder ]
        
        scc_version = None
        
        for folder in folders_to_scan:
            try:
                file = os.path.join ( folder, os.listdir ( folder )[0] )
                
                with Dataset( file ) as dataset:
                    scc_version = dataset.scc_version_description
                    
                break
            except:
                # HiRelPP products might not exist. We can try checking ELPP files next.
                continue
        
        if scc_version is None:
            raise IOError ( "Could not find any preprocessor files." )
        
        return scc_version
        
    def TryUpload ( self, filename, system_id, replace ):
        # try:
        upload = self.client.upload_file ( filename, system_id, replace, False )
        # except Exception, e:
            # measurement_id = os.path.splitext ( os.path.basename(filename) ) [0]
            # logger.warning ( "[%s] SCC upload error: %s" % (measurement_id, str(e)))        
            # upload = False
            
        if upload != False:
            upload = True
            
        return upload

    def UploadMeasurement ( self, filename, system_id, max_retry_count, replace ):
        '''
        Upload a NetCDF file to the SCC and process it.
        
        Parameters
        ----------
        filename : str
            NetCDF file to upload to SCC.
        system_id : int
            System ID as set up in the SCC web interface.
        scc : SCC
            SCC object used for interacting with the SCC API.
        max_retry_count : int
            Maximum number of retries in case of a failed upload.
        '''
        measurement_id = os.path.splitext ( os.path.basename(filename) ) [0]
        
        retry_count = 0
        
        # Send the file to SCC and start the processing chain:
        upload = self.TryUpload (filename, system_id, replace)
        
        # If the upload failed, retry for a given number of times.
        while upload == False and retry_count < max_retry_count:
            retry_count += 1
            logger.warning ( "Upload to SCC failed. Retrying (%d/%d)." % (retry_count, max_retry_count), extra={'scope': measurement_id} )
            
            upload = self.TryUpload (filename, system_id, replace)
            
        return upload
        
    def DownloadProducts ( self, measurements ):
        '''
        Download products for a given set of measurements.
        
        Parameters
        ----------
        measurements : list
            list of measurement names to download
        scc : SCC
            SCC connection to use the SCC API    
        '''
        
        logger.info ( "Downloading SCC products" )
        
        for measurement_id in measurements:
            CURRENT_MEASUREMENT = measurement_id
            
            logger.debug ( "Waiting for processing to finish and downloading files...", extra={'scope': measurement_id} )
            
            result = self.client.monitor_processing ( measurement_id, exit_if_missing = False )
            
            if result is not None:
                logger.debug ( "Processing finished", extra={'scope': measurement_id} )
                
                try:
                    scc_version = GetSCCVersion ( self.client.output_dir, measurement_id )
                except Exception as e:
                    if result.elpp != 127:
                        logger.error ( "No SCC products found", extra={'scope': measurement_id} )
                    else:
                        logger.error ( "Unknown error in SCC products", extra={'scope': measurement_id} )
                    scc_version = "Unknown SCC Version! Check preprocessed NetCDF files."
                    continue
                    
                logger.info ( scc_version, extra={'scope': measurement_id} )
            else:
                logger.error ( "Download failed", extra={'scope': measurement_id} )