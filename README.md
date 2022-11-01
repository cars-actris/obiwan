# Obiwan
Obiwan is a Python package that ties in two popular libraries: [scc-access](https://pypi.org/project/scc-access/) and [atmospheric-lidar](https://pypi.org/project/atmospheric-lidar/).
This bundle of scripts is meant to provide a full processing chain from raw lidar data files, converting them to the required format and uploading the measurements to
[Single Calculus Chain](https://www.earlinet.org/index.php?id=281), providing the option to download the processing results.

Obiwan can be run manually, but it also provides several features that make it suitable for automatic periodic execution in order to fully automate the processing at the station.

### Features

- automatic search of raw lidar measurement files
- automatic split of continuous measurements based on time gaps and measurement lengths
- automatic upload and download withing the Single Calculus Chain
- detection of dark signal measurement files
- detection of QA/QC test files, configurable by the user
- processing resuming in case of interruption/error
- automatic detection of new measurements for continuous measuring lidar systems

## Installation

1. Install the latest version of [Python 3](https://www.python.org/downloads/). Make sure to check the ***Add Python 3.X to PATH***!
2. Run the installation script. For Windows run `setup.bat`, for Linux or Mac operating systems run `sh setup.sh`.

## Configuration

**Before running `obiwan`, initial configuration is mandatory**. The scripts will not work without a proper configuration. This section describes the availabile configuration parameters,
needed to properly run the processing scripts.

These available parameters can be divided in 3 categories:

1. General configuration - sample provided in `conf/obiwan.config.sample.yaml`
2. SCC connection configuration - sample provided in `conf/scc_access.config.sample.yaml`
3. Command line arguments - list availabile by running `obiwan --help`

### General parameter configuration

This configuration resides in a [YAML](https://en.wikipedia.org/wiki/YAML) file, which allows the user to set up quasi-static parameters.
Folder or file paths can be provided as absolute values (e.g.: `C:\Users\obiwan\conf\obiwan.config.yaml`) or as relative (e.g.: `conf/obiwan.config.yaml`).
Keep in mind that paths are relative to configuration file obiwan is using.

The options available in this configuration file affect the measurement detection, conversion and processing algorithm. A sample file is provided in `conf/obiwan.config.sample.yaml`. Available configuration parameters are:

```YAML
# Sets the path for the sample files used to determine the SCC System ID when uploading data to the Single Calculus Chain
scc_configurations_folder: data/Samples

# Path to the Licel lidar system extra parameters, needed for creating the NetCDF files required by Single Calculus Chain.
# You need to supply one parameter file for each SCC System ID you are using.
# Please check the atmospheric-lidar package documentation for more information on the required contents of these files.
system_netcdf_parameters:
    310: conf/system/rali_netcdf_parameters 2020.py
    312: conf/system/rali_netcdf_parameters 2020.py
    375: conf/system/ipral_netcdf_parameters_375-376-377-378_auto.py
    376: conf/system/ipral_netcdf_parameters_375-376-377-378_auto_v2.py

# This parameter will be used to identify real measurements. The algorithm will look for this string in the location field
# in the raw Licel files. In this sample, the instrument will use "Buchares" location:
measurement_location: Buchares

# This parameter will be used to identify dark files. The algorithm will look for this string in the location field
# in the raw Licel files. In this sample, dark measurements will use "Dark" location:
dark_location: Dark

# This folder will hold the converted NetCDF files, which can be uploaded to the Single Calculus Chain:
netcdf_out_folder: data/netcdf

# The directory where to download SCC products:
scc_output_dir: scc_output

# The HTTP username and password that is needed to access the SCC site:
scc_basic_credentials: ['sccuser', 'sccpassword']

# The username and password that is needed to log in to the SCC site:
scc_website_credentials: ['username', 'password']

# SCC base URL. Normally you shouldn't need to change this:
scc_base_url: https://scc.imaa.cnr.it/

# Number of retries in case of connection issues when trying to upload measurements to the Single Calculus Chain:
scc_maximum_upload_retries: 3

# Maximum accepted time gap (in seconds) between two raw data files. Two data files with a time gap below this value will be
# considered as being part of the same measuremnt. A time gap above this value will signal a pause between two different measurements:
maximum_measurement_gap: 600

# Minimum time length (in seconds) for a measurement to be taken into account for further processing. This option is useful for
# filtering incomplete measurements when the instrument was turned off.
minimum_measurement_length: 1800

# Maximum time length (in seconds) for a measurement. Continuous measurements will be split at this length. This option is useful for
# setting the maximum length of a measurement to be uploaded to the Single Calculus Chain (usually 1 hour, or 3600 seconds).

maximum_measurement_length: 3600

# Determines how the algorithm will split the measurements when building the NetCDF files. Three values are accepted:
# * -1 will split them based only on measurement length (see above)
# * 0 will try to center the measurements at fixed (xx:00) hours. First and last measurements from a set are being excepted from this rule (depending on when the measurement started/stopped).
# * 1 will try to center the measurements at half hours (xx:30). First and last measurements from a set are being excepted from this rule (depending on when the measurement started/stopped).
measurement_center_type: -1

# You can define test files lists using the test_TESTTNAME convention. Each item in the list
# corresponds to the location parameter written in the raw file header when the test is run.
# This will identify raw test files based on location information and copies them to the "tests" folder.
# Tests are considered valid and will be copied only if the entire list of tests is present.
test_Telecover:
    - 'Telecove'
    
test_Depolarisation:
    - '+45'
    - '-45'

# When using the --debug command line parameter, raw files will be copied to the specified folder
# in order to verify how the measurements were split before being converted to SCC NetCDF files:
measurements_debug_dir: data/measurement_debug
```

### Command line arguments

Command line arguments can be supplied at run time, either by typing them in the command line/terminal or inside a script file which you can run later.
These arguments vastly affect the operation mode of the software, so make sure you understand them. A list of all the accepted arguments can also be obtained
by running `obiwan --help` in the command line.

- `folder` (**Mandatory**) - The path to the data folder you want to process. The folder is scanned recursively, so any existing subfolders will also be included
in the search

- `--datalog` - Path to save a CSV log of all the processing done over time. Default: `datalog.csv`
- `--startdate` - Minimum start date for measurements to be processed. Measurements taken earlier will be ignored.
- `--enddate` - Maximum start date for measurements to be processed. Measurements taken later will be ignored.
- `--cfg` - General parameter configuration file path. Default: `conf/obiwan.config.yaml`
- `--verbose` or `-v` - Using this the software will output more detailed messages. Can be used twice for different verbosity levels.
- `--replace` or `-r` - If the measurement is already in the SCC database, obiwan will reupload and reprocess it.
- `--reprocess` or `-p` - If the measurement is already in the SCC database, obiwan will trigger reprocessing.
- `--download` or `-d` - Download SCC products after processing.
- `--wait` or `-w` - Wait for SCC to process measurement. If this flag is missing and processing is not finished, obiwan will not download the measurement.
- `--convert` or `-c` - Convert files to SCC NetCDF format without uploading or processing on the SCC.
- `--continuous` - Used for continuous measuring systems. This will determine the date of the last processed measurement and start from there, ignoring older measurements.
- `--resume` - When this flag is set, obiwan will try to resume past interrupted work if possible. Useful on unstable connections or if you don't want to lose data when stopping obiwan.
- `--test-files` - Copies any raw test files to tests folder.
- `debug` - Copies raw measurement files and resulting NetCDF files in the debug folder.

## Usage

After installation and configuration, you can run `obiwan` from the command line if Python was installed in the PATH environment variable.
Otherwise you can navigate to the `src` folder and run `python3 obiwan.py`, along with your chosen command line parameters. For example:

- `obiwan --convert --resume --test-files /mnt/data/lidar/2022`
- `python3 obiwan.py --convert --resume --test-files /mnt/data/lidar/2022`