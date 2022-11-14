# Obiwan
`obiwan` is a Python package that ties in two popular libraries: [scc-access](https://pypi.org/project/scc-access/) and [atmospheric-lidar](https://pypi.org/project/atmospheric-lidar/). This bundle of scripts is meant to provide a full processing chain from raw lidar data files, converting them to the required format and uploading the measurements to [Single Calculus Chain](https://www.earlinet.org/index.php?id=281), providing the option to download the processing results.

Obiwan can be run manually, but it also provides several features that make it suitable for automatic periodic execution in order to fully automate the processing at the station.

This short documentation aims to introduce the main configuration options, as well as to describe the core algorithms of `obiwan`. As such, it is structured in the following order:
1. Installation
2. Configuration
3. Usage
4. Algorithm details

### Features

- automatic search of raw lidar measurement files
- automatic split of continuous measurements based on time gaps and measurement lengths
- measurement set alignment to sharp hours (:00), or half hours (:30)
- automatic detection of SCC system IDs
- able to handle multiple different raw data file formats (in development)
- automatic upload and download within the Single Calculus Chain
- detection of dark signal measurement files
- detection of QA/QC test files, configurable by the user
- processing resuming in case of interruption/error
- automatic detection of new measurements for continuous measuring lidar systems

## Installation

1. Install the latest version of [Python 3](https://www.python.org/downloads/). Make sure to check the ***Add Python 3.X to PATH***!
2. Run the installation script. For Windows run `setup.bat`, for Linux or Mac operating systems run `sh setup.sh`.

## Configuration

**Before running `obiwan`, initial configuration is mandatory**. The scripts will not work without a proper configuration. This section describes the availabile configuration parameters, needed to properly run the processing scripts.

These available parameters can be divided in 3 categories:

1. **General configuration** - sample provided in `conf/obiwan.config.sample.yaml`
This configuration file is the basis for how obiwan will read and process lidar data files. More details about the structure of this configuration file is provided below.
2. **Command line arguments** - full list is available by running `obiwan --help`
The command line arguments determine the run-to-run behaviour of obiwan, such as which files will be processed, whether to upload/download to the SCC, etc.

### General parameter configuration

This configuration resides in a [YAML](https://en.wikipedia.org/wiki/YAML) file, which allows the user to set up quasi-static parameters. Folder or file paths can be provided as absolute values (e.g.: `C:\Users\obiwan\conf\system\rali_netcdf_parameters 2020.py`) or as relative (e.g.: `conf/system/rali_netcdf_parameters 2020.py`). Relative paths will be calculated starting from the folder where the configuration file is read from. For example, if the configuration file is stored in `C:\Users\obiwan`, the two paths exampled earlier will be equivalent.

The options available in this configuration file affect the measurement detection, conversion and processing algorithm. A sample file is provided in `conf/obiwan.config.sample.yaml`. Available configuration parameters are:

```YAML
# Sets the path for the sample files used to determine the SCC System ID when uploading data to the Single Calculus Chain:
scc_configurations_folder: data/Samples

# Path to the Licel lidar system extra parameters, needed for creating the NetCDF files required by Single Calculus Chain.
# You need to supply one parameter file for each SCC System ID you are using. You can specify different files for v1 and v2
# of the Licel raw file format (older and newer specification).
# Please check the atmospheric-lidar package documentation for more information on the required contents of these files.
licel_netcdf_parameters:
    310: conf/system/rali_netcdf_parameters 2020.py
    312: conf/system/rali_netcdf_parameters 2020.py
    375:
        v1: conf/system/ipral_netcdf_parameters_375-376-377-378_auto.py
        v2: conf/system/ipral_netcdf_parameters_375-376-377-378_auto_v2.py
    591:
        v1: conf/system/alpha_netcdf_parameters.py
        v2: conf/system/alpha_netcdf_parameters_v2.py
    610:
        v1: conf/system/alpha_netcdf_parameters.py
        v2: conf/system/alpha_netcdf_parameters_v2.py

# This parameter will be used to identify real measurements. The algorithm will look for these strings in the location field
# in the raw Licel files. In this sample, only "Buchares" and "SIRTA" measurements will be processed.
measurement_identifiers:
    - SIRTA
    - Buchares

# This parameter will be used to identify dark files. The algorithm will look for this string in the location field
# in the raw Licel files. In this sample, dark measurements will use "Dark" location:
dark_identifiers:
    - Dark
    - D

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

# Determines how the algorithm will split the measurements when building the NetCDF files. One of the following values is accepted:
# [ -1, 0, 1, 2, 3 ]
# 
# -1 -- will split them based only on measurement length. When the maximum length is reached, the set will be split.
# 0  -- will center the measurement sets at sharp (xx:00) hours. The rule will not be enforced to the first and last measurements in a set.
#       Any remaining data will be glued at the beginning or end of the set.
# 1  -- behaves like 0 but will not glue any data before or after the aligned segments.
# 2  -- will center the measurement sets at half (xx:30) hours. The rule will not be enforced to the first and last measurements in a set.
#       Any remaining data will be glued at the beginning or end of the set.
# 3  -- behaves like 2 but will not glue any data before or after the aligned segments.
measurement_alignment_type: 0

# You can define test files lists using the test_TESTTNAME convention. Each item in the list
# corresponds to the location parameter written in the raw file header when the test is run.
# This will identify raw test files based on location information and copies them to the "tests" folder.
# Tests are considered valid and will be copied only if the entire list of
# tests is present.
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

Command line arguments can be supplied at run time, either by typing them in the command line/terminal or inside a script file which you can run later. These arguments vastly affect the operation mode of the software, so make sure you understand them. A full list of accepted command line arguments can also be obtained by running `obiwan --help` in the command line.

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

## Details on ...
### ...measurement sets:
`obiwan` was initially written only with Licel data acquisition systems in mind. This type of system continuously writes data files, usually between 30 seconds and 10 minutes in length. In order to identify a continuous atmosphere observation, the algorithm reads the start and end time of each data file, and puts them into so-called measurement sets. A measurement set represents a list of data files belonging to the same, uninterrupted measurement.

For checking whether two files belong to the measurement set, `obiwan` looks at a few different parameters:
- system channels (all files in a measurement set need to have the exact same channels)
- time gap between the two files (maximum acceptable value can be set in the configuration file)
- measurement location information (as read from the raw data file)
- subfolder (all data files belonging to the same measurement set should be stored in the same folder/subfolder)

Once all the files are put into continuous measurement sets, these sets will be split into measurements according to the `maximum_measurement_length` set in the configuration file. Measurements shorter than `minimum_measurement_length` will be discarded, unless the specified `measurement_alignment_type` allows  these short measurements to be glued onto the larger ones (Please check the comments in the configuration file for more information on this behaviour).

### ...system IDs:
Every NetCDF data file that is uploaded on the Single Calculus Chain needs to contain information about the system configuration which produced the data. That information takes form of a numerical ID, which will link to various parameters in the SCC database. Whilst it is possible to try and read the available configurations via the SCC website, the process would be slow and susceptible to errors.

This is why `obiwan` uses sample files to determine the SCC system ID. These files are simply raw data files produced by the corresponding system configuration (usually one for daytime measurements, and one for nighttime measurements). The files should be named analogue to the (numerical) ID of that configuration in the SCC database. Optionally, filenames can also have an extension in the rare case where a system can produce two different types of raw files, for the same configuration ID. As an example, both of the following are valid sample file names for a system with ID `312`:
- `312`
- `312.v2`

Information about the lidar channels from the raw data files will be compared against these sample files in order to determine the system ID. If `obiwan` is not able to determine the system ID, the conversion from raw files to SCC NetCDF format will not be possible and an error message will be shown.

### ...log files:
The software will write three different files in order to assist with keeping track of data processing and, if need be, debugging problems:
- **CSV data log** for keeping track of which files were processed, and the processing results. The default file name is `datalog.csv`, inside the folder where `obiwan` was run from, but this can be changed with the `--datalog` command line argument. Once the application finishes a process run, it will append information about the processed measurement sets to this file.
- **application log** for checking application messages from the last run. This is useful for debugging in case you run into any issues while running `obiwan`. This file will be written as `obiwan.log` inside the folder where `obiwan` was run from.
- **swap file** for keeping track of application state and processing progress. This is the file that `obiwan` uses to resume work if it was interrupted or it crashed. This file gets updated anytime the internal processing state of `obiwan` changes and will always be stored inside the `netcdf_out_folder` specified in the configuration file. This is a binary file which cannot be read with a text editor.

### ...file sets debugging
The `--debug` command line argument can be used to debug the algorithm which identifies measurement sets and splits them into measurements. When using this flag, all the raw data files and the resulting SCC NetCDF files (if any) will be copied to the specified `measurements_debug_dir` from the configuration file. **Beware**, this option **will copy many large files, which take up significant space on the disk**. Please use this option only when trying to debug `obiwan`.