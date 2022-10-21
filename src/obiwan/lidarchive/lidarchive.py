import fileinput

from atmospheric_lidar import licel
from atmospheric_lidar.licel import LicelLidarMeasurement
from datetime import datetime, timedelta
import glob
import os
import shutil

licel_file_header_format = ['Filename',
                            'StartDate StartTime EndDate EndTime Altitude Longtitude Latitude ZenithAngle',
                            # Appart from Site that is read manually
                            'LS1 Rate1 LS2 Rate2 DataSets', ]

class Lidarchive:
    '''
    Class used to read all files from a licel lidar data folder. It crawls through all the subfolders
    and identifies all measurements which are not dark measurements. It can be used to retrieve all
    available measurements in a chronological order or to retrieve all continuous measurement sets with
    a user defineable accepted time gap.
    '''

    class MeasurementChannel:
        '''
        Helper class used to describe a lidar system channel.
        '''

        def __init__(self, licel_channel):
            self.name = licel_channel.name
            self.resolution = licel_channel.resolution
            self.wavelength = licel_channel.wavelength
            self.laser_used = licel_channel.laser_used
            self.adcbits = licel_channel.adcbits
            self.analog = licel_channel.is_analog
            self.active = licel_channel.active

        def Equals(self, channel):
            '''
            Compares two lidar channels.

            Parameters
            ----------
            channel : Channel
                The channel used for comparison.
            '''
            if (self.name == channel.name and
                    self.resolution == channel.resolution and
                    self.laser_used == channel.laser_used and
                    self.adcbits == channel.adcbits and
                    self.analog == channel.analog and
                    self.active == channel.active):
                return True

            return False

    class MeasurementFile:
        def __init__(self, path, start_datetime, end_datetime, site, type):
            self.path = path
            self.start_datetime = start_datetime
            self.end_datetime = end_datetime
            self.site = site
            self.type = type

            licel_measurement = LicelLidarMeasurement([path])

            self.channels = []

            for channel_name, channel in licel_measurement.channels.items():
                self.channels.append(Lidarchive.MeasurementChannel(channel))

        def IsDark(self, dark_location = "Dark"):
            '''
            Checks if a given measurement represents a dark measurement.

            Return value
            ------------
            True if the measurement is a dark measurement, False otherwise.
            '''
            if self.site == dark_location:
                return True

            return False

        def Path(self):
            return self.path
            
        def Filename(self):
            return os.path.basename(self.path)

        def StartDateTime(self):
            return self.start_datetime

        def EndDateTime(self):
            return self.end_datetime

        def Site(self):
            return self.site

        def Type(self):
            return self.type

        def HasSameChannelsAs(self, measurement):
            '''
            Compares two lidar systems by comparing their channels.

            Parameters
            ----------
            system : System
                The system used for comparison.
            '''
            # Make a copy of the other system's channel list:
            other_channels = measurement.channels[:]

            if len(self.channels) != len(other_channels):
                return False
            for channel in self.channels:
                found = False
                for other_channel in other_channels:
                    if channel.Equals(other_channel):
                        found = True
                        other_channels.remove(other_channel)
                        break

                if found == False:
                    return False

            if len(other_channels) > 0:
                return False

            return True

    class Measurement:
        def __init__(self, dark, data, number):
            self.dark_files = dark
            self.data_files = data
            self.number = number

        def DarkFiles(self):
            return self.dark_files

        def DataFiles(self):
            return self.data_files

        def Number(self):
            return self.number

        def NumberAsString(self):
            return "%04d" % (self.number)
            
        def Id(self):
            try:
                date = self.DataFiles()[0].StartDateTime().strftime("%Y%m%d")
                
                return f"{date}_{self.NumberAsString()}"
            except:
                return "UNKNOWN_MEASUREMENT"

    def __init__(self, **kwargs):
        '''
        Constructs a Lidarchive object.
        '''
        self.folder = None
        self.measurements = []
        self.accepted_gap = 0
        self.accepted_min_length = 0
        self.accepted_max_length = 0
        self.accepted_center_type = -1
        self.continuousMeasurements = []
        self.tests = kwargs.get("tests", {})
        self.dark_location = kwargs.get("dark_location", "Dark")
        self.measurement_location = kwargs.get("measurement_location", "N/A")

    def SetFolder(self, folder):
        '''
        Indicate the folder which contains the licel files.

        Parameters
        ----------
        folder : str
            The path to the folder.
        '''
        self.folder = os.path.abspath(folder)
        self.measurements = []

    def MeasurementWasSent(self, last_end, min_length, max_length):
        '''
        Check if current received measurements are meeting the length requirements

        Parameters
        -----------

        last_end: str
            the end time of last sent measurement

        min_length: int
            Minimum accepted length, measured in seconds, of a set of measurements

        max_length: int
            Maximum accepted length, measured in seconds, of a set of measurements

        Returned values
        -----------------

        Boolean which tells if the current set of measurements was sent

        last_end: datetime
            Updated variable of the last set of measurements sent

        '''

        index = 0
        for measurement in self.measurements:
            if measurement.IsDark( self.dark_location ):
                index += 1
        #ignore dark files

        current_end = self.measurements[-1].EndDateTime()
        current_start = self.measurements[index].StartDateTime()


        if str(current_end) == last_end:
            return (True, last_end)

        last_end = current_end

        if (current_end - current_start).total_seconds() >= max_length + min_length:
            return (True, last_end)

        return (False, last_end)

    def FilterByGap(self, measurements, max_gap, same_location=True):
        '''
        Retrieve measurements split into groups based on the
        maximum allowed gap between two consecutive measurements.

        Parameters
        ----------
        measurements : list of Measurement
            List of measurements to filter.
        max_gap : int
            Maximum accepted time gap, measured in seconds, between to consecutive measurements.
        same_location : boolean
            Indicate if all measurements inside a group should have the same location information.

        Returned values
        ---------------
        A list containing zero or more lists of Measurement objects, split accordingly
        to the maximum gap criteria.
        '''
        if len(measurements) < 1:
            return []

        cset = [measurements[0]]
        last_measurement = measurements[0]
        distinct_sets = []

        for measurement_index in range(1, len(measurements)):
            previous_end = measurements[measurement_index - 1].EndDateTime()
            current_start = measurements[measurement_index].StartDateTime()

            gap_trigger = ((current_start - previous_end).total_seconds() > max_gap) or (
                        measurements[measurement_index].HasSameChannelsAs(last_measurement) == False)

            location_trigger = False
            if same_location:
                if measurements[measurement_index - 1].Site() != measurements[measurement_index].Site():
                    location_trigger = True

            if gap_trigger == True or location_trigger == True:
                distinct_sets.append(cset)

                cset = []
                last_measurement = measurements[measurement_index]

            cset.append(measurements[measurement_index])

        if len(cset) > 0:
            distinct_sets.append(cset)

        return distinct_sets


    def FilterByTime(self, measurements, min_length, max_length, center_type):
        '''
        Retrieve sets of measurements split into groups based on minimum and maximum allowed
        length of the set.

        Parameters
        ----------------
        measurements: list of measurements
            A list of one continuous measurement

        min_length: int
            Minimum accepted length, measured in seconds, of a set of measurements

        max_length: int
            Maximum accepted length, measured in seconds, of a set of measurements


        Returned value
        ----------------
         A list containing lists of measurements
        '''

        remaining_hours = int(max_length / 3600)
        
        '''
        with open(os.path.abspath(time_parameter_file), "r") as f:
            last_end = f.readlines()[1][:-1]
        if measurements:
            was_sent = self.MeasurementWasSent(last_end, min_length, max_length)[0]
        if not was_sent:
            return []
        '''

        if len(measurements) < 1:
            return []

        segments = []
        segment = [measurements[0]]


        if measurements[0].EndDateTime().minute - measurements[0].StartDateTime().minute < 0 and center_type == 1:
            remaining_hours -= 1
            if remaining_hours == 0:
                segments.append(segment)
                segment = []
                remaining_hours = int(max_length / 3600)

        if measurements[0].StartDateTime().minute <= 30 and measurements[0].EndDateTime().minute >= 30 and center_type == 0:
            remaining_hours -= 1
            if remaining_hours == 0:
                segments.append(segment)
                segment = []
                remaining_hours = int(max_length / 3600)

        for i in range(1, len(measurements)):
            segment.append(measurements[i])
            if measurements[i].EndDateTime().minute - measurements[i].StartDateTime().minute < 0 and center_type == 1:
                remaining_hours -= 1
                if remaining_hours == 0:
                    segments.append(segment)
                    segment = []
                    remaining_hours = int(max_length / 3600)
                continue
            if measurements[i].StartDateTime().minute - measurements[i - 1].EndDateTime().minute < 0 and center_type == 1:
                remaining_hours -= 1
                if remaining_hours == 0:
                    segments.append(segment)
                    segment = []
                    remaining_hours = int(max_length / 3600)
                continue
            if measurements[i].StartDateTime().minute < 30 and measurements[i].EndDateTime().minute >= 30 and center_type == 0:
                remaining_hours -= 1
                if remaining_hours == 0:
                    segments.append(segment)
                    segment = []
                    remaining_hours = int(max_length / 3600)
                continue
            if measurements[i - 1].EndDateTime().minute < 30 and measurements[i].StartDateTime().minute >= 30 and center_type == 0:
                remaining_hours -= 1
                if remaining_hours == 0:
                    segments.append(segment)
                    segment = []
                    remaining_hours = int(max_length / 3600)
                continue

        if len(segment) > 0:
            segments.append(segment)

        if len(segments) == 0:
            return segments
        if (segments[0][-1].EndDateTime() - segments[0][0].StartDateTime()).total_seconds() < min_length and len(segments) > 1:
            segments[0].extend(segments[1])
            segments.remove(segments[1])

        if (segments[-1][-1].EndDateTime() - segments[-1][0].StartDateTime()).total_seconds() < min_length and len(segments) > 1:
            segments[-2].extend(segments[-1])
            segments.remove(segments[-1])
        return segments

    def FilterByLength(self, measurements, max_length, min_length):
        '''
        Retrieve measurements split into groups based on the
        minimum and maximum allowed length for a continuous measurement.

        Parameters
        ----------
        max_length : int
            Maximum accepted time length of a given measurement. If there are continuous measurements
            longer than this value, they will be split accordingly.
        min_length : int
            Minimum accepted time length of a given measurement. If the last part of the measurement
            would be shorter than this value, this part of the measurement will be glued to the
            previous one.

        Returned values
        ---------------
        A list containing zero or more lists of Measurement objects, split accordingly
        to the length criteria.
        '''
        if len(measurements) < 1:
            return []

        last_end = measurements[-1].EndDateTime()
        segment_start = measurements[0].StartDateTime()
        segments = []
        segment = [measurements[0]]

        for index in range(1, len(measurements)):
            current_start = measurements[index].StartDateTime()

            if (last_end - current_start).total_seconds() < min_length:
                segment.extend(measurements[index:])
                segments.append(segment)
                break

            if (current_start - segment_start).total_seconds() > max_length:
                segments.append(segment)
                segment = [measurements[index]]
                segment_start = measurements[index].StartDateTime()
                continue

            segment.append(measurements[index])

        return segments
        
    def CopyTestFiles ( self, out_folder, date_format = "%Y-%m-%d-%H-%M", strict = True ):
        '''
        Copies test files to the specified folder. It will split them into subfolders based on test date.

        Parameters
        ----------
        out_folder : string
            Path to the folder where we want to copy the files. If it does not exist, it will be created.
        date_format : string
            Date format used to create subfolders for each of the tests.
        strict : Boolean
            If using strict mode, all test files must be present or else the test will be ignored.

        Returned values
        ---------------
        True if the function succeeded, False otherwise.
        '''
        if len ( self.measurements ) < 1:
            return False
            
        potential_tests = dict([ (test_name, []) for test_name in self.tests.keys() ])
        valid_tests = []
            
        # Run through all measurements to see if any test was done:
        for measurement in self.measurements:
            for test_name, test_types in self.tests.items():
                if measurement.Site() in test_types:
                    # Measurement could be part of this test:
                    potential_tests[ test_name ].append (measurement)
                else:
                    # This measurement is not part of the test, so let's see
                    # if the test has completed until now:
                    test_files = potential_tests.get( test_name, [] )
                    potential_tests[ test_name ] = []
                    
                    if Lidarchive.CheckTest ( test_types, test_files, strict ):
                        subfolder_name = test_files[0].StartDateTime().strftime(date_format)
                        test_subfolder = os.path.join ( out_folder, test_name, subfolder_name )
                        
                        if not os.path.isdir ( test_subfolder ):
                            os.makedirs ( test_subfolder )
                        
                        for file in test_files:
                            shutil.copy2(file.Path(), test_subfolder)
            
    @staticmethod
    def CheckTest ( test_types, test_files, strict = True ):
        # No need to check any further:
        if len(test_files) < len(test_types) and strict:
            return False
            
        # If we're running in non-strict mode we basically want
        # at least one file. If we get that, test is considered valid.
        if len(test_files) > 0 and not strict:
            return True
            
        # Check all required types of files are present:
        test_valid = True
        for test_type in test_types:
            if test_type not in [ file.Site() for file in test_files ]:
                # Test type was not found. Can stop looking.
                test_valid = False
                break
                
        return test_valid

    def FindDarkFiles(self, measurement_date):


        '''
        Retrieve all dark measurements within given time stamps.

        Parameters
        -----------------
        measurement_date: datetime object
                    The exact time the measurement begins

        Returned value
        -----------------
        A list of all the dark measurements found between the time parameters.
        '''

        start_date = measurement_date - timedelta(days=1)
        end_date = measurement_date + timedelta(days=1)

        dark_measurements = []
        # Walk the folder tree:
        for root, dirs, files, in os.walk(self.folder):
            for file in files:
                path = os.path.join(root, file)

                try:
                    date = self.DateFromFilename(file)
                except Exception:
                    # Date could not be determined from the filename
                    # as this is most likely not a raw licel file!
                    continue

                if date == None:
                    continue
                # Only read the files that are between specified dates:
                good_file = False
                if start_date == None:
                    if end_date == None:
                        good_file = True
                    elif date <= end_date:
                        good_file = True
                elif end_date == None:
                    if date >= start_date:
                        good_file = True
                elif date >= start_date and date <= end_date:
                    good_file = True

                if good_file == True:
                    try:
                        info = self.ReadInfoFromHeader(path)
                        measurement = Lidarchive.MeasurementFile(
                            path=path,
                            start_datetime=info['StartDateTime'],
                            end_datetime=info['StopDateTime'],
                            site=info['Site'],
                            type=''
                        )

                        if measurement.IsDark( self.dark_location ):
                            dark_measurements.append(measurement)
                    except Exception as e:
                        pass

        return dark_measurements

    def IdentifyDarkFile(self, segment, max_gap):

        '''
        Find the dark measurement which is assign to the given measurement.

        Parameters:
        --------------
        segment: list of measurements
                The measurements set which needs a dark file attached

        Returned value
        --------------
        The dark measurement coresponding to the set

        '''


        dark_measurements = self.FindDarkFiles(segment[0].StartDateTime())
        matching_dark_measurements = []
        for dark_m in dark_measurements:
            if dark_m.HasSameChannelsAs(segment[0]):
                matching_dark_measurements.append(dark_m)

        if len(matching_dark_measurements) < 1:
            return []

        if len(matching_dark_measurements) == 1:
            return matching_dark_measurements

        dark_segments = self.FilterByGap(matching_dark_measurements, max_gap, same_location=True)

        # dark measurement is before the segment
        if dark_segments[0][-1].EndDateTime() < segment[0].StartDateTime():
            smallest_time = segment[0].StartDateTime() - dark_segments[0][-1].EndDateTime()

        # dark measurement is after the segment
        elif dark_segments[0][0].StartDateTime() > segment[-1].EndDateTime():
            smallest_time = dark_segments[0][0].StartDateTime() - segment[-1].EndDateTime()

        nearest_dark = dark_segments[0]

        for dark_segment in dark_segments[1:]:
            dark_start = dark_segment[0].StartDateTime()
            dark_end = dark_segment[-1].EndDateTime()

            segment_start = segment[0].StartDateTime()
            segment_end = segment[-1].EndDateTime()

            if dark_end < segment_start:
                smallest_time = segment_start - dark_end if segment_start - dark_end < smallest_time else smallest_time
                nearest_dark = dark_segment if segment_start - dark_end < smallest_time else nearest_dark
            if segment_end < dark_start:
                smallest_time = dark_start - segment_end if dark_start - segment_end < smallest_time else smallest_time
                nearest_dark = dark_segment if dark_start - segment_end < smallest_time else nearest_dark


        return nearest_dark

    def MergeDarkAndMeas(self, segments, dark_file):
        '''
        Retrieve all measurements with dark file attached

        Parameteres
        --------------
        segments: list of measurements
            The measurement set

        dark_file: list of measurements
            The dark measurement

        Returned value
        --------------
            A dictionary containing the dark measurement as the key and the measurement set
        as the value
        '''

        merged = {}
        merged[dark_file] = []
        for segment in segments:
            merged[dark_file].append(segment)
        return merged

    def ContinuousMeasurements(self, max_gap=300, min_length = 1800, max_length=3600, center_type = 0):
        '''
        Retrieve the continuous measurement sets with a chosen maximum acceptable time gap
        between consecutive measurements.

        Parameters
        ----------
        max_gap : int
            Maximum accepted time gap, measured in seconds, between to consecutive measurements.
            The default is set to 15s.

        Returned value
        --------------
        A list with the continuous measurement sets. Each set is in itself a list of dictionaries
        describing each measurement in the set.
        '''
        if len(self.continuousMeasurements) == 0:
            self.ComputeContinuousMeasurements(max_gap, min_length, max_length, center_type)

        if self.accepted_gap != max_gap:
            self.ComputeContinuousMeasurements(max_gap, min_length, max_length, center_type)

        if self.accepted_min_length != min_length:
            self.ComputeContinuousMeasurements(max_gap, min_length, max_length, center_type)

        if self.accepted_max_length != max_length:
            self.ComputeContinuousMeasurements(max_gap, min_length, max_length, center_type)

        if self.accepted_center_type != center_type:
            self.ComputeContinuousMeasurements(max_gap, min_length, max_length, center_type)

        return self.continuousMeasurements
        
    def ContinuousDarkMeasurements(self, max_gap):
        dark_measurements = [ m for m in self.measurements if m.IsDark ( self.dark_location ) ]
        
        dark_segments = self.FilterByGap ( dark_measurements, max_gap, same_location = False )
        
        return dark_segments
        
    def ContinuousDataMeasurements(self, max_gap):
        data_measurements = [ m for m in self.measurements if not m.IsDark ( self.dark_location ) and m.Site() == self.measurement_location ]
        
        data_segments = self.FilterByGap ( data_measurements, max_gap, same_location = False )
        
        return data_segments
        
    @staticmethod
    def ClosestDarkSegment ( data_segment, dark_segments ):
        data_start = data_segment[0].StartDateTime()
        data_end = data_segment[0].EndDateTime()
        
        closest = None
        best_dark_segment = None
        
        for index, dark_segment in enumerate(dark_segments):
            dark_start = data_segment[0].StartDateTime()
            dark_end = data_end = data_segment[0].EndDateTime()
            
            if dark_end < data_start:
                # Dark measurements were taken before data measurements.
                time_gap = data_start - dark_end
            elif dark_start > data_end:
                # Dark measurements were taken after data measurements.
                time_gap = dark_start - data_end
            else:
                # Dark measurements and data measurements overlap so set
                # the time gap to zero.
                #
                # We can also safely assume no other dark segment will come
                # close to this performance. :P
                time_gap = timedelta(seconds=0)
                return dark_segment
                
            if closest is None:
                closest = time_gap
                best_dark_segment = index
            elif time_gap < closest:
                closest = time_gap
                best_dark_segment = index
                
        if best_dark_segment is None:
            return []
            
        return dark_segments[index]

    def ComputeContinuousMeasurements(self, max_gap, min_length, max_length, center_type):
        '''
        Find all continuous measurement sets in the files identified after calling
        ReadFolder ().

        Parameters
        ----------
        max_gap : int
            Maximum accepted time gap, measured in seconds, between to consecutive measurements.
            The default is set to 15s.
        max_length : int
            Maximum accepted time length of a given measurement. If there are continuous measurements
            longer than this value, they will be split accordingly.
        min_length : int
            Minimum accepted time length of a given measurement. If the last part of the measurement
            would be shorter than this value, this part of the measurement will be glued to the
            previous one.
        '''
        self.continuousMeasurements = []

        if len(self.measurements) < 1:
            self.accepted_gap = max_gap
            self.accepted_min_length = min_length
            self.accepted_max_length = max_length
            self.accepted_center_type = center_type
            return
            
        gapped_dark_segments = self.ContinuousDarkMeasurements ( max_gap )
        gapped_data_segments = self.ContinuousDataMeasurements ( max_gap )

        measurement_number = 0
        last_start = None

        for continuous in gapped_data_segments:
            # First file in continuous should be a dark file:
            #segments = self.FilterByLength(continuous, max_length, min_length)
            if center_type != -1:
                segments = self.FilterByTime(continuous, min_length, max_length, center_type) #, time_parameter_file)
            else:
                segments = self.FilterByLength(continuous, max_length, min_length)
                
            for segment in segments:
                # This is already filtered
                real_measurements = segment
                # Need to find closest continuous dark segment:
                dark_measurements = Lidarchive.ClosestDarkSegment(data_segment = segment, dark_segments = gapped_dark_segments)

                # Apply measurement number if necessary:
                if last_start != None:
                    if segment[0].StartDateTime().date() != last_start.date():
                        measurement_number = 0
                    else:
                        measurement_number += 1
                else:
                    measurement_number = 0

                last_start = segment[0].StartDateTime()

                # Do not add last segment if new data files might appear just in case it's a recent dataset:
                if (datetime.now() - segment[-1].EndDateTime()).total_seconds() >= max_gap:
                    self.continuousMeasurements.append(Lidarchive.Measurement(
                        dark=dark_measurements,
                        data=real_measurements,
                        number=measurement_number
                    ))

        self.accepted_gap = max_gap
        self.accepted_min_length = min_length
        self.accepted_max_length = max_length
        self.accepted_center_type = center_type

    def Measurements(self):
        '''
        Retrieve all licel files inside the folder.

        Returned value
        --------------
        A list of dictionaries describing each measurement.
        '''
        return self.measurements

    def DateFromFilename(self, file):
        '''
        Compute the starting date and time of a specified licel measurement file
        from the file name.

        Parameters
        ----------
        file : str
            The file name.

        Returned value
        --------------
        A datetime object if the date and time could be identified from the filename,
        otherwise it will return None.

        '''
        date = ''
        year = file[2:4]

        if 59 <= int(year) <= 99:
            date = '19' + year
        else:
            date = '20' + year

        month = file[4]
        if month == 'A':
            date += '10'
        elif month == 'B':
            date += '11'
        elif month == 'C':
            date += '12'
        else:
            date += '0'
            date += month

        # Day:
        date += file[5:7]

        # Hour:
        date += ' '
        date += file[7:9]
        date += ':'

        # Minutes:
        date += file[10:12]
        date += ':'

        # Seconds:
        date += file[-1]
        date += '0'
        #print(date)
        try:
            retval = datetime.strptime(date, "%Y%m%d %H:%M:%S")
        except ValueError as e:
            return None

        return retval

    def ReadInfoFromHeader(self, file):
        '''
        Retrieve information about a measurement from the licel file header.

        Parameters
        ----------
        file : str
            The absolute file path.


        Returned value
        --------------
        A dictionary containing the information read from the licel file header.
        '''
        raw_info = {}

        # Read the file header:
        f = licel.LicelFile(file, use_id_as_name=True, import_now=False)
        f.import_header_only()

        raw_info['Filename'] = f.raw_info['Filename']
        raw_info['Site'] = f.raw_info['site']

        # Construct a datetime object from the time and date information in the header:
        start_string = '%s %s' % (f.raw_info['start_date'], f.raw_info['start_time'])
        stop_string = '%s %s' % (f.raw_info['end_date'], f.raw_info['end_time'])

        raw_info['StartDateTime'] = datetime.strptime(start_string, '%d/%m/%Y %H:%M:%S')
        raw_info['StopDateTime'] = datetime.strptime(stop_string, '%d/%m/%Y %H:%M:%S')

        return raw_info

    def ReadFolder(self, start_date=None, end_date=None):
        '''
        Reads the folder and identifies all licel files in the folder and its subdirectories.

        Parameters
        ----------
        start_date : datetime or None
            Datetime object representing the earliest date a measurement could have been taken at.
            If set to None, this 'filter' will not be used.

        end_date : datetime or None
            Datetime object representing the latest date a measurement could have been taken at.
            If set to None, this 'filter' will not be used.
        '''
        # Reset measurements set:
        self.measurements = []

        # Walk the folder tree:
        for root, dirs, files in os.walk(self.folder):
            for file in files:
                path = os.path.join(root, file)

                try:
                    date = self.DateFromFilename(file)
                except Exception:
                    # Date could not be determined from the filename
                    # as this is most likely not a raw licel file!
                    continue

                if date == None:
                    continue

                # Only read the files that are between specified dates:
                good_file = False
                if start_date == None:
                    if end_date == None:
                        good_file = True
                    elif date <= end_date:
                        good_file = True
                elif end_date == None:
                    if date >= start_date:
                        good_file = True
                elif date >= start_date and date <= end_date:
                    good_file = True

                if good_file == True:
                    try:
                        info = self.ReadInfoFromHeader(path)
                        
                        self.measurements.append(Lidarchive.MeasurementFile(
                            path=path,
                            start_datetime=info['StartDateTime'],
                            end_datetime=info['StopDateTime'],
                            site=info['Site'],
                            type=''
                        ))
                    except Exception as e:
                        print(str(e))
                        pass
                    
        # Make sure we get a unique list of files!
        # Since we're walking down the folder tree, it might just so happen
        # that some files can be stored multiple times in different folders.
        #
        # When that happens, atmospheric-lidar is confused and throws errors,
        # so it's better to take care of it here.
        seen = set()
        self.measurements = [ m for m in self.measurements if m.Filename() not in seen and not seen.add(m.Filename()) ]

        self.measurements.sort(key=lambda x: x.StartDateTime())


def match_lines(f1, f2):
    list1 = f1.split()
    list2 = f2.split()

    combined = zip(list2, list1)
    combined = dict(combined)
    return combined 