import argparse
import logging
from datetime import datetime

from ..lidarchive import Lidarchive

def main ():

    parser = argparse.ArgumentParser(description="A program for reading raw atmospheric lidar data folders.")
    parser.add_argument("folder", help="The path to the folder you want to scan.", default=".")
    parser.add_argument("startdate", help="The path to the folder you want to scan.", default="20171201000000")
    parser.add_argument("enddate", help="The path to the folder you want to scan.", default="20171231235959")

    args = parser.parse_args()

    # Get the logger with the appropriate level
    logging.basicConfig(format='%(levelname)s: %(message)s')
    logger = logging.getLogger(__name__)

    lidarchive = Lidarchive ()
    lidarchive.SetFolder (args.folder)

    start_date = datetime.strptime( args.startdate, '%Y%m%d%H%M%S' )
    end_date = datetime.strptime( args.enddate, '%Y%m%d%H%M%S' )

    print "Start Date: %s\nEnd Date: %s" % (start_date, end_date)

    print "Reading files..."
    lidarchive.ReadFolder (start_date, end_date)
    
    licel_measurements = lidarchive.ContinuousMeasurements ( 600, 3600 )

    print "Identified %d continuous measurements." % len ( licel_measurements )
