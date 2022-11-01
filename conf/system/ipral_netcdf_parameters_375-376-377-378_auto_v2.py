# INSERT HERE THE SYSTEM PARAMETERS
general_parameters = \
    {'System': 'IPRAL',  # CHANGE: No need for escape characters any more, before was: '\'IPRAL\''
     'Laser_Pointing_Angle': 0,
     'Molecular_Calc': 0,  # Use model
     'Sounding_File_Name': "" ,
     'Call sign': 'sir',}
     # CHANGE: The following parameters are not needed any more, as they are read from the Licel files.
     # However, if the values in the licel files are not correct, you can specify them again here. 
     # If you specify them here, these values will be used, and values in the licel file will be ignored.
     # 'Latitude_degrees_north': 48.713,     
     # 'Longitude_degrees_east': 2.208,
     # 'Altitude_meter_asl': 156.0,

# LINK YOUR LICEL CHANNELS TO SCC PARAMETERS. USE BT0, BC0 ETC AS NAMES (AS IN LICEL FILES).
channel_parameters = \
    {'BT5_L1_P2': {'channel_ID': 529,
             'Background_Low': 50000,
             'Background_High': 60000,
             'LR_Input': 1,},
     'BC5_L1_P2': {'channel_ID': 530,
             'Background_Low': 50000,
             'Background_High': 60000,
             'LR_Input': 1,},
     'BT0_L1_P1': {'channel_ID': 531,
             'Background_Low': 50000,
             'Background_High': 60000,
             'LR_Input': 1,},
     'BC0_L1_P0': {'channel_ID': 532,
             'Background_Low': 50000,
             'Background_High': 60000,
             'LR_Input': 1,},
     'BT10_L1_P1': {
             'channel_ID': 537,
             'Background_Low': 50000,
             'Background_High': 60000,
             'LR_Input': 1,},
     'BC10_L1_P1': {
             'channel_ID': 538,
             'Background_Low': 50000,
             'Background_High': 60000,
             'LR_Input': 1,},
     'BT11_L1_P0': {
             'channel_ID': 541,
             'Background_Low': 50000,
             'Background_High': 60000,
             'LR_Input': 1,},
     "BC11_L1_P0": {
             'channel_ID': 542,
             'Background_Low': 50000,
             'Background_High': 60000,
             'LR_Input': 1,},
     'BT12_L1_P2': {
             'channel_ID': 539,
             'Background_Low': 50000,
             'Background_High': 60000,
             'LR_Input': 1,},
     'BC12_L1_P2': {
             'channel_ID': 540,
             'Background_Low': 50000,
             'Background_High': 60000,
             'LR_Input': 1,},
     'BT1_L1_P1': {'channel_ID': 525,
             'Background_Low': 50000,
             'Background_High': 60000,
             'LR_Input': 1,},
     'BC1_L1_P1': {'channel_ID': 526,
             'Background_Low': 50000,
             'Background_High': 60000,
             'LR_Input': 1,},
     'BT2_L1_P1': {'channel_ID': 527,
             'Background_Low': 50000,
             'Background_High': 60000,
             'LR_Input': 1,},
     'BC2_L1_P1': {'channel_ID': 528,
             'Background_Low': 50000,
             'Background_High': 60000,
             'LR_Input': 1,},
     'BT3_L1_P0': {'channel_ID': 535,
             'Background_Low': 50000,
             'Background_High': 60000,
             'LR_Input': 1,},
     'BC3_L1_P0': {'channel_ID': 536,
             'Background_Low': 50000,
             'Background_High': 60000,
             'LR_Input': 1,},
     }
