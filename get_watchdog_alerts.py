# -*- coding: utf-8 -*-
"""
Created on Thu Jul 26 14:36:14 2018

@author: V0010894
"""

import pandas as pd
import sqlalchemy as sq
import pyodbc
import os
import boto3
import zipfile
import feather

pd.options.display.max_columns = 10
s3 = boto3.client('s3')

if os.name == 'nt':
    
    uid = os.environ['ATSPM_USERNAME']
    pwd = os.environ['ATSPM_PASSWORD']
    dsn = 'sqlodbc'
    connection_string = 'mssql+pyodbc://{}:{}@{}'.format(uid, pwd, dsn)
    
    engine = sq.create_engine(connection_string, pool_size=20)

elif os.name=='posix':

    def connect():
        return pyodbc.connect(
            'DRIVER=FreeTDS;' + 
            'SERVER={};'.format(os.environ["ATSPM_SERVER_INSTANCE"]) +
            'DATABASE={};'.format(os.environ["ATSPM_DB"]) +
            'UID={};'.format(os.environ['ATSPM_USERNAME']) +
            'PWD={};'.format(os.environ['ATSPM_PASSWORD']) +
            'TDS_Version=8.0;')
    
    engine = sq.create_engine('mssql://', creator=connect)

# Query ATSPM Watchdog Alerts Table

with engine.connect() as conn:

    SPMWatchDogErrorEvents = pd.read_sql_table('SPMWatchDogErrorEvents', con=conn)
    #BadDetectors = pd.read_sql_table('BadDetectors', con=conn)

BadDetectors = feather.read_dataframe('../GDOT-Flexdashboard-Report/bad_detectors.feather')

# Read Corridors File on The SAM

corridors = feather.read_dataframe('../GDOT-Flexdashboard-Report/corridors.feather')
corridors = (corridors[~corridors.SignalID.isna()]
            .assign(SignalID = lambda x: x.SignalID.astype('uint16'))
            .drop(['Description'], axis=1))

# Join and munge the Watchdog Alerts, wd

wd = SPMWatchDogErrorEvents.loc[SPMWatchDogErrorEvents.SignalID != 'null', ]
wd.SignalID = wd.SignalID.astype('uint16')

wd = wd.set_index(['SignalID']).join(corridors.set_index(['SignalID']), how = 'left')
wd = (wd[~wd.Corridor.isna()].drop(['ID', 'Asof', 'Milepost', 'Agency'], axis=1)
    .assign(TimeStamp = lambda x: x.TimeStamp.dt.date))


bd = BadDetectors.loc[BadDetectors.SignalID != 'null', ]
bd.SignalID = bd.SignalID.astype('uint16')

bd = bd.set_index(['SignalID']).join(corridors.set_index(['SignalID']), how = 'left')
bd = (bd[~bd.Corridor.isna()].drop(['Asof', 'Milepost', 'Agency'], axis=1)
    .rename(columns = {'Detector': 'DetectorID', 
                       'Date': 'TimeStamp'})
    .assign(TimeStamp = lambda x: x.TimeStamp.dt.date))

# Concatenate Watchdog with Bad Detectors
    
wd = pd.concat([wd, bd], sort = True)
# Create Alerts column with five possible values

wd = wd.assign(Alert = '')
wd.loc[wd.Message.isna(), 'Alert'] = 'Bad Detection'
wd.loc[wd.Message.isna(), 'Message'] = 'Bad Detection'
wd.loc[wd.Message.str.startswith('Force Offs'), 'Alert'] = 'Force Offs'
wd.loc[wd.Message.str.startswith('Count'), 'Alert'] = 'Count'
wd.loc[wd.Message.str.startswith('Max Outs'), 'Alert'] = 'Max Outs'
wd.loc[wd.Message.str.endswith('Pedestrian Activations'), 'Alert'] = 'Pedestrian Activations'
wd.loc[wd.Message=='Missing Records', 'Alert'] = 'Missing Records'

# Simplify Zones and Districts

wd.Zone = wd.Zone.astype('str')
wd.loc[wd.Zone=='Z1', 'Zone'] = 'Zone 1'
wd.loc[wd.Zone=='Z2', 'Zone'] = 'Zone 2'
wd.loc[wd.Zone=='Z3', 'Zone'] = 'Zone 3'
wd.loc[wd.Zone=='Z4', 'Zone'] = 'Zone 4'
wd.loc[wd.Zone=='Z5', 'Zone'] = 'Zone 5'
wd.loc[wd.Zone=='Z6', 'Zone'] = 'Zone 6'
wd.loc[wd.Zone=='Z7', 'Zone'] = 'Zone 7'

wd.loc[wd.Zone_Group=='D3', 'Zone'] = 'District 3'
wd.loc[wd.Zone_Group=='D4', 'Zone'] = 'District 4'
wd.loc[wd.Zone_Group=='D5', 'Zone'] = 'District 5'
wd.loc[wd.Zone_Group=='D7', 'Zone'] = 'District 7'


#bd.Zone = bd.Zone.astype('str')
#bd.loc[bd.Zone=='Z1', 'Zone'] = 'Zone 1'
#bd.loc[bd.Zone=='Z2', 'Zone'] = 'Zone 2'
#bd.loc[bd.Zone=='Z3', 'Zone'] = 'Zone 3'
#bd.loc[bd.Zone=='Z4', 'Zone'] = 'Zone 4'
#bd.loc[bd.Zone=='Z5', 'Zone'] = 'Zone 5'
#bd.loc[bd.Zone=='Z6', 'Zone'] = 'Zone 6'
#bd.loc[bd.Zone=='Z7', 'Zone'] = 'Zone 7'
#
#bd.loc[bd.Zone_Group=='D3', 'Zone'] = 'District 3'
#bd.loc[bd.Zone_Group=='D4', 'Zone'] = 'District 4'
#bd.loc[bd.Zone_Group=='D5', 'Zone'] = 'District 5'
#bd.loc[bd.Zone_Group=='D7', 'Zone'] = 'District 7'


# Convert to category data type wherever possible to reduce file size

wd.Alert = wd.Alert.astype('category')
wd.DetectorID = wd.DetectorID = wd.DetectorID.astype('category')
wd.Direction = wd.Direction.astype('category')
wd.Phase = wd.Phase.astype('category')
wd.ErrorCode = wd.ErrorCode.astype('category')
wd.Zone = wd.Zone.astype('category')
wd.Zone_Group = wd.Zone_Group.astype('category')

wd.Corridor = wd.Corridor.astype('category')
wd.Name = wd.Name.astype('category')

#bd.DetectorID = bd.DetectorID = bd.DetectorID.astype('category')
#bd.Zone = bd.Zone.astype('category')
#bd.Zone_Group = bd.Zone_Group.astype('category')
#bd.Corridor = bd.Corridor.astype('category')
#bd.Name = bd.Name.astype('category')

#wd.reset_index().to_parquet('SPMWatchDogErrorEvents.parquet')
#s3.upload_file(Filename='SPMWatchDogErrorEvents.parquet',
#               Bucket='gdot-devices', 
#               Key='watchdog/SPMWatchDogErrorEvents.parquet')


# Write to Feather file - WatchDog

def s3_upload(df, feather_filename, zipfile_filename):
    df.reset_index().to_feather(feather_filename)
    
    # Compress file
    
    zf = zipfile.ZipFile(zipfile_filename, 'w', zipfile.ZIP_DEFLATED)
    zf.write(feather_filename)
    zf.close()
    
    # Upload compressed file to s3
    
    s3.upload_file(Filename=zipfile_filename,
                   Bucket='gdot-devices', 
                   Key=zipfile_filename)

feather_filename = 'SPMWatchDogErrorEvents.feather'
#zipfile_filename = feather_filename + '.zip'
s3_upload(wd, feather_filename, feather_filename + '.zip')





