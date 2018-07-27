# -*- coding: utf-8 -*-
"""
Created on Thu Jul 26 14:36:14 2018

@author: V0010894
"""

import pandas as pd
import sqlalchemy as sq
import os
import boto3
import zipfile

pd.options.display.max_columns = 10
s3 = boto3.client('s3')

uid = os.environ['ATSPM_USERNAME']
pwd = os.environ['ATSPM_PASSWORD']
dsn = 'sqlodbc'
connection_string = 'mssql+pyodbc://{}:{}@{}'.format(uid, pwd, dsn)

engine = sq.create_engine(connection_string, pool_size=20)

# Query ATSPM Watchdog Alerts Table

with engine.connect() as conn:

    SPMWatchDogErrorEvents = pd.read_sql_table('SPMWatchDogErrorEvents', con=conn)

wd = SPMWatchDogErrorEvents.loc[SPMWatchDogErrorEvents.SignalID != 'null', ]
wd = wd.assign(SignalID = lambda x: x.SignalID.astype('uint16'))

# Read Corridors File on The SAM

corridors = pd.read_feather('../GDOT-Flexdashboard-Report/corridors.feather')
corridors = (corridors[~corridors.SignalID.isna()]
            .assign(SignalID = lambda x: x.SignalID.astype('uint16'))
            .drop(['Description'], axis=1))

# Join and munge the Watchdog Alerts, wd
wd = wd.set_index(['SignalID']).join(corridors.set_index(['SignalID']), how = 'left')
wd = wd[~wd.Corridor.isna()].drop(['ID'], axis=1)

wd.TimeStamp = wd.TimeStamp.dt.date

# Create Alerts column with five possible values

wd = wd.assign(Alert = '')
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

# Convert to category data type wherever possible to reduce file size

wd.Alert = wd.Alert.astype('category')
wd.DetectorID = wd.DetectorID = wd.DetectorID.astype('category')
wd.Direction = wd.Direction.astype('category')
wd.Phase = wd.Phase.astype('category')
wd.ErrorCode = wd.ErrorCode.astype('category')
wd.Zone = wd.Zone.astype('category')
wd.Zone_Group = wd.Zone_Group.astype('category')



#wd.reset_index().to_parquet('SPMWatchDogErrorEvents.parquet')
#s3.upload_file(Filename='SPMWatchDogErrorEvents.parquet',
#               Bucket='gdot-devices', 
#               Key='watchdog/SPMWatchDogErrorEvents.parquet')


# Write to Feather file

feather_filename = 'SPMWatchDogErrorEvents.feather'
zipfile_filename = feather_filename + '.zip'
wd.reset_index().to_feather(feather_filename)

# Compress file

zf = zipfile.ZipFile(zipfile_filename, 'w', zipfile.ZIP_DEFLATED)
zf.write(feather_filename)
zf.close()

# Upload compressed file to s3

s3.upload_file(Filename=zipfile_filename,
               Bucket='gdot-devices', 
               Key=zipfile_filename)


