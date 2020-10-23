import pandas as pd
import numpy as np
from pandas import json_normalize
import requests as re
import json
import pyodbc
import urllib
import os
#os.chdir('G:\\Shared drives\\MD Database Team\\MA\\RP\\RP_FRPP_Validation_Tool\\MD-RP-FRPP-Validation-Tool')
import glob
import time
import sys
from geopy import distance
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
from utils import config 
import datetime 
from sqlalchemy import create_engine

dater = datetime.datetime.now() 
dater = (str(dater.month) + str(dater.day) + str(dater.year))
#print(dater)
#Get address data from SQL Server
sql_query = '''SELECT top 10 [Asset_ID__c] as OBJECTID
,[ReportingAgency__c] as Agency
,[ReportingBureau__c] as Bureau
,[RealPropertyUniqueId__c] as RPUID
,[StateName__c] as Region
,[CityName__c] as City
,CAST([ZipCode__c] as VARCHAR(5)) as Postal
,[StreetAddress__c] as Address 
FROM [OGPD2D].[dbo].[RP_FRPP_Salesforce_daily_vw] 
where CountryName__c = 'United States'
and [AssetUse__c] in ('office','warehouse')
and LegalInterest__c = 'Owned'
and cast(SquareFeet__c as float) > 10000 
and MSA is not null
'''

def get_frpp():
    '''
    Sends credentials and SQL query, returns to dataframe
    '''
    # cnxn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};SERVER=" + config.serverName + ";DATABASE="+ config.database +";UID="+config.userName+";PWD=" +config.password)
    # df = pd.read_sql(sql_query, cnxn)
    # cnxn.close()

    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+config.serverName+';DATABASE='+config.database+';UID='+config.userName+';PWD='+ config.password)
    #cursor = cnxn.cursor()
    df = pd.read_sql(sql_query, cnxn)
    # print(results)
    return df

FRPP_df = get_frpp()
#print(FRPP_df)

#credentials for ArcGIS

grant_type = 'refresh_token'
url = "https://maps.stg.gsa.gov/arcgis/sharing/rest/oauth2/token"
geocode_url = 'https://gsagis.stg.gsa.gov/arcgis/rest/services/GSA/GSA_USA/GeocodeServer/geocodeAddresses'
payload = {
    'refresh_token': config.refreshToken,
    'client_id': config.clientId,
    'grant_type': grant_type
}

def get_token(url, payload):
    
    token_response = re.get(url, params=payload)
    token = token_response.json()['access_token']
    
    return token

#disable warning 
#urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_json():
    '''
    Loops through 1000 rows of FRPP_df and creates JSON for each row. Sends get request to API,
    captures JSON response and normalizes to dataframe and appends it to json_arr. Saving as a 
    list of objects vs dataframe for performance.
    '''
    json_arr = []
    #init = iter_num * 1000
    #x = init + 1000
    #print("Processed: ", x)
    for index, row in FRPP_df.iterrows():
        time.sleep(0.01)
        FRPP_address = json.dumps(
            {       
                "records": [
                    {
                        'attributes': {
                        'OBJECTID': row['OBJECTID'],
                        'Address': row['Address'],
                        'City': row['City'],
                        'Region': row['Region'],
                        'Postal': row['Postal']
                    }
                    }
                ]
            })
        print("Processed: ", index)
        token = get_token(url, payload)

        geocode_payload = {
            'addresses': FRPP_address,
            'token': token,
            'f': 'pjson'
        }

        geocode_results = re.get(geocode_url, params = geocode_payload)
        #print(geocode_results.json())

        temp_df = json_normalize(geocode_results.json()['locations'])
        temp_df.insert(0,'OBJECTID',row['OBJECTID'])
        json_arr.append(temp_df)
    return json_arr
    
def json_to_excel():
    '''
    Convert json_arr to dataframe. Write dataframe to excel.
    '''
    geocoded_df = pd.concat(json_arr)
    file_name=  'data/archive/geocoded_' + dater + '.xlsx'
    geocoded_df.to_excel(file_name)

def read_multi_excel(path):
    '''
    Given a file path with wildcard and extension, parse all files with that extension in directory 
    into a single dataframe.
    '''
    
    all_files = glob.glob(path)
    li = []
    
    for filename in all_files:
        df = pd.read_excel(filename, index_col=None, header=0)
        df['Source'] = os.path.basename(filename)
        li.append(df)
        
    df = pd.concat(li, axis=0, ignore_index=True)
    
    return df

counter = len(FRPP_df.index) 
f = open("FRPP_Validation_Elapsed_Time_Log.txt", "a")

startTime = datetime.datetime.now()
json_arr = get_json()
closeTime = datetime.datetime.now()
elapsedTime = closeTime - startTime

f.write("ArcGIS API StartTime: " + str(startTime) + " closeTime: " + str(closeTime) + " elapsedTime: " + str(elapsedTime) + " count of API hits: " + str(counter))
f.close()

json_to_excel()

final_df = pd.read_excel("data/archive/geocoded_" + str(dater) + ".xlsx")

final_df.rename(columns={
    'attributes.AddNum' :  'AddNum',
    'attributes.AddNumFrom' :  'AddNumFrom',
    'attributes.AddNumTo' :  'AddNumTo',
    'attributes.Addr_type' :  'Addr_type',
    'attributes.City' :  'City',
    'attributes.Country' :  'Country',
    'attributes.DisplayX' :  'DisplayX',
    'attributes.DisplayY' :  'DisplayY',
    'attributes.Distance' :  'Distance',
    'attributes.LangCode' :  'LangCode',
    'attributes.Loc_name' :  'Loc_name',
    'attributes.Match_addr' :  'Match_addr',
    'attributes.Postal' :  'Postal',
    'attributes.Rank' :  'Rank',
    'attributes.Region' :  'Region',
    'attributes.RegionAbbr' :  'RegionAbbr',
    'attributes.ResultID' :  'ResultID',
    'attributes.Side' :  'Side',
    'attributes.StAddr' :  'StAddr',
    'attributes.StDir' :  'StDir',
    'attributes.StName' :  'StName',
    'attributes.StPreDir' :  'StPreDir',
    'attributes.StPreType' :  'StPreType',
    'attributes.StType' :  'StType',
    'attributes.Status' :  'Status',
    'attributes.X' :  'X',
    'attributes.Xmax' :  'Xmax',
    'attributes.Xmin' :  'Xmin',
    'attributes.Y' :  'Y',
    'attributes.Ymax' :  'Ymax',
    'attributes.Ymin' :  'Ymin',
    'location.x' :  'location.x',
    'location.y ' :  'location.y'}, 
    inplace=True)

final_df.to_excel('data/FRPP_geocoded_' + dater + '.xlsx')

import urllib
from sqlalchemy.engine import create_engine
from sqlalchemy.types import Integer, Text, String, DateTime
import sqlalchemy
params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=" + config.serverName + ";DATABASE="+ config.database +";UID="+ config.userName+";PWD=" + config.password)
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, module=pyodbc,echo=False)

frppValidation = pd.read_excel("data/FRPP_geocoded_" + dater + ".xlsx")
#print(frppValidation)

#FASCN_table.columns
frppValidation.to_sql("RP_FRPP_Validation_Nightly",
               engine,
               if_exists='replace',
               schema='dbo',
               index=False,
               chunksize=500)


# geo_lat = 'location.x'
# geo_long = 'location.x'
# input_lat = 'SUBMITTED LATITUDE'
# input_long = 'SUBMITTED LONGITUDE'
# final_df['DISTANCE VARIANCE'] = final_df.apply(
#     (lambda row: distance.distance(
#         (row[geo_lat], row[geo_long]),
#         (row[input_lat], row[input_long])
#     ).miles),
#     axis=1
# )
