import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import requests as re
import json
import pyodbc
import urllib3
import os
import glob
import time
#from geopy import distance
from utils import config
#from utils import db

url = 'https://agsivvwa.gsa.gov/gsagis1/rest/services/base/GeoLocate/GeocodeServer/geocodeAddresses'

serverName = config.serverName
password = config.password
database= config.database
userName =  config.userName

#sql_query = "SELECT [ReportingAgency__c] as Agency, [ReportingBureau__c] as Bureau, [RealPropertyUniqueId__c] as RPUID, [StateName__c] as Region, [CityName__c] as City, CAST([ZipCode__c] as VARCHAR(5)) as Postal, [StreetAddress__c] as Address FROM [OGPD2D].[dbo].[RP_FRPP_Salesforce_daily] where StreetAddress__c is not null and CountryName__c = 'United States' and DATEADD(SECOND, CAST(LastModifiedDate as BIGINT)/1000 ,'1970/1/1') > '2020/2/12'"
sql_query = "SELECT [Reporting_Agency] as Agency, [Reporting_Bureau] as Bureau, [Real Property Unique Identifier] as RPUID, [State] as Region, [City], CAST([Zip Code] as VARCHAR(5)) as Postal, [Street Address] as Address FROM [OGPD2D].[dbo].[RP_FRPP_Public_Dataset_FY19_Final] where latitude = '' or cast(Latitude as float) = 0 and Country = 'United States'"

def get_frpp():
    '''
    Sends credentials and SQL query, returns to dataframe
    '''
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+serverName+';DATABASE='+database+';UID='+userName+';PWD='+ password+'')
    df = pd.read_sql(sql_query, cnxn)
    cnxn.close()
    return df

FRPP_df = get_frpp()

### Create a unique ID as identifiers aren't standardized accross agencies. 
FRPP_df['OBJECTID'] = FRPP_df[['Agency','Bureau','RPUID']].apply(lambda x: '_'.join(x), axis=1)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_json():
    '''
    Loops through 1000 rows of FRPP_df and creates JSON for each row. Sends get request to API,
    captures JSON response and normalizes to dataframe and appends it to json_arr. Saving as a 
    list of objects vs dataframe for performance.
    '''
    json_arr = []
    init = iter_num * 1000
    x = init + 1000
    for index, row in FRPP_df[init:x].iterrows():
        time.sleep(0.01)
        FRPP_address = json.dumps(
            {       
                "records": [
                    {
                        "attributes": {
                        "OBJECTID": row['OBJECTID'],
                        "Address": row['Address'],
                        "City": row['City'],
                        "Region": row['Region'],
                        "Postal": row['Postal']
                    }
                    }
                ]
            }
            )


        r = re.get(url, params = { 'addresses': FRPP_address, 'f':'pjson'},verify = False)
        temp_df = json_normalize(r.json()['locations'])
        temp_df.insert(0,'OBJECTID',row['OBJECTID'])
        json_arr.append(temp_df)

    return json_arr

def json_to_excel():
    '''
    Convert json_arr to dataframe. Write dataframe to excel.
    '''
    geocoded_df = pd.concat(json_arr)
    file_name=  'data/archive/geocoded' + str(iter_num) + '.xlsx'
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

for i in range(0, 916):
    '''
    This was created to be able to stop/start iteration through all records as desired. Each increment
    reflects 1000 rows of data, leverages get_json function to get json response from api and json_to_excel
    function to convert json to dataframe and write to excel. Each file contains 1000 records. This allowed 
    me to start at x if something timed/errored out. 
    '''
    iter_num = i
    
    json_arr = get_json()
    
    json_to_excel()

final_df = read_multi_excel('data/archive/*.xlsx')

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
    'attributes.Score' :  'Score',
    'attributes.Side' :  'Side',
    'attributes.StAddr' :  'StAddr',
    'attributes.StDir' :  'StDir',
    'attributes.StName' :  'StName,',
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

final_df.to_excel('data/FRPP_geocoded_07092020.xlsx')

geo_lat = 'location.x'
geo_long = 'location.x'
input_lat = 'SUBMITTED LATITUDE'
input_long = 'SUBMITTED LONGITUDE'
final_df['DISTANCE VARIANCE'] = final_df.apply(
    (lambda row: distance.distance(
        (row[geo_lat], row[geo_long]),
        (row[input_lat], row[input_long])
    ).miles),
    axis=1
)