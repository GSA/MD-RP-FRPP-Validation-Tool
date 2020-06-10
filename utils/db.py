import pandas as pd
import pyodbc
import urllib
from sqlalchemy import create_engine,event
from utils import config

serverName = config.serverName
database= config.database
userName =   config.userName
password =  config.password


sql_query = "SELECT [ReportingAgency__c], [ReportingBureau__c], [RealPropertyUniqueId__c], [StateName__c] as Region, [CityName__c] as City, CAST([ZipCode__c] as VARCHAR(5)) as Postal, [StreetAddress__c] as Address FROM [OGPD2D].[dbo].[RP_FRPP_Salesforce_daily] where StreetAddress__c is not null and CountryName__c = 'United States' and DATEADD(SECOND, CAST(LastModifiedDate as BIGINT)/1000 ,'1970/1/1') > '2020/2/12'"


def get_frpp():
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+serverName+';DATABASE='+database+';UID='+userName+';PWD='+ password+'')
    df = pd.read_sql(sql_query, cnxn)
    cnxn.close()
    return df
	

params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=" + serverName + ";DATABASE="+ database +";UID="+userName+";PWD=" +password)
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, module=pyodbc)

@event.listens_for(engine, 'before_cursor_execute') 
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany): 
    print("FUNC call") 
    if executemany: 
        cursor.fast_executemany = True


def send_data(df,sqlDB):
    df.to_sql(sqlDB,engine,if_exists='append',chunksize= None)