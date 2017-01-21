import pandas as pd
import numpy as np
import pymysql
import sys
import time
import datetime
import os


#Set variables
ts = time.time()
getTodayDate = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')

file = open('/Users/dhong/Desktop/pwd.txt','r')

filepath = '/Users/dhong/Desktop/'+getTodayDate+'_weblog.csv'
parser = '/'
mysql_schema = 'Project_A'
mysql_table = 'weblogs'
pwd = file.read()
ifexists_method = 'append'




def myLoadProcess(filepath):
   
    #Import csv file containing links visited by date
    data = pd.read_csv(filepath, parse_dates = True)
    
    #Split dataframe into a tuple. Parse the address link using /
    splits = data['Date'], data['Link'].str.split('/')
    
    #Convert tuple to dataframe
    df = pd.DataFrame({
    'Date': splits[0],
    'Level_A': splits[1].str[2],
    'Level_B': splits[1].str[3],
    'Level_C': splits[1].str[4],
    'Level_D': splits[1].str[5],
    'Total' : ''
    })
    #Replace any null values with blank space
    cleandf = df.replace(np.nan, '')
        
    #Group by levels to get count on most frequent paths
    commonPaths = cleandf.groupby(['Date','Level_A', 'Level_B', 'Level_C', 'Level_D'], as_index=False).count() 
    
    #Get Unique dates. This will produce a numpy array
    unique_date = cleandf.Date.unique()
    
    #Convert array to dataframe for unique dates
    for i in unique_date:
        df_date = pd.DataFrame({
                'Date':unique_date
            })
    
    #Merge the batch number with the commonPaths dataset
    df_date['batch'] = df_date.index + 1
    joined_df = commonPaths.merge(df_date, how='inner', on='Date')
    
    print('Data transformation completed. Preparing to load into MySQL.')
    
    return joined_df
    
    
    
def batch_load(filepath, mysql_schema, mysql_table, ifexists_method):
    #Create batch looping process
    
    #Set max batch number as a variable using the joined_df output
    batch_num = joined_df.batch.unique()
    batch_list = batch_num.tolist() 
    end = max(batch_list)
    
    n = 1
    
    #Connect to MySQL database and insert dataframe into table
    db = pymysql.connect("localhost", "root", pwd, mysql_schema)
    
    
    #Iterate batch load process based on the value of n and stop when n reaches to the end variable
    while n <= end:
        batch = joined_df[joined_df['batch'] == n]
        batch.to_sql(con=db, name=mysql_table, if_exists=ifexists_method, flavor='mysql')
    
        #Create variable to get execution timestamp'''
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    
        print('Batch load',n,'from', filepath, 'to', mysql_schema, mysql_table,'table completed on', st)
        
        #Add 1 to n after each batch load completes
        n = n + 1




joined_df = myLoadProcess(filepath)

batch_load(filepath, mysql_schema, mysql_table, ifexists_method)

