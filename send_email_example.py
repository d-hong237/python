#!/usr/bin/python
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import subprocess
import os
import logging
#Need to do sys path append if running script in crontab
import sys; sys.path.append('file path of your packages')
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from datetime import datetime, timedelta

#Get current UTC date - 1. This date is the regional fcst week and the variable to use for calling the statements below.
get_date = datetime.now() - timedelta(days=1)
fcst_week = get_date.strftime('%Y-%m-%d')

#Get last week date. This will be use to insert forecast spread into historical table in Redshift.
get_last_week_date = datetime.now() - timedelta(days=8)
get_last_week_date = get_last_week_date.strftime('%Y-%m-%d')


def getandsplitFcst():

    #Copy asin forecast file to EC2
    subprocess.call(["aws", "s3", "cp", "s3://xxx/rfs/"+fcst_week+"/", "/dev/shm/", "--recursive", "--exclude" , "*" ,"--include" ,"rfs-us-asin-"+fcst_week+".tsv.gz_part*"])
    #Combine all parts into a single zip file
    subprocess.call("cat /dev/shm/rfs-us-asin-"+fcst_week+".tsv.gz_part*  > /dev/shm/rfs-us-asin-"+fcst_week+".tsv.gz", shell=True)
    #Unzip gz file
    subprocess.call("gzip -d /dev/shm/rfs-us-asin-"+fcst_week+".tsv.gz", shell=True)
    #Split tsv file into chunks of 50 million rows
    subprocess.call("sudo split -l 50000000 /dev/shm/rfs-us-asin-"+fcst_week+".tsv /dev/rfs_folder/"+fcst_week+"asin_load", shell=True)
    #Copy file chunks back to S3 bucket in folder called redshift_test
    subprocess.call(["aws", "s3", "cp", "/dev/rfs_folder/", "s3://xxx/redshift_test/", "--recursive", "--exclude", "*" ,"--include", ""+fcst_week+"asin_load*"])



def loadtoS3andRS():

    #Get list of file names. This will be used to iterate the COPY command
    filenames = next(os.walk('/dev/rfs_folder'))[2]

    get_asin_files = []

    for x in filenames:
        get_asin_files.append(x)


    get_asin_files.sort()

    #Connect to Redshift
    engine_string = "postgresql+psycopg2://{user}:{password}@{endpoint}:{port}/{dbname}".format (user="xxx",password="xxx",endpoint="xxx.redshift.amazonaws.com",port=8192,dbname="xxx")


    engine = create_engine(engine_string)
    connection = engine.connect()


    #Truncate TABLE SCHEMA.TABLE_NAME
    connection.execute(text("TRUNCATE TABLE SCHEMA.TABLE_NAME").execution_options(autocommit=True))

    #For loop statement to iterate the list of filenames for COPY
    for name in get_asin_files:

        #The first file will include a header. Need to implement ignoreheader in sql statement
        if name in get_asin_files[0]:
            connection.execute(text("COPY SCHEMA.TABLE_NAME(column names) FROM 's3://xxx/redshift_test/"+name+"' access_key_id 'xxx' secret_access_key 'xxx' ignoreheader 1 delimiter '\t';").execution_options(autocommit=True))
        else:
            connection.execute(text("COPY SCHEMA.TABLE_NAME(column names) FROM 's3://xxx/redshift_test/"+name+"' access_key_id 'xxx' secret_access_key 'xxx' delimiter '\t';").execution_options(autocommit=True))


    connection.close()


def cleanupEC2Space():
    #Remove asin files in /dev/shm location
    subprocess.call("sudo rm /dev/shm/rfs-us-asin*", shell=True)

    #Remove asin files in /dev location
    subprocess.call("sudo rm /dev/rfs_folder/*", shell=True)



def sendmailError():

    LOG_FILENAME = '/python_scripts/error.txt'
    logging.basicConfig(filename=LOG_FILENAME, level=logging.ERROR)
    logging.exception('Error generated on' +' '+ str(datetime.now()))
    logging.debug('Error found. Please read message.')

    msg = MIMEMultipart()
    sender = 'user@email.com'
    recipient = 'user@email.com'
    msg['Subject'] = "Error running script"
    msg['From'] = sender 
    msg['To'] = recipient
    attachment = open(LOG_FILENAME, 'rb')

    part = MIMEBase('application', 'octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename= LOG_ERROR.txt')

    msg.attach(part)
    text = msg.as_string()
    s = smtplib.SMTP('xxx.amazonaws.com', 587)
    EMAIL_HOST_USER = 'xxx'
    EMAIL_HOST_PASSWORD = 'xxx'
    s.starttls()
    s.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
    s.sendmail(sender, recipient, text)
    s.quit()


def sendmailSuccess():

    msg = MIMEMultipart()
    sender = 'user@email.com'
    recipient = 'user@email.com'
    msg['Subject'] = "Successfully ran script"
    msg['From'] = sender 
    msg['To'] = recipient
    
    body = 'Job finished on' +' '+ str(datetime.now())


    msg.attach(MIMEText(body, 'plain'))
    text = msg.as_string()
    s = smtplib.SMTP('xxx.amazonaws.com', 587)
    EMAIL_HOST_USER = 'xxx'
    EMAIL_HOST_PASSWORD = 'xxx'
    s.starttls()
    s.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
    s.sendmail(sender, recipient, text)
    s.quit()



try:
    getandsplitFcst()
except:
    sendmailError()
    raise


try:
    loadtoS3andRS()
except:
    sendmailError()
    raise


try:
    cleanupEC2Space()
except:
    sendmailError()
    raise


sendmailSuccess()