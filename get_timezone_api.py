import json
import requests
import pandas as pd


df = pd.read_csv('/Users/hongd/Desktop/py_scripts/lat_lng2.csv', delimiter=',')


lat = df['lat_from'].tolist()
lng = df['lng_from'].tolist()

lat = [str(x) for x in lat]
lng = [str(x) for x in lng]

getlist = []


for index, value in enumerate(lat, start=0):
    url = "http://api.geonames.org/timezoneJSON?lat="+lat[index]+"&lng="+lng[index]+"&username=xxx"
    r = requests.get(url)
    jsonstr = json.loads(r.text)
    #Need to make json keys all lowercase for load to Redshift table
    getlist.append({k.lower(): v for k, v in jsonstr.items()})




results = json.dumps(getlist)
results = results.replace('[','').replace(']','').replace('},','}')



json_file = open('/Users/hongd/Desktop/timezone2.txt', 'w')
json_file.write(results)
json_file.close()
