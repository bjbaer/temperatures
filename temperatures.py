import requests
import datetime
import time
import sqlite3 as lite
import collections

cities = {  "Chicago": '41.837551,-87.681844',
            "Cleveland": '41.478462,-81.679435',
            "Denver": '39.761850,-104.881105',
            "Minneapolis": '44.963324,-93.268320',
            "Philadelphia": '40.009376,-75.133346'            
        }
weather_api = "58fbb6dcc963c66e3a2ebb3b101b32bc"
weather_site = "https://api.forecast.io/forecast/"

con = lite.connect('weather.db')
cur = con.cursor()
cities.keys()
with con:
	cur.execute('CREATE TABLE daily_temp ( day_of_reading INT, Chicago REAL, Cleveland REAL, Denver REAL, Minneapolis REAL, Philadelphia REAL)')


cur_time =  time.mktime(datetime.datetime.now().timetuple()) #unix time
last_month = cur_time - (86400 * 29)
moving_time = int(last_month)
with con:
    for i in range(30):
    	cur.execute("INSERT INTO daily_temp(day_of_reading) VALUES (?)", (int(moving_time),))
        moving_time += 86400

for key in cities:

	moving_time = int(last_month)
	x_cord = cities[key].split(",")[0]
	y_cord = cities[key].split(",")[1]
	for i in range(30):
		weather = weather_site + weather_api + "/" + x_cord + "," + y_cord + "," + str(moving_time)
		r = requests.get(weather)
		tempmax = r.json()['daily']['data'][0]['temperatureMax']
		with con:
			cur.execute('UPDATE daily_temp SET '+ key+ '=%s WHERE day_of_reading = %s;' % (tempmax, moving_time))
		moving_time += 86400

con.close()

#create a dataframe based on the data
con = lite.connect('weather.db')
cur = con.cursor()
df = pd.read_sql_query("SELECT * FROM daily_temp ORDER BY day_of_reading",con,index_col='day_of_reading')

#create the differences
day_change = collections.defaultdict(int)
for col in df.columns:
	vals = df[col].tolist()
	id = col[:]
	change = 0
	for k,v in enumerate(vals):
		if k < len(vals) - 2:
			change += abs(vals[k]-vals[k+1])
	change[id] = day_change