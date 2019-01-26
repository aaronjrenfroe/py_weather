from pymongo import MongoClient
import urllib.request as request
import json
from datetime import datetime as dt

API_KEY = ''
url = 'https://api.darksky.net/forecast/'+API_KEY+'/{},{}'

def update_database(forecast_collection, forecast):
  fore_date = dt.fromtimestamp(forecast['time'])
  today_date = dt.now()
  forecasted_day = fore_date.strftime("%Y-%m-%d")
  loc_key = "{}x{}".format(forecast['coords']['lon'], forecast['coords']['lat']).replace('.','_')
  days_before = (today_date - fore_date).days * -1

  day_entry = forecast_collection.find_one({"day":forecasted_day})
  # if Brand new Day
  if day_entry == None:
    day_entry = {"day":forecasted_day,
      "locations" :{
        loc_key: { 
          "coords" :
            { 'lon':forecast['coords']['lon'],
              'lat':forecast['coords']['lat']
            },
            "forecasts": {str(days_before):forecast}
        }
      }
    }
  else:
    # if brand new location
    if loc_key not in day_entry["locations"].keys():
      day_entry["locations"][loc_key] = {
        "coords" :
          { 'lon':forecast['coords']['lon'],
            'lat':forecast['coords']['lat']
          },
        "forecasts" : {}}
    # Adding weather to location for the forecasted day
    day_entry["locations"][loc_key]["forecasts"][str(days_before)] = forecast
  
  forecast_collection.save(day_entry)


def get_forecast(lon, lat):
  location_url = url.format(lon,lat)
  print(location_url)
  src = request.urlopen(location_url).read()
  weather = json.loads(src)
  forecast = weather['daily']['data']
  
  return forecast

def get_points():
  points = []
  with open('points.csv', 'r') as file:
    lines = file.read().split('\n')
    for row in lines:
      points.append(row.split(','))
  return points

def __main__():

  client = MongoClient('192.168.0.34:27017')
  weather_db = client.weather_test2
  forecast_collection = weather_db.forecasts
  
  for p in get_points()[1:]:
    dailies = get_forecast(*p)
    for forecast in dailies:
        forecast['coords'] = {'lon':p[0], 'lat':p[1]}
        update_database(forecast_collection, forecast)
        

if __name__ == '__main__':
  __main__()
