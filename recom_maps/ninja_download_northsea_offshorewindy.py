import pandas as pd
import numpy as np
import requests
import json
import time

# Insert Renewables.ninja API token here:
token_ninja = ""


# No need to edit below here --------------------

file_timeseries = "timeseries_5y.csv"
file_timeseries_temp = "timeseries_5y_temp.csv"

if token_ninja=="":
    raise Exception("You must insert renewables.ninja api token.")

delta_lat = 1
delta_lon = 1.5
lat_range = np.arange(51, 67, delta_lat)
lon_range = np.arange(-6.5, 17, delta_lon)

coords = []
for lat in lat_range:
    for lon in lon_range:
         coords.append((lon, lat))
print(f"Number of coordinate points = {len(coords)}")

# Renewables.ninja API
api_base = "https://www.renewables.ninja/api/"
s = requests.session()
s.headers = {"Authorization": "Token " + token_ninja}
print("limits:", s.get(api_base + "limits").text)

wind_data = {}
counter = 0
for pt in coords:
    lon = pt[0]
    lat = pt[1]
    wind_per_year = []
    for year in [2018, 2019, 2020, 2021, 2022]:
        counter = counter + 1
        print(counter, lat, lon, year)
        args = {
            "lat": lat,
            "lon": lon,
            "date_from": f"{year}-01-01",
            "date_to": f"{year}-12-31",
            "capacity": 1,
            "height": 100,
            "turbine": "Vestas V80 2000",
            "format": "json",
            "raw": True,
        }
        try:
            r = s.get(api_base + "data/wind", params=args)
        except:
            print("GET Failed - trying to reconnect and try again")
            s = requests.session()
            s.headers = {"Authorization": "Token " + token_ninja}
            r = s.get(api_base + "data/wind", params=args)

        parsed_response = json.loads(r.text)
        data = pd.read_json(json.dumps(parsed_response["data"]), orient="index")
        metadata = parsed_response["metadata"]
        wind_per_year.append(data)
        # Wait, to stay within limit : max 50 per hour
        # i.e. should spend 72 sec per request. 72*50=1 hour
        # time.sleep(73)  # wait 73 sec to below 50 requests per hour
        time.sleep(1)
        if counter % 48 == 0:  # 48 instead of 50 to be on the safe side.
            # wait an hour (save first)
            df_all_wind = pd.concat(wind_data, axis=1)
            df_all_wind.to_csv(file_timeseries_temp)
            print(f"Waiting an hour - {time.asctime()}")
            time.sleep(60 * 60)
    wind_data[(lat, lon)] = pd.concat(wind_per_year)
df_all_wind = pd.concat(wind_data, axis=1)
df_all_wind.to_csv(file_timeseries)

