import pandas as pd
import requests
import json
import time

# NOTE: Put in your own nija api token:
ninja_api_token = "PUT IN YOUR OWN API TOKEN HERE"

# OUTPUT DATA:
file_timeseries = "timeseries_nve2023_30y"
file_timeseries_temp = "timeseries_nve2023_30y.csv_temp"

# INPUT DATA: Location of wind farms
# Norwegian offshore wind farm scenario assumed in OceanGrid project
windfarms = pd.read_csv("oceangrid_offshorewind_new.csv")

####

windfarms["oceangrid"] = True
windfarms_additional = pd.DataFrame(
    [
        #    {"index": "X1", "name": "Finnmark", "lat": 70.94348042598155, "lon": 22.550980083019425},
        {"index": "A3", "name": "GB_Doggerbank", "lat": 54.75, "lon": 1.916667},
        {"index": "A2", "name": "DK_Horns Rev", "lat": 55.529722, "lon": 7.906111},
        {"index": "A1", "name": "DE_Baltic2", "lat": 54.9733, "lon": 13.1778},
    ]
)
windfarms_additional["oceangrid"] = False
windfarms = pd.concat([windfarms, windfarms_additional], axis=0)
windfarms = (
    windfarms.sort_values("index", ascending=False)[
        ["name", "lat", "lon", "oceangrid", "2030", "2035", "2040"]
    ]
    .fillna(0)
    .reset_index(drop=True)
)


# Renewables.ninja API
token = ninja_api_token
api_base = "https://www.renewables.ninja/api/"
s = requests.session()
s.headers = {"Authorization": "Token " + token}
print("limits:", s.get(api_base + "limits").text)


turbine = "Vestas V80 2000"
height = 100
year_range = range(1991, 2021)
num_requests = len(year_range) * windfarms.shape[0]
print(f"#requests = {num_requests} => {num_requests%48} hours")

windfarm_data = {}
counter = 0
for i, wfarm in windfarms.iterrows():
    print("\n", i, wfarm["name"])
    lat = wfarm["lat"]
    lon = wfarm["lon"]
    wind_per_year = []
    for year in year_range:
        counter = counter + 1
        print(year, end=" ")
        args = {
            "lat": lat,
            "lon": lon,
            "date_from": f"{year}-01-01",
            "date_to": f"{year}-12-31",
            "capacity": 1,
            "height": height,
            "turbine": turbine,
            "format": "json",
            "raw": True,
        }
        try:
            r = s.get(api_base + "data/wind", params=args)
        except:
            print("GET Failed - trying to reconnect and try again")
            s = requests.session()
            s.headers = {"Authorization": "Token " + token}
            r = s.get(api_base + "data/wind", params=args)
        parsed_response = json.loads(r.text)
        data = pd.read_json(json.dumps(parsed_response["data"]), orient="index")
        metadata = parsed_response["metadata"]
        wind_per_year.append(data)
        # Wait, to stay within limit : max 50 per hour
        # i.e. should spend 72 sec per request. 72*50=1 hour
        # time.sleep(73) # wait 73 sec to below 50 requests per hour
        time.sleep(1)
        if counter % 48 == 0:  # 48 instead of 50 to be on the safe side.
            # wait an hour (save first)
            df_all_wind = pd.concat(windfarm_data, axis=1)
            df_all_wind.to_csv(f"{file_timeseries_temp}.csv")
            print(f"\n{counter} - Waiting an hour - {time.asctime()}")
            time.sleep(60 * 60)
    windfarm_data[i] = pd.concat(wind_per_year)
df_all_wind = pd.concat(windfarm_data, axis=1)
print(df_all_wind.shape)

# Save to excel file for easier use next time:
df_all_wind.to_csv(f"{file_timeseries}.csv")
df_all_wind.xs("electricity", axis=1, level=1).to_excel(
    f"{file_timeseries}_electricity.xlsx"
)
df_all_wind.xs("wind_speed", axis=1, level=1).to_excel(
    f"{file_timeseries}__windspeed.xlsx"
)

print("Done")
