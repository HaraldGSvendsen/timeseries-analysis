import pandas as pd
import requests
import json
import time

# NOTE: Put in your own nija api token:
#ninja_api_token = "PUT IN YOUR OWN API TOKEN HERE"

file_timeseries = "wind_power_ninja_smola2.xlsx"


# Renewables.ninja API
token = ninja_api_token
api_base = "https://www.renewables.ninja/api/"
s = requests.session()
s.headers = {"Authorization": "Token " + token}


# Smøla:
data1=[]
for year in range(2016,2021):
    args = {
        'lat': 63.407, 
        'lon': 7.924,
        'date_from': f'{year}-01-01',
        'date_to': f'{year}-12-31',
        'capacity': 1.0,
        'height': 70,
        'turbine': 'Siemens SWT 2.3 82',
        'format': 'json',
        'raw':True
    }
    r = s.get(api_base + 'data/wind', params=args)
    parsed_response = json.loads(r.text)
    data1.append(pd.read_json(json.dumps(parsed_response['data']), orient='index'))
data_smola = pd.concat(data1)


# Save to excel file for easier use next time:
data_smola.to_excel(file_timeseries)

print("Done")
