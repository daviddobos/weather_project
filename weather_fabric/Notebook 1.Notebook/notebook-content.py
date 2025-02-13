# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

!pip install airbase

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd

# EEA API végpont
url = "https://discomap.eea.europa.eu/map/fme/latest/index.html"

# Magyarország ISO kódja
country_code = "HU"

# API kérés paraméterei
params = {
    "country": country_code,
    "pollutant": "PM10",  # Lehet pl. "NO2", "PM2.5", "O3" stb.
    "year": 2023
}

# API hívás
response = requests.get(url, params=params)

# Ellenőrizzük, hogy sikeres volt-e a lekérés
if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data)
    print(df.head())  # Adatok megjelenítése
else:
    print("Hiba történt az adatok lekérésekor:", response.status_code)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd
from datetime import datetime, timedelta

# Tegnapi dátum kiszámítása
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# EEA API végpont
url = "https://discomap.eea.europa.eu/map/fme/latest/index.html"

# Magyarország ISO kódja
country_code = "HU"

# API kérés paraméterei
params = {
    "country": country_code,
    "pollutant": "PM10",  # Módosítható: "NO2", "PM2.5", "O3", stb.
    "date": yesterday  # Mindig a tegnapi nap adatait kéri le
}

# API hívás
response = requests.get(url, params=params)

# Ellenőrizzük, hogy sikeres volt-e a lekérés
if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data)
    df.show()

else:
    print("Hiba történt az adatok lekérésekor:", response.status_code)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

base_url = "https://api.waqi.info"

# read secret token from file containing the token as a single string
# you need to create this file if you want to reproduce this analysis
token = open('.waqitoken').read()

city = 'Paris'
r = requests.get(base_url + f"/feed/{city}/?token={token}")
"City: {}, Air quality index: {}".format(r.json()['data']['city']['name'], r.json()['data']['aqi'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
