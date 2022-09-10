import os

from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql import Window, DataFrame
from common import init_spark
from tqdm import tqdm
from FlightRadar24.api import FlightRadar24API
import time
import json


#get flight details
def get_flight_details(flights, fr_api):
    flights_details = []
    keys = []
    #get details of each flight
    for f in tqdm(flights):
        details = fr_api.get_flight_details(f.id)
        time.sleep(0.5)
        f.set_flight_details(details)
        keys = list(f.__dict__.keys())
        values = []
        
        for _,v in f.__dict__.items():
            values.append(v)
        res_dct = dict(zip(keys, values))
        flights_details.append(res_dct)
        
    return flights_details

def save_original_file(file_content, file_path):
    res = json.dumps(file_content)
    with open(file_path, 'w') as f:
        f.write(res)
        f.close()


