import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
from OneMinutesData import OneMinutesData

class CryptowatchDataGetter:
    @classmethod
    def check_csv_data(cls):
        print('f')

    @classmethod
    def get_data_from_crptowatch(cls, before, after):
        url = 'https://api.cryptowat.ch/markets/bitflyer/btcfxjpy/ohlc'
        query = {
            'periods': 60,
            'before': before,
            'after': after,
        }
        res = requests.get(url, params = query).json()['result']['60']
        return res

    @classmethod
    def convert_json_to_ohlc(cls, json_data):
        omd = OneMinutesData()
        omd.initialize()
        


if __name__ == '__main__':
    CryptowatchDataGetter.get_data_from_crptowatch()