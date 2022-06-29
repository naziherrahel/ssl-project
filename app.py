from glob import glob
from urllib import response
from flask import Flask, render_template
import requests
import json
import urllib
import pandas as pd
import numpy as np


app = Flask(__name__)


@app.route('/', methods= ['GET'])
def get_data():
    # the API i got it from a website under the name : back4app.com

    url = 'https://parseapi.back4app.com/classes/City?limit=50&order=location,name&keys=name,location'
    headers = {
      'X-Parse-Application-Id': '8OqPs7Pwqv4IGArgwPU8NNR2DXazz13rfoLGqPQw', 
      'X-Parse-Master-Key': 'mU0TGsffKRtfn6l9KvLcPykabYyZwnywFUCyGtYv' 
     }
    data = json.loads(requests.get(url, headers=headers).content.decode('utf-8')) # Here you have the data that you need

    # printing our data in a json format, to have a look at our data 
    print(json.dumps(data, indent=2))

    dataframe= pd.read_json(data)
    return render_template('index.html', data=data)
# df = pd.read_json(data)
# df.to_csv (r'C:\\Users\\nazih\Desktop\\test\\templates\\data_csv', index = None)

if __name__ == '__name__':
    app.run()