from flask import Flask, render_template, jsonify, request
import requests
import json
import pandas as pd
import numpy as np
import random
import psycopg2 as ps  
import os


# lists to store specific data.
csvdata=[]
df1= []
df3=[]


app = Flask(__name__)

@app.route('/',methods= ['GET'])
def get_data():
    
    # the API i got it from a website under the name : back4app.com
    url = 'https://parseapi.back4app.com/classes/City?limit=50&order=location,name&keys=name,location'
    headers = {
      'X-Parse-Application-Id': '8OqPs7Pwqv4IGArgwPU8NNR2DXazz13rfoLGqPQw', 
      'X-Parse-Master-Key': 'mU0TGsffKRtfn6l9KvLcPykabYyZwnywFUCyGtYv' 
     }
    data = json.loads(requests.get(url, headers=headers).content.decode('utf-8')) # Here you have the data that you need
    
    # catching the lat and long variables from the json file 
    
    df_lat=[]
    df_long=[]

    for x in data['results']:
        name = {'city':x['name']}
        location = [x['location']]
        for y in location:
            latitude = {'lat': y['latitude']}
            longitude = {'long':y['longitude']}

            csvdata.append(name)
            df_lat.append(latitude)
            df_long.append(longitude)

#     # converting lat and long from list to pandas dataframe
    df_lat= pd.DataFrame(df_lat)
    df_long= pd.DataFrame(df_long)
    
#     # appending needed variables from json data to a list 
    for key, val in data.items():
       df1.append(val)
    for mylist in df1:
        for i in mylist:
            df3.append(i)
            

#     # converting the list to a pandas dataframe 
    df4= pd.DataFrame(df3) 
    
#     # manipulating our dataframe 
    dataframe= pd.DataFrame(['name', 'lat', 'long'])
    # creating a copy for our dataframe
    dfcopy=df4.copy()
    # deleting some unnecessary columns 
    result= pd.concat([dfcopy, df_lat, df_long], axis=1, join= 'inner')
    del result['location']
    del result['objectId']
    del result['createdAt']
    del result['updatedAt']

    # adding a column with random radius 
    
    result['radius'] = result.apply(lambda row : 0.001* random.randint(25,50), axis = 1)
    result['id']= pd.factorize(result.lat+result.long)[0]
    result = result[ ['id'] + [ col for col in result.columns if col != 'id' ] ]


        
# printing our final result which is our data in a csv format 

    print(result)


# saving our final dataframe results to a csv file 

    result.to_csv(r'C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\data_csv', index=False)  # here you need to use your own path and give your file a name for ex 'data_csv'


# megrating out data to the database
      

      #processing data
    replacements = {
         'int64': 'varchar',
         'object': 'varchar',
         'float64': 'float',
         'float64': 'float',
         'float64': 'varchar'
      }
 
            
    col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(result.columns, result.dtypes.replace(replacements)))
    
    # open a database connection
    host_name= 'localhost' # check your own host name
    dbname= 'project-ssl' # write your own dbname 
    port= '5432'          # check your port from your database 
    username= 'postgres'  # writr your user name
    password= '*********' # here write your own db password 
    conn = None
     
    # a function to automaticly connect to the database

    def connect_to_db(host_name, dbname, port, username, password):
       try:
        conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)

       except ps.OperationalError as e:
        raise e
       else:
         print('Connected!')
         return conn

    
    conn = connect_to_db(host_name, dbname, port, username, password)
    cursor = conn.cursor()  

    # drop tables with the same name 
    cursor.execute("drop table if exists cities")

    # create table 

    cursor.execute("create table cities (id varchar, name varchar, lat varchar, long varchar, radius varchar)")

    
    # open the csv file, save it in an object 

    my_file= open('C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\data_csv.csv')


    # upload to db
    SQL_STATEMENT = """
    COPY cities FROM STDIN WITH 
        CSV
        HEADER
        DELIMITER AS ','
    """

    cursor.copy_expert(sql= SQL_STATEMENT, file= my_file)
    print("file copied to db") 
    
    conn.commit()
    conn.close()
   
    return render_template('index.html', result=result)











if __name__ == '__name__':
    app.run()
