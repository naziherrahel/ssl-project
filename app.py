from flask import Flask, render_template, jsonify, request
import requests
import json
import pandas as pd
import numpy as np
import random
import psycopg2 as ps  
import os
from urllib import request, error
from urllib.parse import urlencode
import time
import re


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
    
    result['radius'] = result.apply(lambda row : 0.0001* random.randint(20,25), axis = 1)
    result['id']= pd.factorize(result.lat+result.long)[0]
    result = result[ ['id'] + [ col for col in result.columns if col != 'id' ] ]


        
# printing our final result(cities, lat, lng, radius) which is our data in a csv format 

    # print(result)


# saving our final dataframe results(cities, lat, lng, radius) to a csv file 

    # result.to_csv(r'C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\data', index=False)  # here you need to use your own path and give your file a name for ex 'data_csv'

# generating user data( email..) using RandomUser API
    
    # Version of the random user API
    API_VERSION = '1.3' 
    email_list = []
    url1 = 'https://randomuser.me/api/?results=5000'
    user_data = json.loads(requests.get(url1).content.decode('utf-8'))
    for x in user_data['results']:
        email = {'email': x['email']}
        email_list.append(email)
    # print(email_list)

    # creating a dataframe 
    email_csv= pd.DataFrame(email_list)

    # saving our final dataframe results(cities, lat, lng, radius) to a csv file 

    # email_csv.to_csv(r'C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\users.csv', index=False)  # here you need to use your own path and give your file a name for ex 'data_csv'
 
    #  generation of random latitude and longitude for each user

    data = pd.read_csv('C:\\Users\\nazih\Desktop\\ssl-project1\\templates\\data.csv')

    # using numpy to generate 100 random lat 
    lat_res=[]
    for index in range(len(data)):
       lat_res.append(np.random.uniform(size=100, high = data.iloc[index,2]+ data.iloc[index,4], low= data.iloc[index,2]-data.iloc[index,4]))
    
    # converting our list to a pd df
    user_csv= pd.DataFrame(lat_res)
    # transpose our df
    user_csv=user_csv.T

    # naming each column with the city name 
    name= data['name']
    # print('{}'.format(name[0]))

    # a dict where to store our column heads
    userdata={}
    for index in range(len(data)):
       userdata[name[index]] = user_csv.iloc[:,index]

    # cenverting dict to pd df   
    df_lat = pd.DataFrame(userdata)
    df_lat_trans= df_lat.T
    # adding a new coulmn 'id' 
    df_lat_trans.insert(0, 'New_ID', range(0,len(df_lat_trans)))

    # converting all coulmns to one coulmn
    result_csv = pd.DataFrame({'lat': df_lat_trans.set_index(df_lat_trans.columns[0]).stack().reset_index(drop=True)})
    
    #  *********************************************************************
    
    # after creating random latitude values now time to generate longitude
    lng_res=[]

    for index in range(len(data)):
        lng_res.append(np.random.uniform(size=100, high = data.iloc[index,3] + data.iloc[index,4], low= data.iloc[index,3]-data.iloc[index,4]))
    
    lng_result= pd.DataFrame(lng_res)

    lng_csv= lng_result.T
 
    userdata1={}
    for index in range(len(data)):
        userdata1[name[index]] = lng_csv.iloc[:,index]

    df_lng = pd.DataFrame(userdata1)

    df_lng_trans=df_lng.T
    
    # adding a new id coulumn 
    df_lng_trans.insert(0, 'New_ID', range(0,len(df_lng_trans)))

    # after we creat our Id now we it's possible to create 1 coulmn from all the 50 coulmns 
    csv_lng = pd.DataFrame({'lng': df_lng_trans.set_index(df_lng_trans.columns[0]).stack().reset_index(drop=True)})
     
    # ************************************************************************************
    # creating a new coulmn with the name city , and append city name from data file, acording to the lat and long value

    var= []
    var1= []
    for index in range(len(data)):
       for ind in range(100):
           var.append(np.random.uniform(size=ind, high = data.iloc[index,2]+ data.iloc[index,4], low= data.iloc[index,2]-data.iloc[index,4]))
           var1.append(data.iloc[index,1])


    var_csv= pd.DataFrame(var)
    var1_csv= pd.DataFrame(var1,columns=['city'])       

    # concatinate our three data frame 
    users_csv= pd.concat([result_csv,csv_lng,var1_csv], axis=1)
     
    # uploading our user file 

    users_data = pd.read_csv('C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\users.csv')
   
    # concatinate our previous df to the email which is inside the user_csv file 
    users= pd.concat([users_data,users_csv], axis=1)

    # saving our final result csv file in this format {email, lat, lng , city}
    # users.to_csv(r'C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\users_data', index=False)







#              ***********************************************************************************************

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
    password= 'nazih_ali97' # here write your own db password 
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

    # drop table "cities" with the same name 
    cursor.execute("drop table if exists cities")
    cursor.execute('drop table if exists users')

    # create table cities

    cursor.execute("create table cities (id varchar, name varchar, lat double precision, lng double precision, radius varchar)")
    cursor.execute('create table users(emails varchar, lat double precision, lng double precision, city varchar)')
    
    # open the csv file, save it in an object 

    cities_file= open('C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\data.csv')
    users_file= open('C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\users_data.csv')


    # upload to db
    SQL_STATEMENT = """
    COPY cities FROM STDIN WITH 
        CSV
        HEADER
        DELIMITER AS ','
    """
    SQL_STATEMENT_1 = """
    COPY users FROM STDIN WITH 
        CSV
        HEADER
        DELIMITER AS ','
    """
    cursor.copy_expert(sql= SQL_STATEMENT, file= cities_file)
    print("cities file copied to db") 
    cursor.copy_expert(sql= SQL_STATEMENT_1, file= users_file)
    print("users file copied to db") 
    
    conn.commit()
    conn.close()
   
    return render_template('index.html', users= users)











if __name__ == '__name__':
    app.run()
