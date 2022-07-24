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
import math
import datetime
from barnum import gen_data # this library used to generate random string, use pip to install barnum
import folium


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
    dataframe= pd.DataFrame(['Names', 'Lat', 'Long'])
    # creating a copy for our dataframe
    dfcopy=df4.copy()
    # deleting some unnecessary columns 
    result= pd.concat([dfcopy, df_lat, df_long], axis=1, join= 'inner')
    del result['location']
    del result['objectId']
    del result['createdAt']
    del result['updatedAt']

    # adding a column with random radius 
    
    result['Radius'] = result.apply(lambda row : 0.0001* random.randint(20,25), axis = 1)
    result['ID']= pd.factorize(result.lat+result.long)[0]
    result = result[ ['ID'] + [ col for col in result.columns if col != 'ID' ] ]




# saving our final dataframe results(cities, lat, lng, radius) to a csv file 

    result.to_csv(r'C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\cities.csv', index=False)  # here you need to use your own path and give your file a name for ex 'data_csv'

# generating user data( email..) using RandomUser API
    
    # Version of the random user API
    API_VERSION = '1.3' 
    email_list = []
    url1 = 'https://randomuser.me/api/?results=5000'
    user_data = json.loads(requests.get(url1).content.decode('utf-8'))
    for x in user_data['results']:
        email = {'Emails': x['email']}
        email_list.append(email)
    

    # creating a dataframe 
    email_csv= pd.DataFrame(email_list)

    # saving our final dataframe results(cities, lat, lng, radius) to a csv file 

    email_csv.to_csv(r'C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\emails.csv', index=False)  # here you need to use your own path and give your file a name for ex 'data_csv'
 
    #  generation of random latitude and longitude for each user

    data = pd.read_csv('C:\\Users\\nazih\Desktop\\ssl-project1\\templates\\cities.csv')

    lat_res=[]
    lng_res = []

    # Equation for testing if a point is inside a circle

    for index in range(len(data)):
        random.seed(5)
        for i in range(0,100):
                center_x =data.iloc[index,2]
                center_y = data.iloc[index,3]
                x = np.random.uniform(data.iloc[index,2]+ data.iloc[index,4], data.iloc[index,2]-data.iloc[index,4])
                y = np.random.uniform(data.iloc[index,3] + data.iloc[index,4], data.iloc[index,3]-data.iloc[index,4])
                radius= data.iloc[index,4]
                square_dist = (center_x - x) ** 2 + (center_y - y) ** 2 
                if (square_dist <= radius ** 2):
                            lat_res.append(x)
                            lng_res.append(y)
                while (square_dist <= radius ** 2) != True:
                  center_x =data.iloc[index,2]
                  center_y = data.iloc[index,3]
                  x = np.random.uniform(data.iloc[index,2]+ data.iloc[index,4], data.iloc[index,2]-data.iloc[index,4])
                  y = np.random.uniform(data.iloc[index,3] + data.iloc[index,4], data.iloc[index,3]-data.iloc[index,4])
                  radius= data.iloc[index,4]
                  square_dist = (center_x - x) ** 2 + (center_y - y) ** 2 
                  if (square_dist <= radius ** 2):
                            lat_res.append(x)
                            lng_res.append(y)
    
    # converting our data to a pandas df
    lng_result= pd.DataFrame(lng_res, columns= ['Lng'])
    lat_result= pd.DataFrame(lat_res, columns= ['Lat'])


     
    # ************************************************************************************
    # creating a new coulmn with the name city , and append city name from data file, acording to the lat and long value

    var= []
    var1= []
    for index in range(len(data)):
       for ind in range(100):
           var.append(np.random.uniform(size=ind, high = data.iloc[index,2]+ data.iloc[index,4], low= data.iloc[index,2]-data.iloc[index,4]))
           var1.append(data.iloc[index,1])


    var_csv= pd.DataFrame(var)
    var1_csv= pd.DataFrame(var1,columns=['Cities']) 
    
        
    # concatinate our three data frame 
    users_csv= pd.concat([lat_result, lng_result,var1_csv], axis=1)
   
    # uploading our user file with just emails in it 

    users_data = pd.read_csv('C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\emails.csv')
   
    # concatinate our previous df to the email which is inside the user_csv file 
    users= pd.concat([users_data,users_csv], axis=1)
    # generating Unique Id 
    users['ID']= pd.factorize(users.Emails)[0]

    # saving our final result csv file in this format {email, lat, lng , city}
    users.to_csv(r'C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\users.csv', index=False)
    
    # Generating users tasks for each city. each user will have have tasks in the ranges of [2,10].
    lat = []
    lng= []
    cities=[]
    names=[]
    for index in range(len(data)):
     random.seed(3)
     for i in range(random.randrange(200,1100)):
                center_x =data.iloc[index,2]
                center_y = data.iloc[index,3]
                x = np.random.uniform(data.iloc[index,2]+ data.iloc[index,4], data.iloc[index,2]-data.iloc[index,4])
                y = np.random.uniform(data.iloc[index,3] + data.iloc[index,4], data.iloc[index,3]-data.iloc[index,4])
                radius= data.iloc[index,4]
                square_dist = (center_x - x) ** 2 + (center_y - y) ** 2 
                if square_dist <= radius ** 2 and x not in lat_res and y not in lng_res:
                            lat.append(x)
                            lng.append(y)
                            cities.append(data.iloc[index,1])
                while (square_dist <= radius ** 2 and x not in lat_res and y not in lng_res) != True:
                  center_x =data.iloc[index,2]
                  center_y = data.iloc[index,3]
                  x = np.random.uniform(data.iloc[index,2]+ data.iloc[index,4], data.iloc[index,2]-data.iloc[index,4])
                  y = np.random.uniform(data.iloc[index,3] + data.iloc[index,4], data.iloc[index,3]-data.iloc[index,4])
                  radius= data.iloc[index,4]
                  square_dist = (center_x - x) ** 2 + (center_y - y) ** 2 
                  if square_dist <= radius ** 2 and x not in lat_res and y not in lng_res:
                            lat.append(x)
                            lng.append(y)
                            cities.append(data.iloc[index,1])

    # generating random strings to names our tasks 
    
    
    for i in range(len(lat)):
        names.append(gen_data.create_company_name(biz_type="Generic"))

    lng_tasks= pd.DataFrame(lng, columns= ['Lng'])
    lat_tasks= pd.DataFrame(lat, columns= ['Lat'])
    cities_tasks= pd.DataFrame(cities, columns= ['Cities'])
    names_tasks= pd.DataFrame(names, columns= ['Names'])
    tasks= pd.concat([cities_tasks,lat_tasks, lng_tasks, names_tasks], axis=1)

    
    # generating an tasks id 

    df_cities = pd.read_csv('C:/Users/nazih/Desktop/ssl-project1/templates/cities.csv')
    id =[]
    var = 0
    for idx in range(len(df_cities)): 
         list = [x+var for x in range(0,100)]
         i = list[0]
         for index in range(len(tasks)):  
             if df_cities.iloc[idx,1] == tasks.iloc[index,0]:
                 id.append(i)
                 i=i+1  
                 if i == list[-1]:
                     i = list[0]       
      
         var =var+100   
    
    #  converting the id list to a pd_df
    df_id = pd.DataFrame(id,columns=['Unique_ID']) 
    tasks['users_id']= df_id
    # generating tasks id 
    tasks['tasks_id']= pd.factorize(tasks.Lat + tasks.Lng)[0]

     # saving our results to csv file
    tasks.to_csv(r'C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\tasks.csv', index=False)

    # creating a users_tasks table 
    tasks= pd.read_csv('C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\tasks.csv')
    
    users_tasks = pd.DataFrame()

    users_tasks['users_id']= tasks['users_id']
    users_tasks['tasks_id']=tasks['tasks_id']
    users_tasks['time_start'] = pd.DataFrame({'time_start': pd.date_range(start='1-1-2022', periods=22150, freq='H')})
    users_tasks['time_end'] = users_tasks['time_start'].apply(lambda x: x + datetime.timedelta())
    
    # saving our users_tasks table 
    users_tasks.to_csv(r'C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\users_tasks.csv', index=False)

    # ploting our cordination 
    @app.route('/map')
    def well_map():
         # Make an empty map
        map = folium.Map(location=[66.32166	,-179.12198], tiles="OpenStreetMap", zoom_start=14, prefer_canvas=True)
        for i in range(len(users)):
             if users.iloc[i,4]==0:
                city = users.iloc[i,3] 
        for x in range(len(data)):
              if data.iloc[x,1]==city:
                    r= data.iloc[x,4]
                    lat= data.iloc[x,2]
                    lng = data.iloc[x,3]
                    name = data.iloc[x,1]
        folium.Circle(
            location=[lat, lng],
            popup=name,
            radius=float(r),
            color='crimson',
            fill=True,
            fill_color='crimson'
            ).add_to(map)

        for i in range(len(users)):
           if users.iloc[i,4]==0:
               lat1= users.iloc[i,1]
               lng1 = users.iloc[i,2]
               email1 = users.iloc[i,0]
        folium.Marker(
              location=[lat1, lng1],
              popup=email1,
              ).add_to(map)  

        lat2=[]
        lng2=[]
        name2=[]
        for y in range(len(tasks)):
           if tasks.iloc[y,4]==0 :
               lat2.append(tasks.iloc[y,1])
               lng2.append(tasks.iloc[y,2])
               name2.append(tasks.iloc[y,3])
        for idx in range(len(lat2)):
           folium.Marker(
              location=[lat2[idx], lng2[idx]],
              popup=name[idx],
              icon = folium.Icon(color='green'),
           ).add_to(map)  

        return map._repr_html_()
      
            #  ***********************************************************************************************
  
#  megrating out data to the database
      

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
    password= '**********' # here write your own db password 
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
    cursor.execute('drop table if exists users')
    cursor.execute('drop table if exists tasks')
    cursor.execute('drop table if exists users_tasks')
    # create table cities using execute methode

    cursor.execute("create table Cities (ID varchar, Names varchar, Lat double precision, Lng double precision, Radius varchar)")
    cursor.execute('create table Users(Emails varchar, Lat double precision, Lng double precision, Cities varchar, ID varchar)')
    cursor.execute('create table Tasks(Cities varchar, Lat double precision, Lng double precision, Names varchar, users_id varchar, tasks_id varchar)')
    cursor.execute('create table users_Tasks(users_id varchar, tasks_id varchar, time_start timestamp , time_end timestamp )')

    # open the csv file, save it in an object 

    cities_file= open('C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\cities.csv')
    users_file= open('C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\users.csv')
    tasks_file= open('C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\tasks.csv')
    users_tasks_file= open('C:\\Users\\nazih\\Desktop\\ssl-project1\\templates\\users_tasks.csv')
    # upload to db
    SQL_STATEMENT = """
    COPY Cities FROM STDIN WITH 
        CSV
        HEADER
        DELIMITER AS ','
    """
    SQL_STATEMENT_1 = """
    COPY Users FROM STDIN WITH 
        CSV
        HEADER
        DELIMITER AS ','
    """
    SQL_STATEMENT_2 = """
    COPY Tasks FROM STDIN WITH 
        CSV
        HEADER
        DELIMITER AS ','
    """
    SQL_STATEMENT_3 = """
    COPY users_Tasks FROM STDIN WITH 
        CSV
        HEADER
        DELIMITER AS ','
    """
    cursor.copy_expert(sql= SQL_STATEMENT, file= cities_file)
    print("cities file copied to db") 
    cursor.copy_expert(sql= SQL_STATEMENT_1, file= users_file)
    print("users file copied to db") 
    cursor.copy_expert(sql= SQL_STATEMENT_2, file= tasks_file)
    print("Tasks file copied to db") 
    cursor.copy_expert(sql= SQL_STATEMENT_3, file= users_tasks_file)
    print("users_Tasks file copied to db")
    
    conn.commit()
    conn.close()
   
    return render_template('index.html', users_tasks= users_tasks )





if __name__ == '__name__':
    app.run()
