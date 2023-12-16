import sqlite3
import requests
from tqdm import tqdm
from flask import Flask, request
import json 
import numpy as np
import pandas as pd

app = Flask(__name__) 

@app.route('/')
def home():
    return 'Hello World'

################ Start Stations ##############

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/stations/<station_id>') #rute dinamis
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()


################ End Point Stations ##############

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result


@app.route('/json', methods = ['POST']) 
def json_example():
    
    req = request.get_json(force=True) # Parse the incoming json data as Dictionary
    
    name = req['name']
    age = req['age']
    address = req['address']
    
    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

################ Function #################

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

#mendefine insert into station
def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'


################ Start Trips ##############

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

@app.route('/trips/<trip_id>') #rute dinamis
def route_trip_id(trip_id):
    conn = make_connection()
    trip = get_trip_id(trip_id, conn)
    return trip.to_json()

#post trip
@app.route('/trips/add', methods=['POST']) 
def route_add_trips():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except Exception as e:
        return f"""ERROR : {e}"""
    conn.commit()
    return 'OK'


################ Function #################

def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

################ End Point Trips ##############

# ANALYTICS
# 1.
@app.route('/trips/max_endstation')
def get_maxstation():
    conn = make_connection()
    query = f"""SELECT count(1) AS jumlah , end_station_id, end_station_name 
                FROM trips 
                GROUP BY end_station_id 
                ORDER BY count(1) 
                DESC 
                LIMIT 5  """
    result = pd.read_sql_query(query, conn)
    return result.to_json()

#2
@app.route('/trips/bike_use/<bike_id>')
def get_bike_use(bike_id):
    conn = make_connection()
    query = f"""SELECT count(1) as jumlah , bikeid 
                FROM trips 
                WHERE bikeid = {bike_id}  
                """
    result = pd.read_sql_query(query, conn)
    return result.to_json()

#3 POST

#GET DATA
def get_trayek():
    conn = make_connection()
    query = f"""SELECT jumlah, count(jumlah) AS total 
                FROM (SELECT count(1) as jumlah , start_station_name, end_station_name from trips GROUP BY start_station_id, end_station_id) 
                GROUP BY jumlah 
                ORDER BY jumlah ASC 
                LIMIT 3  
                """
    result = pd.read_sql_query(query, conn)
    return result.to_json()

#POST & GET DATA
@app.route('/trips/add_trayek', methods=['POST']) 
def route_add_andgettrayek():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    req = request.get_json(force=True)

    conn = make_connection()
    insert_into_trips(data, conn)
    datatrayek = get_trayek()
    return datatrayek


if __name__ == '__main__':
    app.run(debug=True, port=5000)