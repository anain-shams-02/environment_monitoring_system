import sqlite3
import json

class SQLiteStorage:
    def __init__(self, db_name="environmental.db"):
        self.db_file = db_name
        self.connection = sqlite3.connect(self.db_file)

        self.make_tables()
        print("Connected to sqlite database: " + self.db_file)
    
    def make_tables(self):
        cur = self.connection.cursor()
        
        cur.execute("CREATE TABLE IF NOT EXISTS temperature (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, building TEXT, room TEXT, value REAL, sensor_id TEXT)")
        
        cur.execute("CREATE TABLE IF NOT EXISTS humidity (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, building TEXT, room TEXT, value REAL, sensor_id TEXT)")

        cur.execute("CREATE TABLE IF NOT EXISTS airquality (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, building TEXT, room TEXT, value INTEGER, sensor_id TEXT)")
        
        self.connection.commit()
    
    def save_reading(self, topic, payload):
        data_dict = json.loads(payload)
        c = self.connection.cursor()
        
        if topic.find('temperature') != -1:
            t = data_dict['timestamp']
            b = data_dict['building']
            r = data_dict['room']
            v = data_dict['value']
            sid = data_dict['sensor_id']
            c.execute("INSERT INTO temperature (timestamp, building, room, value, sensor_id) VALUES (?, ?, ?, ?, ?)", (t, b, r, v, sid))
            
        if topic.find('humidity') != -1:
            t = data_dict['timestamp']
            b = data_dict['building']
            r = data_dict['room']
            v = data_dict['value']
            sid = data_dict['sensor_id']
            c.execute("INSERT INTO humidity (timestamp, building, room, value, sensor_id) VALUES (?, ?, ?, ?, ?)", (t, b, r, v, sid))
            
        if topic.find('airquality') != -1:
            t = data_dict['timestamp']
            b = data_dict['building']
            r = data_dict['room']
            v = data_dict['value']
            sid = data_dict['sensor_id']
            c.execute("INSERT INTO airquality (timestamp, building, room, value, sensor_id) VALUES (?, ?, ?, ?, ?)", (t, b, r, v, sid))

        self.connection.commit()
    
    def close(self):
        self.connection.close()
        print("db closed")