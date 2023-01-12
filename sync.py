import json 
import sqlite3
from time import time
from time import sleep

'''
## ---- SYNC SCRIPT ---- ##
This script will synchronize JSON data with the database,
ensuring that the information stored in the database is up-to-date.
'''

#Connect to db
conn = sqlite3.connect('climbersapp.db')
#Load json data
expeditionsJSON = json.load(open('expeditions.json')) # open stream

climbers = []
mountains = []
expeditions = []
expeditionClimberIDs = []

def timer_func(func):
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        print(f'(Success) Succesfully synced in {(t2-t1):.4f}s')
        return result
    return wrap_func

def insertClimbers() -> str:
    stmt = "INSERT OR REPLACE INTO climbers (id, first_name, last_name, nationality, date_of_birth) VALUES (?,?,?,?,?)"
    cursorExe(stmt, climbers)
    print('(1/2) Completed syncing Climbers!')

def insertMountains() -> str:
    res = tuple(set(mountains))
    stmt = "INSERT OR REPLACE INTO mountains (name, country, rank, height, prominence, range) VALUES (?,?,?,?,?,?)"
    cursorExe(stmt, res)
    print('(1/2) Completed syncing Mountains!')
    
def insertExpeditions() -> str:
    stmt = "INSERT OR REPLACE INTO expeditions (id, name, mountain_id, start_location, date, country, duration, success) VALUES (?,?,?,?,?,?,?,?)"
    cursorExe(stmt, expeditions)
    print('(2/2) Completed syncing Expeditions!')
    
def insertRelations() -> str:
    stmt = "INSERT OR REPLACE INTO expedition_climbers (climber_id, expedition_id) VALUES (?,?)"
    cursorExe(stmt, expeditionClimberIDs)
    print('(1/2) Completed syncing Relations!')
    
def cursorExe(stmt :str, data :tuple) -> None:
    conn = sqlite3.connect('climbersapp.db')
    cursor = conn.cursor()
    cursor.executemany(stmt, data)
    conn.commit()
    #close connection to be more secure
    conn.close()
    
def cursorSelect(stmt :str) -> None:
    conn = sqlite3.connect('climbersapp.db')
    cursor = conn.cursor()
    x = cursor.execute(stmt).fetchone()
    conn.commit()
    #close connection to be more secure
    conn.close()
    
    return x[0]

def syncData1() -> None:
    #prepare data for inserting
    for data in expeditionsJSON:
        #Step 1: Climbers
        for x in data['climbers']:
            #query to insert climbers;
            climber = (x['id'],x['first_name'],x['last_name'],x['nationality'],x['date_of_birth'])
            IDs = (x['id'],data['id'])
            #append to list
            climbers.append(climber)
            expeditionClimberIDs.append(IDs)
            
        #Step 2: Mountains
        mountain = (data['mountain']['name'], 
                    data['mountain']['countries'][0],data['mountain']['rank'],
                    data['mountain']['height'], data['mountain']['prominence'], data['mountain']['range'])
        mountains.append(mountain)
        
    insertClimbers()
    insertMountains()
    insertRelations()

def syncData2() -> None:
            #Step 3: Expeditions
    for data in expeditionsJSON:
        #get mountain ID's
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM mountains WHERE name = '{data['mountain']['name']}'")
        fetchID = cursor.fetchone()
        conn.commit()
        
        #Make tuple
        expedition = (data['id'], data['name'], fetchID[0], 
                    data['start'], data['date'], 
                    data['country'], data['duration'], data['success'])
        #append data to lists
        expeditions.append(expedition)
    insertExpeditions()

def checkandsync() -> str:
    #check if you get count rows
    mountaincheckRows = cursorSelect("SELECT COUNT(*) FROM mountains")
    climbersRows = cursorSelect("SELECT COUNT(*) FROM climbers")
    expeditionClimbersRows = cursorSelect("SELECT COUNT(*) FROM expedition_climbers")
    expeditionsRows = cursorSelect("SELECT COUNT(*) FROM expeditions")
    if mountaincheckRows and climbersRows and expeditionClimbersRows and expeditionsRows:
        print('Database: Not empty please empty before syncing')
        return 0
    else:
        @timer_func
        def sync() -> None:
            print('(1/2) Trying to sync data 1')
            #First part of syncing
            syncData1()
            #sleep when syncData1 is done
            sleep(0.1)
            print('(2/2) Trying to sync data 2')
            #Second part of syncing
            syncData2()
            return 1
        
        sync()
        
if __name__ == '__main__':
    #this will run the script
    checkandsync()
