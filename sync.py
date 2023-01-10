import json 
import sqlite3
import pprint

#EXAMPLE QUERY
#INSERT INTO mysql_table (column1, column2, …) VALUES (value1, value2, …);

#Connect to db
conn = sqlite3.connect('climbersapp.db')

#Load json data
expeditionsJSON = json.load(open('expeditions.json')) # open stream

climbers = []
mountains = []
expeditions = []
expeditionClimberIDs = []

def checkData() -> str:
    if climbers:
        insertClimbers()
    else:
        print('Failed to find data')

def insertClimbers():
    stmt = "INSERT OR REPLACE INTO climbers (id, first_name, last_name, nationality, date_of_birth) VALUES (?,?,?,?,?)"
    cursorExe(stmt, climbers)
    print('Climbers succesfully inserted !')

def insertMountains():
    res = tuple(set(mountains))
    stmt = "INSERT OR REPLACE INTO mountains (name, country, rank, height, prominence, range) VALUES (?,?,?,?,?,?)"
    cursorExe(stmt, res)
    print('Mountains succesfully inserted !')
    
def insertExpeditions():
    stmt = "INSERT OR REPLACE INTO expeditions (id, name, mountain_id, start_location, date, country, duration, success) VALUES (?,?,?,?,?,?,?,?)"
    cursorExe(stmt, expeditions)
    print('Expeditions succesfully inserted !')
    
def insertRelations():
    stmt = "INSERT OR REPLACE INTO expedition_climbers (climber_id, expedition_id) VALUES (?,?)"
    cursorExe(stmt, expeditionClimberIDs)
    print('Expeditions succesfully inserted !')
    
def cursorExe(stmt :str, data :tuple) -> None:
    conn = sqlite3.connect('climbersapp.db')
    cursor = conn.cursor()
    cursor.executemany(stmt, data)
    conn.commit()
    #close connection to be more secure
    conn.close()

def exportData() -> None:
    #prepare data for inserting
    for data in expeditionsJSON:
        # print(data['id']) id, name, country, rank, height, prominence
        # collectCountries = ''
        # if len(data['mountain']['countries']) > 0:
        #     for x in data['mountain']['countries']:
        #         collectCountries += ' ' + x
        # else:
        #     collectCountries += '' + data['mountain']['countries'][0]
        #Step 1: Climbers
        for x in data['climbers']:
            #query to insert climbers;
            climber = (x['id'],x['first_name'],x['last_name'],x['nationality'],x['date_of_birth'])
            IDs = (x['id'],data['id'])
            #append to list
            climbers.append(climber)
            expeditionClimberIDs.append(IDs)
            insertClimbers()
        #get mountain ID's
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM mountains WHERE name = '{data['mountain']['name']}'")
        fetchID = cursor.fetchone()
        conn.commit()
        

            
        
        
        mountain = (data['mountain']['name'], 
                    data['mountain']['countries'][0],data['mountain']['rank'],
                    data['mountain']['height'], data['mountain']['prominence'], data['mountain']['range'])
        
        expedition = (data['id'], data['name'], fetchID[0], 
                    data['start'], data['date'], 
                    data['country'], data['duration'], data['success'])
        
        #append data to lists
        mountains.append(mountain)
        expeditions.append(expedition)
        

            
            

# print(res)
print(expeditionClimberIDs)
        
#check data before running insert

    

exportData()

insertExpeditions()
insertMountains()
insertClimbers()
insertRelations()


# # checkData()
