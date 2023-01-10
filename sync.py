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

#mean putting in a list

#prepare data for inserting
for data in expeditionsJSON:
    # print(data['id']) id, name, country, rank, height, prominence
    collectCountries = ''
    if len(data['mountain']['countries']) > 0:
        for x in data['mountain']['countries']:
            collectCountries += ' ' + x
    else:
        collectCountries += '' + data['mountain']['countries'][0]
        
    mountain = (data['id'], data['mountain']['name'], 
                collectCountries,data['mountain']['rank'],
                data['mountain']['height'], data['mountain']['prominence'], data['mountain']['range'])
    mountains.append(mountain)

    for x in data['climbers']:
        #query to insert climbers;
        climber = (x['id'],x['first_name'],x['last_name'],x['nationality'],x['date_of_birth'])
        climbers.append(climber)
        
#print(collectCountries)
print(mountains)
        
#check data before running insert
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
    stmt = "INSERT OR REPLACE INTO mountains (id, name, country, rank, height, prominence, range) VALUES (?,?,?,?,?,?,?)"
    cursorExe(stmt, mountains)
    print('Mountains succesfully inserted !')
    
    
def cursorExe(stmt :str, data :tuple) -> None:
    cursor = conn.cursor()
    cursor.executemany(stmt, data)
    conn.commit()
    #close connection to be more secure
    conn.close()

insertMountains()
# checkData()


