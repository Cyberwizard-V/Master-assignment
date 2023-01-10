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

#insert data to tables
for data in expeditionsJSON:
    # print(data['id'])
    # print(data['mountain'])
    for x in data['climbers']:
        #query to insert climbers;
        climber = (x['id'],x['first_name'],x['last_name'],x['nationality'],x['date_of_birth'])
        climbers.append(climber)
        

def checkData():
    #check data before running insert
    if climbers:
        insertClimbers()
    else:
        print('Failed to find data')

def insertClimbers():
    stmt = "INSERT OR REPLACE INTO climbers (id, first_name, last_name, nationality, date_of_birth) VALUES (?,?,?,?,?)"
    cursorExe(stmt, climbers)
    print('Climbers succesfully inserted !')
    
    
def cursorExe(stmt :str, data :tuple) -> None:
    cursor = conn.cursor()
    cursor.executemany(stmt, data)
    conn.commit()
    conn.close()
    
checkData()


