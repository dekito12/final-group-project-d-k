#Dekow & Kasim
#CNE3340
#final group project
#03/21/23
#Nikita helped us with part of the project
import requests
import json
import pymysql
import matplotlib.pyplot as plt

# Set up connection to MySQL database
def connect_to_sql():
    mydb = pymysql.connect(user='root', password='root',
                           host='127.0.0.1', port=8889,
                           database='soccer')
    return mydb

# Insert variables
insert_from_reg_no = 0
insert_to_reg_no = 700
count_reg_ok = 0

# Create cursor to execute SQL queries
mydb = connect_to_sql()
mycursor = mydb.cursor()


# Check if table exists
mycursor.execute("SHOW TABLES LIKE 'soccer_data'")
result = mycursor.fetchone()

# If table does not exist, create it
if not result:
    mycursor.execute(
        "CREATE TABLE soccer_data (id INT PRIMARY KEY, area_id INT, area_name VARCHAR(255), area_code VARCHAR(255), area_flag VARCHAR(255), competition_id INT, competition_name VARCHAR(255), competition_code VARCHAR(255), competition_type VARCHAR(255), competition_emblem VARCHAR(255), season_id INT, season_startDate VARCHAR(255), season_endDate VARCHAR(255), season_currentMatchday INT, season_winner VARCHAR(255), utcDate VARCHAR(255), status VARCHAR(255), matchday INT, stage VARCHAR(255), group_name VARCHAR(255), lastUpdated VARCHAR(255), homeTeam_id INT, homeTeam_name VARCHAR(255), homeTeam_shortName VARCHAR(255), homeTeam_tla VARCHAR(255), homeTeam_crest VARCHAR(255), awayTeam_id INT, awayTeam_name VARCHAR(255), awayTeam_shortName VARCHAR(255), awayTeam_tla VARCHAR(255), awayTeam_crest VARCHAR(255), winner VARCHAR(255), duration VARCHAR(255), fullTime_home INT, fullTime_away INT, halfTime_home INT, halfTime_away INT, odds_msg VARCHAR(255), referee_id INT, referee_name VARCHAR(255), referee_type VARCHAR(255), referee_nationality VARCHAR(255))")
    print("Table created successfully.")
else:
    print("Table already exists.")

# Check if a record already exists in the soccer_data table
def check_record_exists(mycursor, record_id):
    sql = "SELECT * FROM soccer_data WHERE id = %s"
    mycursor.execute(sql, (record_id,))
    result = mycursor.fetchone()
    if result:
        print(f"ID:{record_id} record already exists")
        return True
    return False


# Fetch data from API
uri = 'https://api.football-data.org/v4/competitions/CL/matches'
headers = {'X-Auth-Token': '387fc0a4f73e46daa59ef542171eb50f'}

response = requests.get(uri, headers=headers)
matches = response.json()['matches']

# Insert data into table
for match in matches[insert_from_reg_no:insert_to_reg_no]:
    if match['awayTeam']['name'] is not None:
        awayTeamName = match['awayTeam']['name'].encode('ascii', 'ignore').decode('ascii')
    else:
        awayTeamName = None

    if match['homeTeam']['name'] is not None:
        homeTeamName = match['homeTeam']['name'].encode('ascii', 'ignore').decode('ascii')
    else:
        homeTeamName = None
    referee_id = None
    referee_name = None
    referee_type = None
    referee_nationality = None
    if match['referees']:
        referee_id = match['referees'][0]['id']
        referee_name = match['referees'][0]['name'].encode('ascii', 'ignore').decode('ascii')
        referee_type = match['referees'][0]['type']
        referee_nationality = match['referees'][0]['nationality']

    if check_record_exists(mycursor, match['id']):
        # If the record already exists, do not insert the new record.
        pass
    else:
        sql = "INSERT INTO soccer_data (id, area_id, area_name, area_code, area_flag, competition_id, competition_name, competition_code, competition_type, competition_emblem, season_id, season_startDate, season_endDate, season_currentMatchday, season_winner, utcDate, status, matchday, stage, group_name, lastUpdated, homeTeam_id, homeTeam_name, homeTeam_shortName, homeTeam_tla, homeTeam_crest, awayTeam_id, awayTeam_name, awayTeam_shortName, awayTeam_tla, awayTeam_crest, winner, duration, fullTime_home, fullTime_away, halfTime_home, halfTime_away, referee_id, referee_name, referee_type, referee_nationality) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (
            match['id'],
            match['area']['id'],
            match['area']['name'],
            match['area']['code'],
            match['area']['flag'],
            match['competition']['id'],
            match['competition']['name'],
            match['competition']['code'],
            match['competition']['type'],
            match['competition']['emblem'],
            match['season']['id'],
            match['season']['startDate'],
            match['season']['endDate'],
            match['season']['currentMatchday'],
            match['season']['winner'],
            match['utcDate'],
            match['status'],
            match['matchday'],
            match['stage'],
            match['group'],
            match['lastUpdated'],
            match['homeTeam']['id'],
            homeTeamName,
            match['homeTeam']['shortName'].encode('ascii', 'ignore').decode('ascii') if match['homeTeam'][
                                                                                            'shortName'] is not None else None,
            match['homeTeam']['tla'],
            match['homeTeam']['crest'],
            match['awayTeam']['id'],
            awayTeamName,
            match['awayTeam']['shortName'].encode('ascii', 'ignore').decode('ascii') if match['awayTeam'][
                                                                                            'shortName'] is not None else None,
            match['awayTeam']['tla'],
            match['awayTeam']['crest'],
            match['score']['winner'],
            match['score']['duration'],
            match['score']['fullTime']['home'],
            match['score']['fullTime']['away'],
            match['score']['halfTime']['home'],
            match['score']['halfTime']['away'],
            referee_id,
            referee_name,
            referee_type,
            referee_nationality
        )
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            count_reg_ok = count_reg_ok + 1
        except pymysql.Error as e:
            print('Error message:', e.args[1])
            print('SQL:', mycursor._last_executed)
            print(val)

print(count_reg_ok, " New reg inserted")
print("all processor records.")
print("--------------")
print("The last 20 records")

# Select the last 20 records inserted
mycursor.execute("SELECT * FROM soccer_data ORDER BY id DESC LIMIT 20")
result = mycursor.fetchall()

# Display the result in a table format
print("{:<10} {:<15} {:<25} {:<25} {:<15} {:<25} {:<25}".format(
    "ID", "Competition", "Home Team", "Away Team", "Score", "Status", "UTC Date"))
print("-" * 125)
for row in result:
    home_team_name = row[22] if row[22] is not None else "N/A"
    away_team_name = row[27] if row[27] is not None else "N/A"
    score = f"{row[32]}-{row[33]}" if row[32] is not None and row[33] is not None else "N/A"
    status = row[16] if row[16] is not None else "N/A"
    utc_date = row[14] if row[14] is not None else "N/A"
    print("{:<10} {:<15} {:<25} {:<25} {:<15} {:<25} {:<25}".format(
        row[0], row[6], home_team_name, away_team_name, score, status, utc_date))
print("--------------")


import matplotlib.pyplot as plt

# Count the number of matches played by each team
mycursor.execute("SELECT homeTeam_name, COUNT(*) FROM soccer_data GROUP BY homeTeam_name")
result = mycursor.fetchall()

# Extract the team names and match counts from the result
teams = [row[0] if row[0] is not None else 'Unknown' for row in result]

matches_played = [row[1] for row in result]

# Create a bar chart of the number of matches played by each team
fig, ax = plt.subplots()
ax.bar(teams, matches_played)

# Adjust the font size and spacing of the x-axis labels
ax.set_xticks(range(len(teams)))
ax.set_xticklabels(teams, rotation=90, fontsize=6, ha='center')
plt.subplots_adjust(bottom=0.3)

plt.xlabel("Team")
plt.ylabel("Number of matches played")
plt.title("Number of matches played by each team")
plt.show()
