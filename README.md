# se4sci-dataWareHouse



# Aim of Project
The purpose of this project is to use the song and event datasets available in S3, to create a star schema optimized for queries on song play analysis using the following fact and dimension tables.

### **Fact Table**

1. **songplays** - records in event data associated with song plays i.e. records with page `NextSong`
    - *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

### **Dimension Tables**

1. **users** - users in the app
    - *user_id, first_name, last_name, gender, level*
2. **songs** - songs in music database
    - *song_id, title, artist_id, year, duration*
3. **artists** - artists in music database
    - *artist_id, name, location, lattitude, longitude*
4. **time** - timestamps of records in **songplays** broken down into specific units
    - *start_time, hour, day, week, month, year, weekday*


# How to Run the project

## Setup
* The project requires a running AWS Redshift cluster with connection and user account details placed in a file called `dwh.cfg` in the root directory

* Install pipEnv: `brew upgrade pipenv`
* Install all project dependencies from pipFile.lock: `pipenv synch`

## Project File Structure
* `README.md`: This file describing the project and how to run it
* `create_load_tables.py`: Creates all tables necessary for this project and loads the dataset inside the staging tables
* `etl.py`: Extracts data in the staging tables into the star schema
* `Database Debug.ipynb`: Useful jupyter notebook for querying the DBs and displaying results
* `requirements.txt`: Requirement file containing all necessary project dependencies
* `dwh.cfg`: contains all configurations necessary to run the project

## Run the project
* To run the project simply run the 2 following commands:
```
python create_load_tables.py
python etl.py
```
* The scripts will create staging, fact and dimention tables. They will then load the dataset from S3 to the staging tables and finally extract this data to transform it into a star schema with fact and dimension tables. The script will finally provide a list of each table with the number of entries they contain.

## Useful SQL Queries to run
`SELECT user_id, COUNT(*) as count FROM user_table GROUP BY user_id ORDER BY countcl DESC;`

`SELECT user_id, first_name, last_name FROM user_table GROUP BY user_id, first_name, last_name ORDER BY user_id DESC;`

`SELECT artist_id, artist_name, location FROM artist_table GROUP BY artist_id, artist_name, location ORDER BY artist_id DESC;`