import json
import pandas as pd
from os import environ
from time import sleep
from sqlalchemy import create_engine
from utils.utilities import calculate_distance
from sqlalchemy.exc import OperationalError

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

# Connect to PostgreSQL and MySQL databases
while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"],
                                    pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')

while True:
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"],
                                     pool_pre_ping=True, pool_size=10,
                                     isolation_level="AUTOCOMMIT")
        break
    except OperationalError:
        sleep(0.1)
print('Connection to MySQL successful.')

# Set pandas to display all columns upon print
pd.set_option('display.max_columns', None)

query = "SELECT * FROM devices"

# Read the PostgreSQL table into a pandas DataFrame
df = pd.read_sql(query, psql_engine)

# Add time_hour column in dataframe to add hour value for each record

df['time'] = pd.to_numeric(df['time'])  # Cast 'time' column to numeric type
df['time_hour'] = pd.to_datetime(df['time'], unit='s').dt.floor('H')

# PartitionBy data according to device_id and time_hour and sort by time asc

# Sort the DataFrame by 'device_id', 'time_hour', and 'time'
df = df.sort_values(by=['device_id', 'time_hour', 'time'])

# Apply row number window function partitioned by 'device_id' and 'time_hour'
df['row_number'] = df.groupby(['device_id', 'time_hour']).cumcount() + 1

# Parse the 'location' column as JSON
df['location'] = df['location'].apply(json.loads)

# Extract the latitude and longitude into separate columns
df['latitude'] = df['location'].apply(lambda loc: float(loc['latitude']))
df['longitude'] = df['location'].apply(lambda loc: float(loc['longitude']))

# Calculate latitude and longitude of the next row
df['latitude_next'] = df['latitude'].shift(-1)
df['longitude_next'] = df['longitude'].shift(-1)

# Calculate distance using the calculate_distance function
df['distance'] = df.apply(calculate_distance, axis=1)

# Aggregate the data using GroupBy device_id, time_hour
# Apply SUM(distance), MAX(temperature), COUNT(device_id) aggregations

df = df.groupby(['device_id', 'time_hour']).agg({
    'distance': 'sum',
    'temperature': 'max',
    'location': 'count'
}).rename(columns={'distance': 'total_distance',
                   'temperature': 'max_temperature',
                   'location': 'data_points'}).reset_index()


# Load the DataFrame into the MySQL table
mysql_table_name = 'device_stats'

print("Here is the final data that will be loaded into MySQL table.")
print(df.head(10))

try:
    df.to_sql(mysql_table_name, con=mysql_engine,
              if_exists='replace', index=False)
    print(f"Loaded data into table {mysql_table_name}")
except Exception as e:
    print(f"Error occurred while loading data into table {mysql_table_name}: "
          f"{str(e)}")
finally:
    # close postgres connection as well.
    psql_engine.dispose()
    mysql_engine.dispose()
