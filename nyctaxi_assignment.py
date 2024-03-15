import pandas as pd
import glob
import sys
import numpy as np

#df = pd.read_parquet("/data/nyctaxi/yellow_tripdata_2023.parquet")
df = None
for file in sorted(glob.glob("/data/nyctaxi/yellow_tripdata_2023*.parquet")):
    print("Reading", file)
    if df is None:
        df = pd.read_parquet(file)
    else:
        df = pd.concat([df, pd.read_parquet(file)])



print(df)
print(df.columns)

#print("Avg trip distance ", df["trip_distance"].mean())
#print("Min fare amount ", df["fare_amount"].min(), "Max fare amount", df["fare_amount"].max())
#print("Avg fare amount per # of passengers ", df.groupby('passenger_count')["fare_amount"].mean())
#print("Avg fare ammount for trips from the airport ", df.query("Airport_fee > 0")["fare_amount"].mean())
#df["time_column"] = df.time.dt.hour
# Group by time and then find the mean surcharge for those times
df['hour_of_day'] = df['tpep_pickup_datetime'].dt.hour
df['day_of_week'] = df['tpep_pickup_datetime'].dt.dayofweek
df['hour_of_day'] = df['hour_of_day'].astype(int)
print("Avg surcharge per hour of day ", df.groupby('hour_of_day')['congestion_surcharge'].mean())
print("Avg surcharge per day ", df.groupby('day_of_week')['congestion_surcharge'].mean())
print(df.groupby(["PULocationID", "DOLocationID"]).count()) 
#print(df["PULocationID", "DOLocationID"].groupby(["PULocationID"])
#print(df.query(('hour_of_day' > 18) & ('day_of_week' == 5 | 6)))
#print(df.query('hour_of_day'> 18))
#.groupby('PULoactionID')


"""
Tasks
- Flind average trip distance
- Find min and max fare amount
- Find average fare amount per # of passengers
- Find average fare amount for trips from the airport
- Find average congestion surcharge for each hour of the day; and for each day of the week
- Find most frequent pick up and drop off locations
- Find most frequent pick up/drop off pairs
- Find most frequent pickup locations for night hours on weekends
- It's 3:35p on a Saturday. I'm at the met. How much will it cost me any my two friends to get to the World Trade Center memorial
"""


"""
Graphs:
    - trips per day
    - Average trip distance per hour of the day
    - average fare amount per # of passengers
    - average fare amount for trips from the airport vs non-airport
    - Median congestian surcharge per hour of the day; and per day of the week (grid)
    - Overlay on ma: most frequent pick up and drop off locations
"""

import matplotlib.pyplot as plt

# Trips per day
#df ['day_of_week'] = df['tped_pickup_time']
day_pickup_amount = df.groupby('day_of_week').count()
print(day_pickup_amount)
trips_column = day_pickup_amount['tped_pickup_datetime']

ypoints = np.array([1,7])
xpoints = df['tped_pickup_datetime']
plt.plot(xpoints, ypoints)
plt.show()
#print(piciup_amount)
#pickup_amount['tped_pickup_time'].plt.bar()
#plt.show()

#Airport vs non-airport
plt.clf()
df['airport_pickup'] = df['Airport_fee'] > 0
airport_fare = df.query('airport_pickup')['fare_amount'].mean()
nonairport_fare = df.query('~airport_pickup')['fare_amount'].mean()
#plt.bar(['
print(df['airport_pickup'].value.counts())
plt.bar




# second to last
#plt.clf()
#df['day_of_week'] = df['tped_pickup_datetime'].dt.dayofweek
#df['hour_of_day'] = df['tped_pickup_datetime'].dt.hour
#xy = df.groupby(['day_of_week', 'hour_of_day'])
