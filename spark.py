from pyspark import SparkConf, SparkContext
import pyspark.pandas as ps
from glob import glob

conf = SparkConf()
conf.setAppName("nyctaxi")
conf.set("spark.executor.memory", "2g")
conf.set("spark.executor.cores", "1")
conf.set("spark.driver.memory", "8g")
conf.set("spark.driver.cores", "5")
spark = SparkContext(conf=conf)



for f in glob("/data/nyctaxi/set1/*parquet"):
    print(f)
    df = ps.read_parquet(f)
    print(df.columns)
    print(df.info(verbose=True))
    df['trip_distance'] = df['trip_distance'].astype(int)
    df.round({'pickup_latitude': 3, 'pickup_longitude': 3, 'dropoff_latitude': 3. 'dropoff_longitude': 3})
    df['year'] = df['pickup_datetime'].dt.year
    df['month'] = df['pickup_datetime'].dt.month
    #df.groupby(['year', 'month']).agg({'grouped_pickup'})
    grouped_pickup = df.groupby('year', 'month').agg(count('*').alias('grouped_pickup'))
    df = ps.join(grouped_pickup, on=['year', 'month'], how='left')
    df.select('tolls_amount', 'trip_distance', 'mta_tax', 'dropoff_latitude', 'dropoff_longitude', 'pickup_latitude', 'dropoff_longitude', 'grouped_trips').write.csv("output")



# df = ps.read_parquet("/data/nyctaxi/set1/*.parquet")
# print(df.columns)
# print(df.info(verbose=True))

# print(df.groupby('payment_type')['fare_amount'].mean())

# Average ratio of trip cost that is tolls
# Total num of trips per month across all years
# Average price per mile, excluding tolls and mta taxes
# Most popular pickup/dropoff locations (use lat/long but rounded to 3 decimal places
print(['tolls_amount'].mean())
print(df.groupby(['trip_distance'])['tolls_amount'].mean())




#mean = df['fare_amount'].mean()
#print(mean)



