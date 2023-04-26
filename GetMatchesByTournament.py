
import requests
import pprint
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode 
from GetJSONNumberOfLevels import count_levels
import pandas as pd
import dbutils


pp = pprint.PrettyPrinter(indent=4)
pd.set_option('display.max_columns',None)

spark = SparkSession.builder.getOrCreate()

url = "https://tennis-live-data.p.rapidapi.com/matches-results/1193"

headers = {
	"X-RapidAPI-Key": "c9a486b794msh0ed978fc19efe93p11f9fdjsn34a363c2926e",
	"X-RapidAPI-Host": "tennis-live-data.p.rapidapi.com"
}

response = requests.get(url, headers=headers, verify=False).json()

pp.pprint(response)

print(type(response))

# Normalizing JSON with PySpark

df = spark.read.json("./testausopen.json")
df.show()

df2 = df.select(col("results"))
df2.show()

df.printSchema()



# Normalizing JSON with Pandas --> OK ! 

df_pd = pd.json_normalize(response,record_path=["results","matches"], meta=[["results","tournament","id"]])
df_pd.head(10)

df_pd_matches = pd.json_normalize(response["results"]["matches"])
df_pd_matches.head(10)


df_pd_tournament = pd.json_normalize(response["results"]["tournament"])
df_pd_tournament.head(10)

# Count number of levels for tournaments results

nb_levels = count_levels(response)
print(nb_levels)