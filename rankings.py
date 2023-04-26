import requests
import pprint
import pandas as pd
import os, uuid, sys
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient, DataLakeFileClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings

pp = pprint.PrettyPrinter(indent=4)

url = "https://tennis-live-data.p.rapidapi.com/rankings/ATP"

headers = {
	"X-RapidAPI-Key": "c9a486b794msh0ed978fc19efe93p11f9fdjsn34a363c2926e",
	"X-RapidAPI-Host": "tennis-live-data.p.rapidapi.com"
}

response = requests.get(url, headers=headers,verify=False).json()["results"]["rankings"]

df = pd.json_normalize(response)
df.to_parquet('rankings.parquet')
df.to_parquet('https://azplaygroundstoragedev.dfs.core.windows.net/tennisproject/rankings/rankings.parquet',verify=False)
pd.read_parquet('rankings.parquet')
print(df)

# Write to Azure Data Lake

accessKey = "tzO4CUfreIMwTGDWDm5rPpmJhkJcIJPTk10WULnweTNSrKsv7GOKfSgI3PFXwbllrRNz34ueJokZ+ASteFq9FA=="
# sas_token = "?sv=2021-12-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-03-28T18:07:28Z&st=2023-03-14T21:07:28Z&spr=https&sig=BNQ%2Bg%2B%2FO1JdQtzBxhfXm0pa4EqwRC92A7cxr9bp37bE%3D"

service_client = DataLakeServiceClient(account_url="https://azplaygroundstoragedev.dfs.core.windows.net", credential=accessKey)


# Define file system and directory

file_system_client = service_client.get_file_system_client(file_system="tennisproject")

directory_client = file_system_client.get_directory_client("rankings")

file_client = directory_client.create_file("rankings.parquet")

local_file = open("rankings.parquet")

files_contents = local_file.read()

file_client.append_data(data=files_contents, offset=0, length=len(files_contents))

file_client.flush_data(len(files_contents))