import requests
import pprint
import pandas as pd

pp = pprint.PrettyPrinter(indent=4)

url = "https://tennis-live-data.p.rapidapi.com/rankings/ATP"

headers = {
	"X-RapidAPI-Key": "c9a486b794msh0ed978fc19efe93p11f9fdjsn34a363c2926e",
	"X-RapidAPI-Host": "tennis-live-data.p.rapidapi.com"
}

response = requests.get(url, headers=headers,verify=False).json()["results"]["rankings"]

df = pd.json_normalize(response)
print(df)

pp.pprint(response)