import requests
import json
import certifi


BASE_URL = "http://minedatabase.ci.northwestern.edu/mineserver/"

DB_NAME = "kegg_lte600_500mcy"  # KEGG MINE 2.0


url = BASE_URL + f"quick-search/{DB_NAME}/q=C00583"
response = requests.get(url, verify=False, timeout=100)
json.loads(response.content)
