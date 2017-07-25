import requests
import json
from bs4 import BeautifulSoup


headers = {'accept' : 'application/json, text/javascript, */*; q=0.01',
'accept-encoding':'gzip, deflate, br',
'accept-language':'en-US,en;q=0.8',
'content-type':'application/json',
'origin':'https://www.apartments.com',
'referer':'https://www.apartments.com/',
'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
'x-requested-with':'XMLHttpRequest'}

url = 'https://www.apartments.com/services/property/infoCardData'

def getInfo(listingID):
	'''Takes a string, listingID, and returns the infoCardData for that ID.'''
	payload = {"ListingKeys":[listingID],"SearchCriteria":{}}
	req = requests.post(url, headers=headers, json=payload)
	return req.json()