import requests
import json
from bs4 import BeautifulSoup


url = 'https://www.apartments.com/services/search/'

headers = {'accept' : 'application/json, text/javascript, */*; q=0.01',
'accept-encoding' : 'gzip, deflate, br',
'accept-language' : 'en-US,en;q=0.8,es-419;q=0.6,es;q=0.4',
'content-type' : 'application/json',
'origin' : 'https://www.apartments.com',
'referer' : 'https://www.apartments.com/',
'user-agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
'x-requested-with' : 'XMLHttpRequest'}


#hardPayload = '{"Map":{"BoundingBox":{"UpperLeft":{"Latitude":33.98037811701899,"Longitude":-116.5371322631836},"LowerRight":{"Latitude":33.7645924612227,"Longitude":-116.40083312988281}},"CountryCode":"US"},"Geography":{"GeographyType":7,"Address":{},"Location":{"Latitude":33.81202,"Longitude":-116.3889}},"Listing":{},"Paging":{"Page":null},"ResultSeed":558390,"Options":1,"IsBoundedSearch":null}'


payload = {"Map":{"BoundingBox":{"UpperLeft":{"Latitude":33.98037811701899,"Longitude":-116.5371322631836},"LowerRight":{"Latitude":33.7645924612227,"Longitude":-116.40083312988281}},"CountryCode":"US"},"Geography":{"GeographyType":7,"Address":{},"Location":{"Latitude":33.81202,"Longitude":-116.3889}},"Listing":{},"Paging":{"Page":None},"ResultSeed":558390,"Options":1,"IsBoundedSearch":None}


def getResponse(upperLeftCoordinates, lowerRightCoordinates, page = 1):
	upperLeftLat = upperLeftCoordinates[0]
	upperLeftLon = upperLeftCoordinates[1]
	lowerRightLat = lowerRightCoordinates[0]
	lowerRightLon = lowerRightCoordinates[1]
	
	payload['Map']['BoundingBox']['UpperLeft']['Latitude'] = upperLeftLat
	payload['Map']['BoundingBox']['UpperLeft']['Longitude'] = upperLeftLon
	payload['Map']['BoundingBox']['LowerRight']['Latitude'] = lowerRightLat
	payload['Map']['BoundingBox']['LowerRight']['Longitude'] = lowerRightLon
	
	if not page == 1:
		payload['Paging']['Page'] = page
	else: 
		payload['Paging']['Page'] = None
	
	request = requests.post(url, headers=headers, json=payload)
	if not request.status_code == 200:
		print('The request returned an error: ', str(request.status_code))
	else:
		return request.json()
	

def getNumberOfListings(responseJSON):
	#NOTE, THIS ONLY RETURNS ACTIVE LISTINGS!!!
	#This isn't working well at the moment
	numberString = responseJSON['MetaState']['Title'].split()[0].replace(',', '')
	return int(numberString)
	
def getHTML(responseJSON):
	'''Returns the placard html from a response JSON.'''
	return responseJSON['PlacardState']['HTML']
	

def getLinks(responseJSON):
	'''Gets the 25 links per page of listing from the responseJSON.'''
	soup = BeautifulSoup(getHTML(responseJSON), 'html5lib')
	
	results = []
	
	for tag in soup.find_all(class_ = 'placardTitle js-placardTitle '):
		results.append(tag['href'])
	return results


def getPinIDs(responseJSON):
	'''Returns a set of pin IDs from a response.'''
	#NOTE: if there are over 700 pins, then the server hit a max
	#TODO, check for double listing pins (contains a list in the 3rd position instead of null)
	dataString = responseJSON['PinsState']['cl']
	unformattedData =  dataString.split('|')[::5]
	
	results = set()
	
	if not unformattedData == ['']:
		results.add(unformattedData[0])
	
	for data in unformattedData[1:]:
		length =len(data)
		
		# End of string sometimes
		if length <= 1:
			continue
			
		# This should be 99.8% of cases
		elif length == 9:
			results.add(data[2:])
	
		# Something probably went wrong with parsing if this occurs
		else:
			print('Something went wrong parsing the cl.')
			print(data)
			print(unformattedData)
	return results
	




unitedStatesCoords = ((51.177315, -134.031213),(16.755917, -53.814910))
sanFranCoords = ((37.811131, -122.585923), (37.659626, -122.301309))