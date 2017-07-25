import requests
import json
from bs4 import BeautifulSoup


url = 'https://www.apartments.com/services/search/'

headers = {#':authority' : 'www.apartments.com', ':method' : 'POST',
#':path' : '/services/search/',
#':scheme' : 'https',
'accept' : 'application/json, text/javascript, */*; q=0.01',
'accept-encoding' : 'gzip, deflate, br',
'accept-language' : 'en-US,en;q=0.8,es-419;q=0.6,es;q=0.4',
'content-length' : '384',
'content-type' : 'application/json',
'cookie' : 'mv=%7b%22IsEnrolled%22%3atrue%2c%22CampaignId%22%3a10004%2c%22VariantId%22%3a1%7d; dlf=%7B%22FirstName%22%3A%22%22%2C%22LastName%22%3A%22%22%2C%22PhoneNumber%22%3A%22%22%2C%22Email%22%3A%22%22%2C%22MoveInDate%22%3A%2208%2F01%2F2017%22%2C%22EmailListings%22%3Atrue%2C%22MaxRent%22%3Anull%2C%22ContactVia%22%3Anull%2C%22Beds%22%3Anull%2C%22Bath%22%3Anull%2C%22ReasonForMoving%22%3Anull%2C%22IsSubmitted%22%3Afalse%7D; gip=%7b%22Display%22%3a%22San+Francisco%2c+CA%22%2c%22GeographyType%22%3a2%2c%22Address%22%3a%7b%22City%22%3a%22San+Francisco%22%2c%22State%22%3a%22CA%22%7d%2c%22Location%22%3a%7b%22Latitude%22%3a37.7708%2c%22Longitude%22%3a-122.3961%7d%7d; _vwo_uuid_v2=75ABDE245084350CBBF8E7A97C72E9AB|3eb271a6bb720863d8219308e7ba036e; _ga=GA1.2.1820291637.1500594541; _gat=1; uat=%7B%22VisitorId%22%3A%227ee46198-995c-40a7-8f4f-b046dd37cb87%22%2C%22VisitId%22%3A%22a5d4a742-5102-4e6f-ba1c-f00ec57e0557%22%2C%22LastActivityDate%22%3A%222017-07-24T12%3A58%3A51.598859-07%3A00%22%2C%22LastFrontDoor%22%3A%22APTS%22%2C%22LastSearchId%22%3A%22C9C79056-E2F0-40B0-9050-3130D3DAA948%22%7D; lsc=%7B%22Map%22%3A%7B%22BoundingBox%22%3A%7B%22LowerRight%22%3A%7B%22Latitude%22%3A33.84076%2C%22Longitude%22%3A-116.35586%7D%2C%22UpperLeft%22%3A%7B%22Latitude%22%3A34.05636%2C%22Longitude%22%3A-116.49216%7D%7D%2C%22CountryCode%22%3A%22US%22%7D%2C%22Geography%22%3A%7B%22GeographyType%22%3A7%2C%22Address%22%3A%7B%7D%2C%22Location%22%3A%7B%22Latitude%22%3A33.81202%2C%22Longitude%22%3A-116.3889%7D%7D%2C%22Listing%22%3A%7B%7D%2C%22Paging%22%3A%7B%7D%2C%22ResultSeed%22%3A558390%2C%22Options%22%3A1%7D; s=; _mibhv=anon-1500594541616-864067622_6339; sr=%7B%22Width%22%3A562%2C%22Height%22%3A920%2C%22PixelRatio%22%3A1%7D',
'origin' : 'https://www.apartments.com',
'referer' : 'https://www.apartments.com/',
'user-agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
'x-requested-with' : 'XMLHttpRequest'}




hardPayload = '{"Map":{"BoundingBox":{"UpperLeft":{"Latitude":33.98037811701899,"Longitude":-116.5371322631836},"LowerRight":{"Latitude":33.7645924612227,"Longitude":-116.40083312988281}},"CountryCode":"US"},"Geography":{"GeographyType":7,"Address":{},"Location":{"Latitude":33.81202,"Longitude":-116.3889}},"Listing":{},"Paging":{"Page":null},"ResultSeed":558390,"Options":1,"IsBoundedSearch":null}'


payload = {"Map":{"BoundingBox":{"UpperLeft":{"Latitude":33.98037811701899,"Longitude":-116.5371322631836},"LowerRight":{"Latitude":33.7645924612227,"Longitude":-116.40083312988281}},"CountryCode":"US"},"Geography":{"GeographyType":7,"Address":{},"Location":{"Latitude":33.81202,"Longitude":-116.3889}},"Listing":{},"Paging":{"Page":None},"ResultSeed":558390,"Options":1,"IsBoundedSearch":None}



def getResponse(upperLeftCoordinates, lowerRightCoordinates):
	upperLeftLat = upperLeftCoordinates[0]
	upperLeftLon = upperLeftCoordinates[1]
	lowerRightLat = lowerRightCoordinates[0]
	lowerRightLon = lowerRightCoordinates[1]
	
	payload['Map']['BoundingBox']['UpperLeft']['Latitude'] = upperLeftLat
	payload['Map']['BoundingBox']['UpperLeft']['Longitude'] = upperLeftLon
	payload['Map']['BoundingBox']['LowerRight']['Latitude'] = lowerRightLat
	payload['Map']['BoundingBox']['LowerRight']['Longitude'] = lowerRightLon
	request = requests.post(url, headers=headers, json=payload)
	if not request.status_code == 200:
		print('The request returned an error: ', str(request.status_code))
	else:
		return request
	

def getNumberOfListings(responseJSON):
	#TODO Fix for commas in response
	numberString = response['MetaState']['Title'].split()[0]
	return int(numberString)
	
def getHTML(responseJSON):
	return responseJSON['PlacardState']['HTML']
	

def getLinks(response):
	responseJSON = response.json()
	soup = BeautifulSoup(getHTML(responseJSON), 'html5lib')
	
	results = []
	
	for tag in soup.find_all(class_ = 'placardTitle js-placardTitle '):
		results.append(tag['href'])
	return results

#TODO:
#	Parse response
#	Figure out recursive solution




unitedStatesCoords = ((51.177315, -134.031213),(16.755917, -53.814910))
sanFranCoords = ((37.811131, -122.585923), (37.659626, -122.301309))