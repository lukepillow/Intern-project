import requests
import json
from bs4 import BeautifulSoup


infoCardHeaders = {'accept' : 'application/json, text/javascript, */*; q=0.01',
'accept-encoding':'gzip, deflate, br',
'accept-language':'en-US,en;q=0.8',
'content-type':'application/json',
'origin':'https://www.apartments.com',
'referer':'https://www.apartments.com/',
'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
'x-requested-with':'XMLHttpRequest'}

def getInfo(listingID):
	'''Takes a string, listingID, and returns the infoCardData for that ID.'''
	payload = {"ListingKeys":[listingID],"SearchCriteria":{}}
	req = requests.post('https://www.apartments.com/services/property/infoCardData', headers=infoCardHeaders, json=payload)
	#return req.json()
	return req


def parseInfo(infoJSON):
	'''Takes an infoJSON and returns desired values in tuple form.'''
	PropertyName = retrieveInfo(infoJSON, ['PropertyName'])
	PropertyNameTitle = retrieveInfo(infoJSON, ['PropertyNameTitle'])
	PropertyUrl = retrieveInfo(infoJSON, ['PropertyUrl'])
	PropertyPhotoUrl = retrieveInfo(infoJSON, ['PropertyPhotoUrl'])
	TFNPhoneLabel = retrieveInfo(infoJSON, ['TFNPhoneLabel'])
		#Amenities #LIST
		
	
	#Listing
	ListingKey = retrieveInfo(infoJSON, ['Listing', 'ListingKey'])
	ListingType = retrieveInfo(infoJSON, ['Listing', 'ListingType'])
		#RentRollups #What is this?
	PropertyName = retrieveInfo(infoJSON, ['Listing', 'PropertyName'])
	PropertyStyle = retrieveInfo(infoJSON, ['Listing', 'PropertyStyle'])
	HasAvailabilities = retrieveInfo(infoJSON, ['Listing', 'HasAvailabilities'])
	Rating = retrieveInfo(infoJSON, ['Listing', 'Rating'])
		#Phones #LIST, dump as json
	CertifiedFreshDate = retrieveInfo(infoJSON, ['Listing', 'CertifiedFreshDate'])
		#Amenities #INT
	ExternalListingProvider = retrieveInfo(infoJSON, ['Listing', 'ExternalListingProvider'])
	
	#Listing_Attachment
		#Uri #What is this? Main photo?
		#CarouselCount #Image Count?
	
	#Listing_Address
	City = retrieveInfo(infoJSON, ['Listing', 'Address', 'City'])
	Country = retrieveInfo(infoJSON, ['Listing', 'Address', 'Country'])
	DeliveryAddress = retrieveInfo(infoJSON, ['Listing', 'Address', 'DeliveryAddress'])
	PostalCode = retrieveInfo(infoJSON, ['Listing', 'Address', 'PostalCode'])
	State = retrieveInfo(infoJSON, ['Listing', 'Address', 'State'])
	Submarket = retrieveInfo(infoJSON, ['Listing', 'Address', 'Submarket'])
	
	#Listing_Property Management Company
		#CompanyID
		#CompanyName
	
	#Listing_Address_Location
	# rewrite to converntion
	Latitude = infoJSON['Listing']['Address']['Location']['Latitude']
	Longitude = infoJSON['Listing']['Address']['Location']['Longitude']
	
def retrieveInfo(infoJSON, path):
	try:
		if path == []:
			return infoJSON
		else:
			return retrieveInfo(infoJSON[path[0]], path[1:])
	except Exception as e:
		print(e)
		print('\n')
		print(path)
		return -1
	