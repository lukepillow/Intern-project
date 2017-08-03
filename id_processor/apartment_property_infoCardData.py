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
	Amenities = str(retrieveInfo(infoJSON, ['Amenities'])) # LIST 
	CheckAvailabilityTitle = retrieveInfo(infoJSON, ['CheckAvailabilityTitle'])
	IncludeCheckAvailability = retrieveInfo(infoJSON, ['IncludeCheckAvailability'])
	LanguagesSpoken = retrieveInfo(infoJSON, ['LanguagesSpoken'])
	Listing_Address_City = retrieveInfo(infoJSON, ['Listing', 'Address', 'City'])
	Listing_Address_Country = retrieveInfo(infoJSON, ['Listing', 'Address', 'Country'])
	Listing_Address_DeliveryAddress = retrieveInfo(infoJSON, ['Listing', 'Address', 'DeliveryAddress'])
	Listing_Address_Location_Latitude = retrieveInfo(infoJSON, ['Listing', 'Address', 'Location', 'Latitude'])
	Listing_Address_Location_Longitude = retrieveInfo(infoJSON, ['Listing', 'Address', 'Location', 'Longitude'])
	Listing_Address_PostalCode = retrieveInfo(infoJSON, ['Listing', 'Address', 'PostalCode'])
	Listing_Address_State = retrieveInfo(infoJSON, ['Listing', 'Address', 'State'])
	Listing_Address_Submarket = retrieveInfo(infoJSON, ['Listing', 'Address', 'Submarket'])
	Listing_Address_UnitNumber = retrieveInfo(infoJSON, ['Listing', 'Address', 'UnitNumber'])
	Listing_Amenities = retrieveInfo(infoJSON, ['Listing', 'Amenities'])
	Listing_Attachment_AttachmentId = retrieveInfo(infoJSON, ['Listing', 'Attachment', 'AttachmentId'])
	Listing_Attachment_AttachmentType = retrieveInfo(infoJSON, ['Listing', 'Attachment', 'AttachmentType'])
	Listing_Attachment_ContentType = retrieveInfo(infoJSON, ['Listing', 'Attachment', 'ContentType'])
	Listing_Attachment_Description = retrieveInfo(infoJSON, ['Listing', 'Attachment', 'Description'])
	Listing_Attachment_EntityType = retrieveInfo(infoJSON, ['Listing', 'Attachment', 'EntityType'])
	Listing_Attachment_Height = retrieveInfo(infoJSON, ['Listing', 'Attachment', 'Height'])
	Listing_Attachment_ImageSize = retrieveInfo(infoJSON, ['Listing', 'Attachment', 'ImageSize'])
	Listing_Attachment_ImageSizeRequested = retrieveInfo(infoJSON, ['Listing', 'Attachment', 'ImageSizeRequested'])
	Listing_Attachment_IsUriOptimized = retrieveInfo(infoJSON, ['Listing', 'Attachment', 'IsUriOptimized'])
	Listing_Attachment_SortIndex = retrieveInfo(infoJSON, ['Listing', 'Attachment', 'SortIndex'])
	Listing_Attachment_UniqueId = retrieveInfo(infoJSON, ['Listing', 'Attachment', 'UniqueId'])
	Listing_Attachment_Uri = retrieveInfo(infoJSON, ['Listing', 'Attachment', 'Uri'])
	Listing_Attachment_Width = retrieveInfo(infoJSON, ['Listing', 'Attachment', 'Width'])
	Listing_CarouselCount = retrieveInfo(infoJSON, ['Listing', 'CarouselCount'])
	Listing_CertifiedFreshDate = retrieveInfo(infoJSON, ['Listing', 'CertifiedFreshDate'])
	Listing_ExternalListingProvider = retrieveInfo(infoJSON, ['Listing', 'ExternalListingProvider'])
	Listing_HasAvailabilities = retrieveInfo(infoJSON, ['Listing', 'HasAvailabilities'])
	Listing_HasLeadEmail = retrieveInfo(infoJSON, ['Listing', 'HasLeadEmail'])
	Listing_IsFavorite = retrieveInfo(infoJSON, ['Listing', 'IsFavorite'])
	Listing_LanguagesSpoken = str(retrieveInfo(infoJSON, ['Listing', 'LanguagesSpoken'])) # LIST
	Listing_ListHubListingId = retrieveInfo(infoJSON, ['Listing', 'ListHubListingId'])
	Listing_ListingKey = retrieveInfo(infoJSON, ['Listing', 'ListingKey'])
	Listing_ListingSummaryType = retrieveInfo(infoJSON, ['Listing', 'ListingSummaryType'])
	Listing_ListingType = retrieveInfo(infoJSON, ['Listing', 'ListingType'])
	Listing_ListingTypeId = retrieveInfo(infoJSON, ['Listing', 'ListingTypeId'])
	Listing_Options = retrieveInfo(infoJSON, ['Listing', 'Options'])
	Listing_PetFriendly = retrieveInfo(infoJSON, ['Listing', 'PetFriendly'])
	Listing_Phones = str(retrieveInfo(infoJSON, ['Listing', 'Phones'])) # LIST
	Listing_PropertyManagementCompany_CompanyId = retrieveInfo(infoJSON, ['Listing', 'PropertyManagementCompany', 'CompanyId'])
	Listing_PropertyManagementCompany_CompanyName = retrieveInfo(infoJSON, ['Listing', 'PropertyManagementCompany', 'CompanyName'])
	Listing_PropertyManagementCompany_Logo_AltText = retrieveInfo(infoJSON, ['Listing', 'PropertyManagementCompany', 'Logo', 'AltText'])
	Listing_PropertyManagementCompany_Logo_AttachmentId = retrieveInfo(infoJSON, ['Listing', 'PropertyManagementCompany', 'Logo', 'AttachmentId'])
	Listing_PropertyManagementCompany_Logo_AttachmentType = retrieveInfo(infoJSON, ['Listing', 'PropertyManagementCompany', 'Logo', 'AttachmentType'])
	Listing_PropertyManagementCompany_Logo_ContentType = retrieveInfo(infoJSON, ['Listing', 'PropertyManagementCompany', 'Logo', 'ContentType'])
	Listing_PropertyManagementCompany_Logo_Description = retrieveInfo(infoJSON, ['Listing', 'PropertyManagementCompany', 'Logo', 'Description'])
	Listing_PropertyManagementCompany_Logo_EntityType = retrieveInfo(infoJSON, ['Listing', 'PropertyManagementCompany', 'Logo', 'EntityType'])
	Listing_PropertyManagementCompany_Logo_Height = retrieveInfo(infoJSON, ['Listing', 'PropertyManagementCompany', 'Logo', 'Height'])
	Listing_PropertyManagementCompany_Logo_ImageSize = retrieveInfo(infoJSON, ['Listing', 'PropertyManagementCompany', 'Logo', 'ImageSize'])
	Listing_PropertyManagementCompany_Logo_ImageSizeRequested = retrieveInfo(infoJSON, ['Listing', 'PropertyManagementCompany', 'Logo', 'ImageSizeRequested'])
	Listing_PropertyManagementCompany_Logo_IsUriOptimized = retrieveInfo(infoJSON, ['Listing', 'PropertyManagementCompany', 'Logo', 'IsUriOptimized'])
	Listing_PropertyManagementCompany_Logo_SortIndex = retrieveInfo(infoJSON, ['Listing', 'PropertyManagementCompany', 'Logo', 'SortIndex'])
	Listing_PropertyManagementCompany_Logo_UniqueId = retrieveInfo(infoJSON, ['Listing', 'PropertyManagementCompany', 'Logo', 'UniqueId'])
	Listing_PropertyManagementCompany_Logo_Uri = retrieveInfo(infoJSON, ['Listing', 'PropertyManagementCompany', 'Logo', 'Uri'])
	Listing_PropertyManagementCompany_Logo_Width = retrieveInfo(infoJSON, ['Listing', 'PropertyManagementCompany', 'Logo', 'Width'])
	Listing_PropertyName = retrieveInfo(infoJSON, ['Listing', 'PropertyName'])
	Listing_PropertyStyle = retrieveInfo(infoJSON, ['Listing', 'PropertyStyle'])
	Listing_Rating = retrieveInfo(infoJSON, ['Listing', 'Rating'])
	Listing_Reinforcements = str(retrieveInfo(infoJSON, ['Listing', 'Reinforcements'])) # LIST
	Listing_RentRollups = str(retrieveInfo(infoJSON, ['Listing', 'RentRollups'])) # LIST
	Listing_Specialties = retrieveInfo(infoJSON, ['Listing', 'Specialties'])
	Listing_Video_AttachmentType = retrieveInfo(infoJSON, ['Listing', 'Video', 'AttachmentType'])
	Listing_Video_EntityType = retrieveInfo(infoJSON, ['Listing', 'Video', 'EntityType'])
	Listing_Video_IsUriOptimized = retrieveInfo(infoJSON, ['Listing', 'Video', 'IsUriOptimized'])
	Listing_Video_SortIndex = retrieveInfo(infoJSON, ['Listing', 'Video', 'SortIndex'])
	Listing_Video_UniqueId = retrieveInfo(infoJSON, ['Listing', 'Video', 'UniqueId'])
	Listing_Video_Uri = retrieveInfo(infoJSON, ['Listing', 'Video', 'Uri'])
	Listing_VirtualTour_AttachmentType = retrieveInfo(infoJSON, ['Listing', 'VirtualTour', 'AttachmentType'])
	Listing_VirtualTour_EntityType = retrieveInfo(infoJSON, ['Listing', 'VirtualTour', 'EntityType']) 
	Listing_VirtualTour_IsUriOptimized = retrieveInfo(infoJSON, ['Listing', 'VirtualTour', 'IsUriOptimized'])
	Listing_VirtualTour_SortIndex = retrieveInfo(infoJSON, ['Listing', 'VirtualTour', 'SortIndex'])
	Listing_VirtualTour_UniqueId = retrieveInfo(infoJSON, ['Listing', 'VirtualTour', 'UniqueId'])
	Listing_VirtualTour_Uri = retrieveInfo(infoJSON, ['Listing', 'VirtualTour', 'Uri'])
	PropertyName = retrieveInfo(infoJSON, ['PropertyName'])
	PropertyNameTitle = retrieveInfo(infoJSON, ['PropertyNameTitle'])
	PropertyPhotoUrl = retrieveInfo(infoJSON, ['PropertyPhotoUrl'])
	PropertyUrl = retrieveInfo(infoJSON, ['PropertyUrl'])
	RentFormat_Alert = retrieveInfo(infoJSON, ['RentFormat', 'Alert'])
	RentFormat_Availability = retrieveInfo(infoJSON, ['RentFormat', 'Availability'])
	RentFormat_Beds = retrieveInfo(infoJSON, ['RentFormat', 'Beds'])
	RentFormat_New = retrieveInfo(infoJSON, ['RentFormat', 'New'])
	RentFormat_Rents = retrieveInfo(infoJSON, ['RentFormat', 'Rents'])
	SearchCriteria_Options = retrieveInfo(infoJSON, ['SearchCriteria', 'Options'])
	TFNPhoneLabel = retrieveInfo(infoJSON, ['TFNPhoneLabel'])
	VideoLabel = retrieveInfo(infoJSON, ['VideoLabel'])
	VirtualTourLabel = retrieveInfo(infoJSON, ['VirtualTourLabel'])

	return (Amenities
, CheckAvailabilityTitle
, IncludeCheckAvailability
, LanguagesSpoken
, Listing_Address_City
, Listing_Address_Country
, Listing_Address_DeliveryAddress
, Listing_Address_Location_Latitude
, Listing_Address_Location_Longitude
, Listing_Address_PostalCode
, Listing_Address_State
, Listing_Address_Submarket
, Listing_Address_UnitNumber
, Listing_Amenities
, Listing_Attachment_AttachmentId
, Listing_Attachment_AttachmentType
, Listing_Attachment_ContentType
, Listing_Attachment_Description
, Listing_Attachment_EntityType
, Listing_Attachment_Height
, Listing_Attachment_ImageSize
, Listing_Attachment_ImageSizeRequested
, Listing_Attachment_IsUriOptimized
, Listing_Attachment_SortIndex
, Listing_Attachment_UniqueId
, Listing_Attachment_Uri
, Listing_Attachment_Width
, Listing_CarouselCount
, Listing_CertifiedFreshDate
, Listing_ExternalListingProvider
, Listing_HasAvailabilities
, Listing_HasLeadEmail
, Listing_IsFavorite
, Listing_LanguagesSpoken
, Listing_ListHubListingId
, Listing_ListingKey
, Listing_ListingSummaryType
, Listing_ListingType
, Listing_ListingTypeId
, Listing_Options
, Listing_PetFriendly
, Listing_Phones
, Listing_PropertyManagementCompany_CompanyId
, Listing_PropertyManagementCompany_CompanyName
, Listing_PropertyManagementCompany_Logo_AltText
, Listing_PropertyManagementCompany_Logo_AttachmentId
, Listing_PropertyManagementCompany_Logo_AttachmentType
, Listing_PropertyManagementCompany_Logo_ContentType
, Listing_PropertyManagementCompany_Logo_Description
, Listing_PropertyManagementCompany_Logo_EntityType
, Listing_PropertyManagementCompany_Logo_Height
, Listing_PropertyManagementCompany_Logo_ImageSize
, Listing_PropertyManagementCompany_Logo_ImageSizeRequested
, Listing_PropertyManagementCompany_Logo_IsUriOptimized
, Listing_PropertyManagementCompany_Logo_SortIndex
, Listing_PropertyManagementCompany_Logo_UniqueId
, Listing_PropertyManagementCompany_Logo_Uri
, Listing_PropertyManagementCompany_Logo_Width
, Listing_PropertyName
, Listing_PropertyStyle
, Listing_Rating
, Listing_Reinforcements
, Listing_RentRollups
, Listing_Specialties
, Listing_Video_AttachmentType
, Listing_Video_EntityType
, Listing_Video_IsUriOptimized
, Listing_Video_SortIndex
, Listing_Video_UniqueId
, Listing_Video_Uri
, Listing_VirtualTour_AttachmentType
, Listing_VirtualTour_EntityType
, Listing_VirtualTour_IsUriOptimized
, Listing_VirtualTour_SortIndex
, Listing_VirtualTour_UniqueId
, Listing_VirtualTour_Uri
, PropertyName
, PropertyNameTitle
, PropertyPhotoUrl
, PropertyUrl
, RentFormat_Alert
, RentFormat_Availability
, RentFormat_Beds
, RentFormat_New
, RentFormat_Rents
, SearchCriteria_Options
, TFNPhoneLabel
, VideoLabel
, VirtualTourLabel)

def getUrl(listingID):
	return parseInfo(getInfo(listingID).json())[-10]

def retrieveInfo(infoJSON, path):
	try:
		if path == []:
			return infoJSON
		else:
			return retrieveInfo(infoJSON[path[0]], path[1:])
	except Exception as e:
		return None
	