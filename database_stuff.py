import psycopg2
def connect_postgresql(
                       host='ec2-13-56-139-208.us-west-1.compute.amazonaws.com',
                       user='power_user',
                       password='power_password'):
    """Set up the connection to postgresql database"""
    try:
        conn = psycopg2.connect(
                "dbname ='postgres' host={} user={} \
                 password={}".format(host,user,password))
        cur = conn.cursor()
        return conn,cur
    except Exception as e:
        print("Unable to connect to the database Error is ",e)
		

def fillTable(IDset, cur, conn):
	ids = []
	for id in IDset:
		ids.append((id[0], float(id[1]), float(id[2])))
	
	query = """INSERT INTO garrett_apartments_pin_data VALUES (%s, %s, %s)"""
	
	while len(ids) > 10000:
		temp = ids[:10000]
		try:
			cur.executemany(query, temp)
			conn.commit()
			ids = ids[10000:]
			print(str(len(ids)) + ' remaining entries to insert.')
		except:
			print("Error submitting last batch of data.")
			break
	
	cur.executemany(query, ids)
	conn.commit()
	print('Done.')

query = """CREATE TABLE garrett_apartments_infoCard_data (
	id TEXT UNIQUE NOT NULL,
	Amenities TEXT,
	CheckAvailabilityTitle TEXT,
	IncludeCheckAvailability TEXT,
	LanguagesSpoken TEXT,
	Listing_Address_City TEXT,
	Listing_Address_Country TEXT,
	Listing_Address_DeliveryAddress TEXT,
	Listing_Address_Location_Latitude FLOAT8,
	Listing_Address_Location_Longitude FLOAT8,
	Listing_Address_PostalCode TEXT,
	Listing_Address_State TEXT,
	Listing_Address_Submarket TEXT,
	Listing_Address_UnitNumber TEXT,
	Listing_Amenities INTEGER,
	Listing_Attachment_AttachmentId INTEGER,
	Listing_Attachment_AttachmentType SMALLINT,
	Listing_Attachment_ContentType TEXT,
	Listing_Attachment_Description TEXT,
	Listing_Attachment_EntityType SMALLINT,
	Listing_Attachment_Height SMALLINT,
	Listing_Attachment_ImageSize SMALLINT,
	Listing_Attachment_ImageSizeRequested SMALLINT,
	Listing_Attachment_IsUriOptimized BOOLEAN,
	Listing_Attachment_SortIndex SMALLINT,
	Listing_Attachment_UniqueId INTEGER,
	Listing_Attachment_Uri TEXT,
	Listing_Attachment_Width SMALLINT,
	Listing_CarouselCount SMALLINT,
	Listing_CertifiedFreshDate TEXT,
	Listing_ExternalListingProvider INTEGER,
	Listing_HasAvailabilities BOOLEAN,
	Listing_HasLeadEmail BOOLEAN,
	Listing_IsFavorite BOOLEAN,
	Listing_LanguagesSpoken  TEXT,
	Listing_ListHubListingId TEXT,
	Listing_ListingKey TEXT,
	Listing_ListingSummaryType SMALLINT,
	Listing_ListingType SMALLINT,
	Listing_ListingTypeId SMALLINT,
	Listing_Options SMALLINT,
	Listing_PetFriendly SMALLINT,
	Listing_Phones TEXT,
	Listing_PropertyManagementCompany_CompanyId INTEGER,
	Listing_PropertyManagementCompany_CompanyName TEXT,
	Listing_PropertyManagementCompany_Logo_AltText TEXT,
	Listing_PropertyManagementCompany_Logo_AttachmentId INTEGER,
	Listing_PropertyManagementCompany_Logo_AttachmentType SMALLINT,
	Listing_PropertyManagementCompany_Logo_ContentType TEXT,
	Listing_PropertyManagementCompany_Logo_Description TEXT,
	Listing_PropertyManagementCompany_Logo_EntityType SMALLINT,
	Listing_PropertyManagementCompany_Logo_Height SMALLINT,
	Listing_PropertyManagementCompany_Logo_ImageSize SMALLINT,
	Listing_PropertyManagementCompany_Logo_ImageSizeRequested SMALLINT,
	Listing_PropertyManagementCompany_Logo_IsUriOptimized BOOLEAN,
	Listing_PropertyManagementCompany_Logo_SortIndex SMALLINT,
	Listing_PropertyManagementCompany_Logo_UniqueId SMALLINT,
	Listing_PropertyManagementCompany_Logo_Uri TEXT,
	Listing_PropertyManagementCompany_Logo_Width SMALLINT,
	Listing_PropertyName TEXT,
	Listing_PropertyStyle SMALLINT,
	Listing_Rating REAL,
	Listing_Reinforcements TEXT,
	Listing_RentRollups TEXT,
	Listing_Specialties INTEGER,
	Listing_Video_AttachmentType SMALLINT,
	Listing_Video_EntityType SMALLINT,
	Listing_Video_IsUriOptimized BOOLEAN,
	Listing_Video_SortIndex SMALLINT,
	Listing_Video_UniqueId SMALLINT,
	Listing_Video_Uri TEXT,
	Listing_VirtualTour_AttachmentType SMALLINT,
	Listing_VirtualTour_EntityType SMALLINT,
	Listing_VirtualTour_IsUriOptimized BOOLEAN,
	Listing_VirtualTour_SortIndex SMALLINT,
	Listing_VirtualTour_UniqueId SMALLINT,
	Listing_VirtualTour_Uri TEXT,
	PropertyName TEXT,
	PropertyNameTitle TEXT,
	PropertyPhotoUrl TEXT,
	PropertyUrl TEXT,
	RentFormat_Alert TEXT,
	RentFormat_Availability TEXT,
	RentFormat_Beds TEXT,
	RentFormat_New TEXT,
	RentFormat_Rents TEXT,
	SearchCriteria_Options SMALLINT,
	TFNPhoneLabel TEXT,
	VideoLabel TEXT,
	VirtualTourLabel TEXT);
	"""