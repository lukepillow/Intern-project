import apartment_property_infoCardData as info

import psycopg2
import pickle
import json
from multiprocessing import Pool

import time



ids_to_crawl = []
crawled_ids = set()
insert_query = 'INSERT INTO {0} VALUES (%s' + (', %s'*88) + ')'

def login_to_database():
	'''Wrapper for connect_postgresql() that uses credentials stored in "credentials.py"'''
	try:
		import credentials
		conn, cur = connect_postgresql(host=credentials.host, user=credentials.user, password=credentials.password)
		return conn, cur
	except:
		print("Error reading credentials.py and connecting to server. Do you have credentials.py in the same directory?")

def connect_postgresql(
                       host='',
					   user='',
                       password=''):
    """Set up the connection to postgresql database"""
    try:
        conn = psycopg2.connect(
                "dbname ='postgres' host={} user={} \
                 password={}".format(host,user,password))
        cur = conn.cursor()
        return conn,cur
    except Exception as e:
        print("Unable to connect to the database Error is ", e)

def fast_connect(
                       host='',
                       user='',
                       password=''):
    """Set up the connection to postgresql database"""
    conn = psycopg2.connect(
            "dbname ='postgres' host={} user={} \
            password={}".format(host,user,password))
    cur = conn.cursor()
    return conn,cur

def makeTable(conn, tableName):
	query = """CREATE TABLE {0} (
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
	VirtualTourLabel TEXT);""".format(tableName)
	
	cur = conn.cursor()
	try:
		cur.execute(query)
		conn.commit()
	except:
		print('Error creating pin database.')
	cur.close()
	
def saveProgress():
	'''Saves data to pickled files to be loaded later with loadProgress().'''
	global crawled_ids
	global ids_to_crawl
	
	print('Saving progress...')
	
	with open('crawled_ids.pickle', 'wb') as f1:
		pickle.dump(crawled_ids, f1)
	with open('ids_to_crawl.pickle', 'wb') as f2:
		pickle.dump(ids_to_crawl, f2)
	
	print('Done saving progress!')

def loadProgress():
	'''Loads the pickled data.'''
	print('Loading previous progress...')

	global crawled_ids
	global ids_to_crawl
	with open('crawled_ids.pickle', 'rb') as f1:
		crawled_ids = crawled_ids | pickle.load(f1)
	with open('ids_to_crawl.pickle', 'rb') as f2:
		ids_to_crawl = list(set(ids_to_crawl) | set(pickle.load(f2)))
	
	ids_to_crawl = list(set(ids_to_crawl) - crawled_ids)
	
	print('Done loading crawl progress!')
	
def update_ids_to_crawl(cur):
	'''Updates from the pin_data db to get new ids to crawl. This is a quick operation.'''
	global crawled_ids
	global ids_to_crawl
	
	print('Updating ids_to_crawl using garrett_apartments_pin_data...')
	ids = set()
	query = 'SELECT id FROM garrett_apartments_pin_data'
	cur.execute(query)
	raw_ids = cur.fetchall() # Returns a list of the form[('7charid',), ('7charid',), ...]
	for id in raw_ids:
		ids.add(id[0])
	
	ids = ids - crawled_ids # Remove any crawled_ids
	ids_to_crawl = list(set(ids_to_crawl) | ids) # Add any new ids to the list
	print('Done updating ids_to_crawl.')
	
def update_crawled_ids(cur):
	'''Updates from the infoCard_data db to get already crawled ids. This is slower that updating ids_to_crawl but should be rather quick.'''
	global crawled_ids
	global ids_to_crawl
	
	print('Updating crawled_ids using garrett_apartments_infoCard_data. This may take a second...')
	ids = set()
	query = 'SELECT Listing_ListingKey FROM garrett_apartments_infoCard_data'
	cur.execute(query)
	raw_ids = cur.fetchall() # Returns a list of the form[('7charid',), ('7charid',), ...]
	for id in raw_ids:
		ids.add(id[0])
	
	crawled_ids = crawled_ids | ids # Update the list of crawled_ids
	ids_to_crawl = list(set(ids_to_crawl) - crawled_ids) # Remove any crawled_ids from ids_to_crawl
	print('Done updating crawled_ids.')
	
def updateFromDatabase(conn):
	global crawled_ids
	global ids_to_crawl
	
	cur = conn.cursor()
	loadProgress()
	update_ids_to_crawl(cur)
	update_crawled_ids(cur)
	saveProgress()


def crawl(id, tableName):
	'''Takes an id string, crawls it, and logs it in the database.'''
	conn, cur = fast_connect()
	
	# Get the data
	infoJSON = info.getInfo(id).json()
	features = info.parseInfo(infoJSON) # The info is parsed within the infoCard api wrapper
	
	# Insert into table 89 values
	global insert_query
	#query = 'INSERT INTO garrett_apartments_infoCard_data VALUES (%s' + (', %s'*88) + ')'
	cur.execute(query, features)
	
	conn.commit()
	
	cur.close()
	conn.close()
	

def go(numThreads, batchSize):
	'''Runs the crawler. It can safely be stopped using "ctrl-c" while crawling.'''
	# Prepares the crawler.
	loadProgress()
	
	# Crawl in batches of 100
	global ids_to_crawl
	global crawled_ids
	while len(ids_to_crawl) >= batchSize:
		batch = ids_to_crawl[:batchSize]
		with Pool(processes=numThreads) as pool:
			pool.map(crawl, batch)
		
		for id in batch:
			crawled_ids.add(id)
		ids_to_crawl = ids_to_crawl[batchSize:]
		print(str(len(crawled_ids)) + " crawled so far.")

	# If any ids remain, crawl them.
	if len(ids_to_crawl) < batchSize:
		try:
			with Pool(processes=numThreads) as pool:
				pool.map(crawl, ids_to_crawl)
			
			for id in ids_to_crawl:
				crawled_ids.add(id)
			ids_to_crawl = []
			print('Done crawling!')
		except:
			print("Error crawling last few ids")

def goWrapper(tableName, numThreads = 42, batchSize = 10000):
	'''Wrapper to restart on error crash. Not very elegant.'''
	global insert_query
	insert_query.format(tableName)
	
	
	try:
		go(numThreads, batchSize)
	except Exception as e:
		print(e)
		saveProgress()
		conn, cur = login_to_database()
		updateFromDatabase(conn)
		cur.close()
		conn.close()
		goWrapper(numThreads, batchSize)

if __name__ == '__main__':
	#goWrapper(tableName)
	#time.time()