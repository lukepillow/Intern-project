import apartment_property_infoCardData as info

import psycopg2
import pickle
import json



ids_to_crawl = []
crawled_ids = set()

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

def saveProgress():
	global crawled_ids
	global ids_to_crawl
	
	print('Saving progress...')
	
	with open('crawled_ids.pickle', 'wb') as f1:
		pickle.dump(crawled_ids, f1)
	with open('ids_to_crawl.pickle', 'wb') as f2:
		pickle.dump(ids_to_crawl, f2)
	
	print('Done saving progress!')

def loadProgress():
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
	print('Done updating ids_to_crawl')
	
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
	print('Done updating crawled_ids')
	
def updateFromDatabase(cur):
	global crawled_ids
	global ids_to_crawl
	
	loadProgress()
	update_ids_to_crawl(cur)
	update_crawled_ids(cur)
	saveProgress()
	

def crawl(id, cur):
	'''Takes an id string, crawls it, and logs it in the database.'''
	# Get the data
	infoJSON = info.getInfo(id).json()
	features = info.parseInfo(infoJSON)
	
	# Insert into table 89 values
	query = 'INSERT INTO garrett_apartments_infoCard_data VALUES (%s' + (', %s'*88) + ')'
	cur.execute(query, features)
	

def go():
	'''Runs the crawler. It can safely be stopped using "ctrl-c" while crawling.'''
	# Prepares the crawler.
	conn, cur = login_to_database()
	loadProgress()
	
	# Crawl in batches of 100
	global ids_to_crawl
	global crawled_ids
	while len(ids_to_crawl) >= 100:
		try:
			crawlBatch(100, cur, conn)
			print(str(len(crawled_ids)) + ' ids crawled so far.')
		except:
			print('Error crawling last batch.')
			print('Changes not commited.')
			saveProgress()
			break
			
	# If any ids remain, crawl them. Otherwise, assume we broke out of the while loop due to error and exit.
	if len(ids_to_crawl < 100):
		try:
			crawlBatch(len(ids_to_crawl), cur, conn)
			print('Done crawling!')
		except:
			print("Error crawling last few ids")
	
		
def crawlBatch(batchSize, cur, conn):
	'''Crawls an amount of ids specified by batchSize, then commits changes.'''
	global ids_to_crawl
	global crawled_ids
	
	batch_ids_to_crawl = ids_to_crawl[0:batchSize]
	batch_crawled = set()
	
	for id in batch_ids_to_crawl:
		crawl(id,cur)
		batch_crawled.add(id)
		
	# Commit the changes to the database after the batch is done.
	conn.commit()
	
	# Now that the changes are commited, log the ids as crawled.
	crawled_ids = crawled_ids | batch_crawled
	ids_to_crawl = ids_to_crawl[batchSize:]
	
	