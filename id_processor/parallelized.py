import apartment_property_infoCardData as info

import psycopg2
import pickle
import json
from multiprocessing import Pool, Process

import time



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
	
def updateFromDatabase(cur):
	global crawled_ids
	global ids_to_crawl
	
	loadProgress()
	update_ids_to_crawl(cur)
	update_crawled_ids(cur)
	saveProgress()


def crawl(id):
	'''Takes an id string, crawls it, and logs it in the database.'''
	conn, cur = fast_connect()
	
	# Get the data
	infoJSON = info.getInfo(id).json()
	features = info.parseInfo(infoJSON) # The info is parsed within the infoCard api wrapper
	
	# Insert into table 89 values
	query = 'INSERT INTO garrett_apartments_infoCard_data VALUES (%s' + (', %s'*88) + ')'
	cur.execute(query, features)
	
	conn.commit()
	
	cur.close()
	conn.close()
	

def go():
	'''Runs the crawler. It can safely be stopped using "ctrl-c" while crawling.'''
	# Prepares the crawler.
	loadProgress()
	
	# Crawl in batches of 100
	global ids_to_crawl
	global crawled_ids
	while len(ids_to_crawl) >= 10000:
		batch = ids_to_crawl[:10000]
		with Pool(processes=42) as pool:
			pool.map(crawl, batch)
		
		for id in batch:
			crawled_ids.add(id)
		ids_to_crawl = ids_to_crawl[10000:]
		print(str(len(crawled_ids)) + " crawled so far.")
	# If any ids remain, crawl them. Otherwise, assume we broke out of the while loop due to error and exit.
	if len(ids_to_crawl) < 10000:
		try:
			with Pool(processes=32) as pool:
				pool.map(crawl, ids_to_crawl)
			
			for id in ids_to_crawl:
				crawled_ids.add(id)
			ids_to_crawl = []
			print('Done crawling!')
		except:
			print("Error crawling last few ids")

def goWrapper():
	try:
		go()
	except Exception as e:
		print(e)
		saveProgress()
		conn, cur = login_to_database()
		updateFromDatabase(cur)
		cur.close()
		conn.close()
		goWrapper()
			
if __name__ == '__main__':
	goWrapper()
	#time.time()