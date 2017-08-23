import psycopg2
import psycopg2.extras
import requests
import json
import re
import demjson
import pickle
import os
import logging
import shutil
import time

from multiprocessing import Pool

import bs4
from bs4 import BeautifulSoup


urls_to_crawl = []
crawled_urls = set()


def loadProgress():
	'''Loads the current progress from pickled data.'''
	print('Loading previous progress...')
	logging.info('Page Crawler is loading previous progress...')

	global crawled_urls
	global urls_to_crawl
	
	try:
		with open('page_data/crawled_urls.pickle', 'rb') as f1:
			crawled_urls = crawled_urls | pickle.load(f1)
		with open('page_data/urls_to_crawl.pickle', 'rb') as f2:
			urls_to_crawl = list(set(urls_to_crawl) | set(pickle.load(f2)))
	
	except:
		print('Error loading previous progress from page_data folder.')
		logging.warning('Error loading previous progress from page_data folder.')
		return
	
	
	for url in urls_to_crawl:
		if url in crawled_urls:
			urls_to_crawl.remove(url)
	
	print('Done loading crawl progress!')
	logging.info('Page Crawler is done loading previous progress!')

def saveProgress():
	'''Saves the current progress to pickled data.'''
	global crawled_urls
	global urls_to_crawl
	
	print('Saving progress...')
	logging.info('Page Crawler is saving progress...')
	
	# If the page_data directory doesn't exist, make it
	if not os.path.exists('page_data'):
		os.makedirs('page_data')
	
	with open('page_data/crawled_urls.pickle', 'wb') as f1:
		pickle.dump(crawled_urls, f1)
	with open('page_data/urls_to_crawl.pickle', 'wb') as f2:
		pickle.dump(urls_to_crawl, f2)
	
	print('Done saving progress!')
	logging.info('Page Crawler is done saving progress!')

def updateFromDatabase():
	
	loadProgress()

	conn, cur = login_to_database()
	update_crawled_urls(cur)
	cur.close()
	conn.close()
	
	saveProgress()


def update_crawled_urls(cur):
	'''Updates from the apartments_page_data db to get already crawled urls.'''
	global crawled_urls
	global urls_to_crawl
	
	#temp
	table_name = 'apartments_page_data'
	
	print('Updating crawled_urls using ' +table_name+ '. This may take a second...')
	urls = set()
	query = 'SELECT url FROM apartments_page_data'
	cur.execute(query)
	raw_urls = cur.fetchall()
	for url in raw_urls:
		urls.add(url[0])
	
	crawled_urls = urls										# Update the list of crawled_urls
	urls_to_crawl = list(set(urls_to_crawl) - crawled_urls)	# Remove any crawled_urls from urls_to_crawl
	print('Done updating crawled_urls.')

def login_to_database():
	'''Wrapper for connect_postgresql() that uses credentials stored in "credentials.py"'''
	#try:
	import credentials
	try:
		conn, cur = connect_postgresql(host=credentials.host, user=credentials.user, password=credentials.password)
	except:
		print('Retrying connection...')
		logging.debug('Retrying connection...')
		time.sleep(1)
		conn, cur = connect_postgresql(host=credentials.host, user=credentials.user, password=credentials.password)
	return conn, cur
	
def connect_postgresql(
                       host='',
                       user='',
                       password=''):
    """Set up the connection to postgresql database."""
    try:
        conn = psycopg2.connect(
                "dbname ='postgres' host={} user={} \
                 password={}".format(host,user,password))
        cur = conn.cursor()
        return conn,cur
    except Exception as e:
        print("Unable to connect to the database.")# Error is ",e)
        logging.debug("Unable to connect to the database.")
        logging.debug(e)

def create_table():
    '''Luke code. Creates a table for all the stuff we crawl.'''
    # I have added the last 6
    import time
    date = time.strftime("%x").replace('/', '_')
    create_cmds = '''CREATE TABLE apartments_page_data
                        (
                            url text NOT NULL,
                            owner text ,
                            title text ,
                            unit_type text ,
                            price_type text ,
                            street_address text ,
                            city text ,
                            region text ,
                            zip_code text ,
                            neighborhood text,
                            building_info text,
                            n_of_unit integer,
                            lat double precision,
                            lon double precision,
                            image_json text,
                            amenities text,
                            state text,
                            description text,
                            phone_number text,
                            profileType text,
							
							mediaCollection TEXT,
							rentals TEXT,
							reviews TEXT,
							costarVerified BOOLEAN,
							propertyType TEXT,
							ListingId CHARACTER(7),
							
							
                            CONSTRAINT crawled_apart_listing_{0}_pkey PRIMARY KEY (url)
                        )
                        WITH (
                            OIDS = FALSE
                        )
                        TABLESPACE pg_default'''.format(date)
    conn,cur = login_to_database()
    cur.execute(create_cmds)
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Table created for url_data_crawler.")

def getResponse(url):
	'''Takes a url and returns a response object.'''
	try:
		response = requests.get(url)
		
	# Attempt a longer wait if the connection was denied from server due to too many requests
	except requests.exceptions.ConnectionError:
		time.sleep(5)
		response = requests.get(url)
	
	# This catches all other exceptions
	except:									# This retries the url ONCE
		time.sleep(1)
		response = requests.get(url)
	
	# Process the status codes
	if not response.status_code == 200:		# 200 means there were no problems
		if response.status_code == 404: 	# This occurs when the listing is no longer on the site and the url gets redirected
			return None
		else:
			print('Status code other than 200 received. Uh oh.')
			print(response.status_code)
			print(url)
	else:
		return response

def getHTML(response):
	'''Wrapper to return the HTML from a 'requests' response.'''
	return response.text

def getJSON(soup):
	'''Takes a BeautifulSoup and returns the JSON from the bottom of an apartments.com page.'''
	# Find the info JSON at the bottom
	infoText = soup.find_all('script')[-1].text
	
	# Parse the JSON
	marker = 'startup.init('
	#	Cut off the front
	infoJSON = infoText[infoText.find(marker)+len(marker):]
	#	Cut off the back
	infoJSON = infoJSON[:-(infoJSON[::-1].find(';)') + len(';)'))]
	infoJSON = infoJSON[:-(infoJSON[::-1].find(';)') + len(';)'))]
	try:
		result = demjson.decode(infoJSON)
	except:
		infoJSON = infoJSON[:-(infoJSON[::-1].find(';)') + len(';)'))]
		infoJSON = infoJSON[:-(infoJSON[::-1].find(';)') + len(';)'))]
		infoJSON = infoJSON[:-(infoJSON[::-1].find(';)') + len(';)'))]
		result = demjson.decode(infoJSON)
	return result

def crawl(url):
	'''A crawler that follows Luke's old format exactly, but uses BeautifulSoup to parse.'''
	response = getResponse(url)
	
	# Check for redirect
	if response == None:
		return
	elif not response.url == url:
		return
	
	soup = BeautifulSoup(getHTML(response), 'lxml')
	responseJSON = getJSON(soup)
	
	#TODO improve
	try:
		owner = soup.find_all(class_='logo')[1]['alt']
	except:
		owner = None
	
	title = soup.title.text
	
	#TODO improve
	try:
		unit_type = soup.find(class_='rentRollup').find(class_='shortText').text
	except:
		unit_type = None
	
	try:
		price_type_raw = list(filter(lambda x: type(x) is bs4.element.NavigableString, soup.find(class_='rentRollup').contents)) # Tries to follow Luke's format as close as possible on parsing
		price_type = ' '.join(price_type_raw).strip()
	except:
		price_type = None
	
	street_address = responseJSON['listingAddress']
	city = responseJSON['listingCity']
	region = soup.find(itemprop='addressRegion').text
	zip_code = responseJSON['listingZip']
	neighborhood = responseJSON['listingNeighborhood']
	
	try:
		building_info = re.sub('(\n)+', '', soup.find(class_='specList propertyFeatures js-spec').text.strip()).replace('\u2022', '\n')
	except:
		building_info = None
	
	n_of_unit = -1
	try:
		for tag in soup.find(class_='specList propertyFeatures js-spec').ul.find_all('li'):
			if 'Units' in tag.text:
				n_of_unit = re.search('[0-9]+', re.search('([0-9]+)(\s)(Units)', tag.text).group()).group()
	except:
		n_of_unit = None
	
	lat = responseJSON['location']['latitude']
	lon = responseJSON['location']['longitude']
	image_json = json.dumps(responseJSON['carouselCollection'])
	
	amenities = ''
	amenitiesList = []
	
	try:
		for tag in soup.find(class_='specList js-spec').ul.find_all('li'):
			amenitiesList.append(tag.text.replace(u'\u2022',''))
		amenities = '\n'.join(amenitiesList)
	except:
		amenities = None
	
	state = responseJSON['listingState']
	description = responseJSON['listingDescription']
	phone_number = responseJSON['phoneNumber']
	profileType = responseJSON['profileType']


	

	# My additions to Luke's orginal format
	
	mediaCollection = json.dumps(responseJSON['mediaCollection']) # mediaCollection contains more data than the carouselCollection that Luke orginally harvested.
	rentals = json.dumps(responseJSON['rentals']) # Contains a json list of all the rental data. However does NOT contain the amount of units per specific style/model.
	reviews = json.dumps(responseJSON['reviews']) # Review data json list, usually empty on apartments.com
	
	if len(soup.find_all(class_='costarVerifiedBadge')) != 0:
		costarVerified = True
	else:
		costarVerified = False
	
	propertyType = responseJSON['propertyType']
	listingId = responseJSON['listingId']
	
	
	# Return data to be inserted into database
	data = (url,owner,title,unit_type,price_type,street_address,city,region,zip_code,\
		neighborhood,building_info,n_of_unit,lat,lon,image_json,amenities,state,description,\
		phone_number,profileType, mediaCollection, rentals, reviews, costarVerified, propertyType, listingId)
	
	return data

def insert_into_database(page_data):
	'''This function is responsible for inserting all the data returned by the Pool scrape.'''
	conn, cur = login_to_database()
	query = 'INSERT INTO apartments_page_data VALUES (%s' + (', %s'*25) + ')'	# 26 total values to insert
	psycopg2.extras.execute_batch(cur, query, page_data, page_size=20)			# extras.execute_batch is MUCH faster than individual executes
	conn.commit()
	cur.close()
	conn.close()
	
	
def go(numThreads, batchSize):
	'''Runs the crawler. Cant quite be gracefully stopped yet.'''
	# Crawl in batches of batchSize
	global urls_to_crawl
	global crawled_urls
	done = False
	if len(urls_to_crawl) >= batchSize:
		batch = urls_to_crawl[:batchSize]
	else:
		batch = urls_to_crawl
		done = True
	
	# Using a pool, asynchronously map threads to scrape all the urls.
	with Pool(processes=numThreads) as pool:
		page_data = pool.map(crawl, batch)
	
	# Clean the input to filter out urls that did not get a response.
	cleaned_input = []
	for page in page_data:
		if page != None:
			cleaned_input.append(page)
			
	insert_into_database(cleaned_input)
	
	# Mark the urls as crawled on the local cache.
	for url in batch:
		crawled_urls.add(url)
		urls_to_crawl.remove(url)
	
	print("\n"+str(len(crawled_urls)) + " crawled so far.")
	logging.info(str(len(crawled_urls)) + " crawled so far.")
	
	if done:
		return
	else:
		go(numThreads, batchSize)
	
			
def doesTableExist(tableName):
	'''Returns True if table exists and False if it doesn't.'''
	query = "SELECT '{0}'::regclass".format(tableName)
	conn, cur = login_to_database()
	
	try:
		cur.execute(query)
		return True
	except psycopg2.ProgrammingError as e:
		return False			
			
def goWrapper():
	'''A wrapper for go that reupdates if go crashes.
	It revalidates with the database to avoid recrawling any ids.'''
	try:
		go(24, 1000) # Number of threads to use, Number to crawl per batch.
	
	except requests.exceptions.ConnectionError as e:	# If it was just a connection error restart
		print(e)
		goWrapper()
	except Exception as e:						# Otherwise it was probably a database error and we should update from database
		print(e)
		updateFromDatabase()
		goWrapper()
			
			
def main():
	# Check to see if table exists
	# Otherwise make the table
	#page_table = 'apartments_page_data'
	#if not doesTableExist(infoCard_table):
	#	print("Making new page data table with name: " + page_table)
	#	conn, cur = login_to_database()
	#	create_table(conn, page_table)
	#	cur.close()
	#	conn.close()
	
	# Figure out what urls to crawl
	import url_scrape
	global urls_to_crawl
	urls_to_crawl = list(url_scrape.crawl_apartments())
	saveProgress()
	
	# Reconcile the new list with already crawled urls.
	updateFromDatabase()
	
	# Crawl it all
	print(str(len(urls_to_crawl))+" new urls to crawl.")
	goWrapper()



if __name__ == '__main__':
	logging.basicConfig(filename="debug.log", level=logging.DEBUG)
	main()
	pass