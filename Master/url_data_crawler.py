import psycopg2
import requests
import json
import re
import demjson
import pickle
import os

from multiprocessing import Pool

import bs4
from bs4 import BeautifulSoup


urls_to_crawl = []
crawled_urls = set()


def loadProgress():
	'''Loads the current progress from pickled data.'''
	print('Loading previous progress...')

	global crawled_urls
	global urls_to_crawl
	with open('page_data/crawled_urls.pickle', 'rb') as f1:
		crawled_urls = crawled_urls | pickle.load(f1)
	with open('page_data/urls_to_crawl.pickle', 'rb') as f2:
		urls_to_crawl = list(set(urls_to_crawl) | set(pickle.load(f2)))
	
	for url in urls_to_crawl:
		if url in crawled_urls:
			urls_to_crawl.remove(url)
	
	print('Done loading crawl progress!')

def saveProgress():
	'''Saves the current progress to pickled data.'''
	global crawled_urls
	global urls_to_crawl
	
	print('Saving progress...')
	
	# If the page_data directory doesn't exist, make it
	if not os.path.exists('page_data'):
		os.makedirs('page_data')
	
	with open('page_data/crawled_urls.pickle', 'wb') as f1:
		pickle.dump(crawled_urls, f1)
	with open('page_data/urls_to_crawl.pickle', 'wb') as f2:
		pickle.dump(urls_to_crawl, f2)
	
	print('Done saving progress!')

#TODO
def updateFromDatabase():

	loadProgress()

	conn, cur = login_to_database()
	update_urls_to_crawl(cur)
	update_crawled_urls(cur)
	cur.close()
	conn.close()
	
	saveProgress()

def update_urls_to_crawl(cur):
	'''Updates from the infoCard_data to get new urls to crawl.'''
	global crawled_urls
	global urls_to_crawl
	
	print('Updating urls_to_crawl using apartments_infoCard_data...')
	urls = set()
	
	#temp
	#query = 'SELECT PropertyUrl FROM apartments_infoCard_data'
	query = 'SELECT PropertyUrl FROM garrett_infoCard_test'
	
	cur.execute(query)
	raw_urls = cur.fetchall() # Returns a list of the form[('url',), ('url',), ...]
	for url in raw_urls:
		urls.add(url[0])
	
	urls = urls - crawled_urls # Remove any crawled_urls
	urls_to_crawl = list(set(urls_to_crawl) | urls) # Add any new urls to the list
	print('Done updating urls_to_crawl.')

#TODO
def update_crawled_urls(cur):
	'''Updates from the ___ db to get already crawled urls.'''
	global crawled_urls
	global urls_to_crawl
	
	#temp
	table_name = 'apartments_page_data'
	
	print('Updating crawled_urls using' +table_name+ '. This may take a second...')
	urls = set()
	query = 'SELECT url FROM apartments_page_data'
	cur.execute(query)
	raw_urls = cur.fetchall()
	for url in raw_urls:
		urls.add(url[0])
	
	crawled_urls = crawled_urls | urls 						# Update the list of crawled_urls
	urls_to_crawl = list(set(urls_to_crawl) - crawled_urls)	# Remove any crawled_urls from urls_to_crawl
	print('Done updating crawled_urls.')


# Regular expressions used to correct the response to json
#	Catches words
correction1 = re.compile("\'(((\w)*\s)*)(\w)*\'")
#	Catches phone numbers
correction2 = re.compile("\'((\d)*(\-))*(\d)*\'")
#	Catches the listing description
correction3 = re.compile("listingDescription:(.)*\n")
#	Catches all the values without quotes
correction4 = re.compile("(\s|\t)(([a-z]|[A-Z])+):\s")

def correctionFunction1(matchObject):
	'''Corrects quotes on words and phone numbers.'''
	return '"' + matchObject[0][1:-1] + '"'

def correctionFunction2(matchObject):
	'''Corrects quotes on the "listingDescription"'''
	return 'listingDescription: "' + matchObject[0][21:-4] + '",\r\n'
	
def correctionFunction3(matchObject):
	'''Corrects quotes on the json's keys.'''
	return ' "' + matchObject[0][1:-2] + '": '

# I wrote all this before finding that demjson can read it just fine -_-
def correctJSON(badJSON):
	'''Corrects the JSON response to be parsable with json.loads().'''
	# Correct the data to conform to JSON standards
	# (Fix single quotes to double quotes)
	badJSON = correction1.sub(correctionFunction1, badJSON)
	badJSON = correction2.sub(correctionFunction1, badJSON)
	badJSON = correction3.sub(correctionFunction2, badJSON)
	badJSON = correction4.sub(correctionFunction3, badJSON)
	badJSON = badJSON.replace('seoGeographyCriteria:', '"seoGeographyCriteria": ')
	return json.loads(badJSON)
	
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
    """Set up the connection to postgresql database."""
    try:
        conn = psycopg2.connect(
                "dbname ='postgres' host={} user={} \
                 password={}".format(host,user,password))
        cur = conn.cursor()
        return conn,cur
    except Exception as e:
        print("Unable to connect to the database Error is ",e)

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

def getResponse(url):
	'''Takes a url and returns a response object.'''
	response = requests.get(url)
	if not response.status_code == 200:
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
	
	
	# Insert data into database
	data = (url,owner,title,unit_type,price_type,street_address,city,region,zip_code,\
		neighborhood,building_info,n_of_unit,lat,lon,image_json,amenities,state,description,\
		phone_number,profileType, mediaCollection, rentals, reviews, costarVerified, propertyType, listingId)
	
	conn, cur = login_to_database()
	query = 'INSERT INTO apartments_page_data VALUES (%s' + (', %s'*25) + ')'	# 26 total values to insert
	cur.execute(query, data)
	conn.commit()
	cur.close()
	conn.close()
	#return data
	
	
def crawlBatch(batchSize, cur, conn):
	'''Crawls a batch of urls and commits the changes to the DB.'''
	global urls_to_crawl
	global crawled_urls
	
	batch_urls_to_crawl = urls_to_crawl[0:batchSize]
	batch_crawled = set()
	
	for url in batch_urls_to_crawl:
		#DOESNT INSERT YET
		crawl(url,cur)
		batch_crawled.add(url)
		
	# Commit the changes to the database after the batch is done.
	conn.commit()
	
	# Now that the changes are commited, log the urls as crawled.
	crawled_urls = crawled_urls | batch_crawled
	urls_to_crawl = urls_to_crawl[batchSize:]

def go(numThreads, batchSize):
	'''Runs the crawler. It can safely be stopped using "ctrl-c" while crawling.'''
	# Crawl in batches of batchSize
	global urls_to_crawl
	global crawled_urls
	while len(urls_to_crawl) >= batchSize:
		batch = urls_to_crawl[:batchSize]
		with Pool(processes=numThreads) as pool:
			pool.map(crawl, batch)
		
		for url in batch:
			crawled_urls.add(url)
		urls_to_crawl = urls_to_crawl[batchSize:]
		print(str(len(crawled_urls)) + " crawled so far.")

	# If any urls remain, crawl them.
	if len(urls_to_crawl) < batchSize:
		try:
			with Pool(processes=numThreads) as pool:
				pool.map(crawl, urls_to_crawl)
			
			for url in urls_to_crawl:
				crawled_urls.add(url)
			urls_to_crawl = []
			print('Done crawling!')
		except:
			print("Error crawling last few urls")

if __name__ == '__main__':
	updateFromDatabase()
	#loadProgress()
	go(10, 1000)