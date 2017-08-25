import requests
import gzip
from bs4 import BeautifulSoup
import time
import psycopg2
import psycopg2.extras
import pickle



def getFile(url):
	'''Takes a url for a file and returns the path to the downloaded file.
	Files are downloaded to the temp/ folder.'''
	
	# Get the requests.Response object for the given url
	response = requests.get(url)
	
	# Generate the filename using the filename in the url and add to the temp/ directory
	filename = 'temp/' + url[-(url[::-1].find('/')):]
	
	# Save the file
	with open(filename , 'wb') as f:
		f.write(response.content)
	
	# Return the filename of the saved file
	return filename

def decompress(filename):
	'''Takes a path to a gzip encoded file and decodes it.
	Returns the path to the decoded file in the temp/ folder.'''
	
	# Checks the last 3 letters of the filename to verify .gz file
	if (filename[-3:] != '.gz'):
		print('This is not a gz file.')

	# Opens the file using python's gzip package
	with gzip.open(filename, 'rb') as f:
		data = f.read()
	
	# Saves the file to the original location without the .gz extension
	with open(filename[:-3], 'wb') as f:
		f.write(data)
	
	return filename[:-3] # This is the filename without the .gz

def getSoup(url):
	'''Takes a url for a gzip encoded XML document and decodes it, returning the BeautifulSoup.'''
	
	# This will download the .gz file, decode it, and open it.
	# Both the .gz and decoded xml will be stored in temp/
	with open(decompress(getFile(url)), 'rb') as f:
		file = f.read()
	
	return BeautifulSoup(file, 'xml')
	
def getUrls(url):
	'''Recursively crawls a sitemap for airbnb and filters out links without "/rooms/"
	Returns a set containing tuples of (url, airbnb id, date).'''
	soup = getSoup(url)
	results = set()
	
	# If the file declares itself a sitemapindex, crawl it for other sitemap files
	if soup.contents[0].name == 'sitemapindex':
		for sitemap in soup.find_all('sitemap'):
			if not 'main1' in sitemap.find('loc').text: # Excludes general airbnb urls from the search
				# Adds all the urls from the sitemap to results recursively
				results = results | getUrls(sitemap.find('loc').text)
	
	# If the file declares itself a urlset, extract all urls from it
	elif soup.contents[0].name == 'urlset':
		for listing in soup.find_all('url'):
			url = listing.find('loc').text
			
			# Checks to verify the url is a room/listing url
			if '/rooms/' in url:
				date = listing.find('lastmod').text
				id = url[-url[::-1].find('/'):]
				results.add((url, id, date))
	
	# Every file in the airbnb robots.txt should be a sitemapindex or urlset so this should never hit.
	else:
		print('Error crawling the sitemap.')
		print(url)
		print(soup)
	
	return results

def crawl_airbnb():
	'''Returns a set containing all links under the airbnb.com robots.txt in data tuples.'''
	results = getUrls('https://www.airbnb.com/sitemap-main-index.xml.gz')
	return results
	
	
def main():
	
	# table_name is here just for record keeping
	#table_name = 'bnb_active_ids'
	
	# Recursively crawl the sitemap for url data
	active_urls = crawl_airbnb()
	
	# Save the crawled data to a local, dated, .csv file.
	filename = save(active_urls)
	
	# Open a new connection to database
	conn, cur = login_to_database()
	
	# Drop the previous table to make room for the updated table
	dropTable(cur)
	conn.commit()
	
	# Make a new table with all the updated data.
	makeTable(cur)
	conn.commit()
	
	# Close the connections (insert_into_table makes its own connection)
	cur.close()
	conn.close()
	
	# Insert all the data into the table
	insert_into_table(active_urls)
	
	
	
	
def login_to_database():
	'''Wrapper for connect_postgresql() that uses credentials stored in "credentials.py"'''
	import credentials
	try:
		conn, cur = connect_postgresql(host=credentials.host, user=credentials.user, password=credentials.password)
	except:
		print('Retrying connection...')
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
        print("Unable to connect to the database Error is ",e)

def makeTable(cur, table_name = 'bnb_active_ids'):
	'''Makes a new table for the crawled airbnb robots.txt sitemap data.'''
	query = """CREATE TABLE """ + table_name + """ (
	url TEXT UNIQUE NOT NULL,
	id INTEGER UNIQUE NOT NULL,
	date TIMESTAMP WITH TIME ZONE);"""
	cur.execute(query)

def dropTable(cur, table_name = 'bnb_active_ids'):
	'''Drops the old crawled data table.'''
	query = "DROP TABLE " + table_name
	cur.execute(query)

def insert_into_table(url_set, batch_size=200):
	'''Inserts the ids into the database 100,000 at a time.'''
	active_urls = list(url_set)
	query = 'INSERT INTO bnb_active_ids VALUES (%s, %s, %s)'
	conn, cur = login_to_database()
	while len(active_urls) > 100000:
		print(str(len(active_urls))+ ' urls left to insert.')
		psycopg2.extras.execute_batch(cur, query, active_urls[:100000], page_size=batch_size)
		active_urls = active_urls[100000:]
		conn.commit()
	
	psycopg2.extras.execute_batch(cur, query, active_urls, page_size=batch_size)
	conn.commit()
	cur.close()
	conn.close()

def load():
	with open('data.pickle', 'rb') as f:
		return pickle.load(f)


def save(url_set):
	'''Saves a set of (url, id, date) to a .csv file using pandas.'''
	filename = "Airbnb_active_ids_" + time.strftime("%d_%m_%Y") + ".csv"
	print("Saving file to: " + filename)
	
	import pandas as pd
	labels = ['url', 'id', 'date']
	airbnb_dataframe = pd.DataFrame.from_records(list(url_set), columns=labels)
	with open(filename, 'w') as f:
		airbnb_dataframe.to_csv(f)
	
	print('Done saving file.')
	return filename

if __name__ == '__main__':
	#main()
	pass