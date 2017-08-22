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
	response = requests.get(url)
	filename = 'temp/' + url[-(url[::-1].find('/')):]
	
	with open(filename , 'wb') as f:
		f.write(response.content)
		
	return filename

def decompress(filename):
	'''Takes a path to a gzip encoded file and decodes it.
	Returns the path to the decoded file in the temp/ folder.'''
	if (filename[-3:] != '.gz'):
		print('This is not a gz file.')

	with gzip.open(filename, 'rb') as f:
		data = f.read()
	
	with open(filename[:-3], 'wb') as f:
		f.write(data)
	
	return filename[:-3] # This is the filename without the .gz

def getSoup(url):
	'''Takes a url for a gzip encoded XML document and decodes it, returning the BeautifulSoup.'''
	with open(decompress(getFile(url)), 'rb') as f:
		file = f.read()
	
	return BeautifulSoup(file, 'xml')
	
def getUrls(url):
	'''Recursively crawls a sitemap for airbnb and filters out links without "/rooms/"
	Returns a set containing tuples of (url, airbnb id, date).'''
	soup = getSoup(url)
	results = set()
	
	if soup.contents[0].name == 'sitemapindex':
		for sitemap in soup.find_all('sitemap'):
			if not 'main1' in sitemap.find('loc').text: # Excludes general urls from the search
				results = results | getUrls(sitemap.find('loc').text)
	elif soup.contents[0].name == 'urlset':
		for listing in soup.find_all('url'):
			url = listing.find('loc').text
			if '/rooms/' in url:
				date = listing.find('lastmod').text
				id = url[-url[::-1].find('/'):]
				results.add((url, id, date))
	else:
		print('Error crawling the sitemap.')
		print(url)
		print(soup)
	
	return results

def crawl_airbnb():
	'''Returns a set containing all links under the airbnb.com robots.txt in data tuples.'''
	results = getData('https://www.airbnb.com/sitemap-main-index.xml.gz')
	#results = getData('https://www.airbnb.com/sitemap-p382.xml.gz')
	return results
	
	
def main():
	
	table_name = 'bnb_active_ids'
	
	active_urls = crawl_airbnb()
	filename = save(active_urls)
	
	conn, cur = login_to_database()
	dropTable(cur)
	conn.commit()
	makeTable(cur)
	conn.commit()
	cur.close()
	conn.close()
	
	insert_into_table(active_urls)
	
	
	
	
def login_to_database():
	'''Wrapper for connect_postgresql() that uses credentials stored in "credentials.py"'''
	#try:
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
	
	query = """CREATE TABLE """ + table_name + """ (
	url TEXT UNIQUE NOT NULL,
	id INTEGER UNIQUE NOT NULL,
	date TIMESTAMP WITH TIME ZONE);"""
	cur.execute(query)

def dropTable(cur, table_name = 'bnb_active_ids'):
	
	query = "DROP TABLE " + table_name
	cur.execute(query)

def insert_into_table(url_set, batch_size=200):
	active_urls = list(url_set)
	query = 'INSERT INTO bnb_active_ids VALUES (%s, %s, %s)'
	conn, cur = login_to_database()
	while len(active_urls) > 10000:
		print(str(len(active_urls))+ ' urls left to insert.')
		psycopg2.extras.execute_batch(cur, query, active_urls[:10000], page_size=batch_size)
		active_urls = active_urls[10000:]
		conn.commit()
	
	psycopg2.extras.execute_batch(cur, query, active_urls, page_size=batch_size)
	conn.commit()
	cur.close()
	conn.close()

def load():
	with open('data.pickle', 'rb') as f:
		return pickle.load(f)


def save(url_set):
	filename = "Airbnb_active_ids_" + time.strftime("%d/%m/%Y") + ".csv"
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