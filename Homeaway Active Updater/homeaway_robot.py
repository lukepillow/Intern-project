import requests
import gzip
from bs4 import BeautifulSoup
import time
import psycopg2
import psycopg2.extras
import pickle



def getFile(url):
	
	response = requests.get(url)
	filename = 'temp/' + url[-(url[::-1].find('/')):]
	
	with open(filename , 'wb') as f:
		f.write(response.content)
		
	return filename

def getSoup(url):
	
	with open(getFile(url), 'rb') as f:
		file = f.read()
	
	return BeautifulSoup(file, 'xml')
	
def getData(url):
	
	soup = getSoup(url)
	results = set()
	
	if soup.contents[0].name == 'sitemapindex':
		for sitemap in soup.find_all('sitemap'):
			results = results | getData(sitemap.find('loc').text)
	elif soup.contents[0].name == 'urlset':
		for listing in soup.find_all('url'):
			url = listing.find('loc').text
			results.add(url)
	else:
		print('Error crawling the sitemap.')
		print(url)
		print(soup)
	
	return results

def crawl_homeaway():
	'''Returns a set containing all links under the airbnb.com robots.txt in data tuples.'''
	results = getData('https://www.homeaway.com/seo_index_sitemap_haus_native_https.xml')
	return results
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
def main():
	
	table_name = 'bnb_active_ids'
	
	active_urls = list(crawl_airbnb())
	save(active_urls)
	
	
	
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
		
		
		
		
def makeTable(cur, table_name):
	
	table_name = 'bnb_active_ids'
	query = """CREATE TABLE """ + table_name + """ (
	url TEXT UNIQUE NOT NULL,
	id INTEGER UNIQUE NOT NULL,
	date TIMESTAMP WITH TIME ZONE);"""
	cur.execute(query)

def insert_into_table(batch_size=200):
	active_urls = load()
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

def insert_wrapper():
	return -1

def save(data):
	with open('data.pickle', 'wb') as f:
		pickle.dump(data, f)

def load():
	with open('data.pickle', 'rb') as f:
		return pickle.load(f)





if __name__ == '__main__':
	#main()
	pass