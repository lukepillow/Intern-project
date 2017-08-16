import requests
import gzip
from bs4 import BeautifulSoup
import time
import psycopg2

robotUrl = "https://www.apartments.com/robots.txt"


#bookUrl = "https://www.booking.com/hotel/py/dazzler-asuncion.en-gb.html?label=gen173nr-1DCAsovQFCEGNhc2EtZGVsLWNvY29udXRICWIFbm9yZWZyBXVzX2NhiAEBmAEuuAEHyAEM2AED6AEB-AECkgIBeagCAw;sid=75f9e88a9dcb4b0a176e3afdbc3a47fb;all_sr_blocks=166612103_89736821_2_1_0;checkin=2017-08-14;checkout=2017-08-15;dest_id=-910015;dest_type=city;dist=0;group_adults=2;highlighted_blocks=166612103_89736821_2_1_0;hpos=2;room1=A%2CA;sb_price_type=total;srfid=58cc57e6d21a46b5dcfda56d0353bc275265bd3bX2;srpvid=eca9841c582e00ee;type=total;ucfs=1&#hotelTmpl"




def getFile(url):
	
	response = requests.get(url)
	filename = 'temp/' + url[-(url[::-1].find('/')):]
	
	with open(filename , 'wb') as f:
		f.write(response.content)
		
	return filename

		
def decompress(filename):
	if (filename[-3:] != '.gz'):
		print('This is not a gz file.')

	with gzip.open(filename, 'rb') as f:
		data = f.read()
	
	with open(filename[:-3], 'wb') as f:
		f.write(data)
	
	return filename[:-3] # This is the filename without the .gz
		

def getSoup(url):
	
	with open(decompress(getFile(url)), 'rb') as f:
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
			if '/rooms/' in url:
				date = listing.find('lastmod').text
				id = url[-url[::-1].find('/'):]
				results.add((url, id, date))
	else:
		print('Error crawling the sitemap.')
		print(url)
		print(soup)
	
	return results

	
	
	
def main():
	
	table_name = 'bnb_active_ids'
	
	active_urls = list(crawl_airbnb())
	
	query = 'INSERT INTO ' + table_name + ' VALUES (%s, %s, %s)'
	conn, cur = login_to_database()
	while len(active_urls) < 10000:
		print(str(len(active_urls))+ ' urls left to insert.')
		cur.executemany(query, active_urls[:10000])
		active_urls = active_urls[10000:]
		conn.commit()
	
	cur.executemany(query, active_urls)
	conn.commit()
	conn.close()
	
	
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
	
def crawl_airbnb():
	'''Returns a set containing all links under the airbnb.com robots.txt in data tuples.'''
	#results = getData('https://www.airbnb.com/sitemap-main-index.xml.gz')
	results = getData('https://www.airbnb.com/sitemap-p382.xml.gz')
	return results


def makeTable(cur, table_name):
	query = """CREATE TABLE """ + table_name + """ (
	url TEXT UNIQUE NOT NULL,
	id INTEGER UNIQUE NOT NULL,
	date TIMESTAMP WITH TIME ZONE);"""
	cur.execute(query)

if __name__ == '__main__':
	#main()
	pass
		