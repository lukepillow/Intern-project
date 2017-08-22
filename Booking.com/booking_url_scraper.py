import requests
import gzip
from bs4 import BeautifulSoup

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
	
def getUrls(url):
	
	soup = getSoup(url)
	results = set()
	
	if soup.contents[0].name == 'sitemapindex':
		for sitemap in soup.find_all('sitemap'):
			results = results | getUrls(sitemap.find('loc').text)
	elif soup.contents[0].name == 'urlset':
		for url in soup.find_all('url'):
			results.add(url.find('loc').text)
	else:
		print('Error crawling the sitemap.')
		print(url)
		print(soup)
	
	return results

def crawl_booking():
	'''Returns a set containing all the links under the booking.com robots.txt.'''
	results = set()
	with open(getFile('https://www.booking.com/sitembk-index-https.xml'), 'rb') as f:
		soup = BeautifulSoup(f.read(), 'xml')
	for sitemap in soup.find_all('sitemap'):
		sitemap_url = sitemap.find('loc').text
		if 'en-gb' in sitemap_url and 'hotel' in sitemap_url:
			results = results | getUrls(sitemap_url)
	
	return results