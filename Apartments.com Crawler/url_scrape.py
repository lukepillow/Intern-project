import requests
import gzip
import logging
from bs4 import BeautifulSoup


def getFile(url):
	'''Takes a string containing a url and returns the local path to the file.
	Downloads the file to temp/ folder'''
	response = requests.get(url)
	filename = 'temp/' + url[-(url[::-1].find('/')):]
	
	with open(filename , 'wb') as f:
		f.write(response.content)
		
	return filename

		
def decompress(filename):
	'''Takes a string containing the path to a gzip encoded file.
	Returns the path to the decoded file.
	Files are decoded to same directory as source.'''
	if (filename[-3:] != '.gz'):
		print('This is not a gz file.')

	with gzip.open(filename, 'rb') as f:
		data = f.read()
	
	with open(filename[:-3], 'wb') as f:
		f.write(data)
	
	return filename[:-3] # This is the filename without the .gz
		

def getSoup(url):
	'''Takes the url of a gzip encoded xml file.
	Returns the BeautifulSoup opbject for the file.
	Downloads and decodes the file to temp/ folder.'''
	with open(decompress(getFile(url)), 'rb') as f:
		file = f.read()
	
	return BeautifulSoup(file, 'xml')
	
def getUrls(url):
	'''Recursively crawls a sitemap for urls.
	Returns a Set object containing the urls.'''
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
	
	return results	# Make sure to typecast to a list after calling this function if you need to

def crawl_apartments():
	'''Returns a set containing all the links under the apartments.com robot.txt.'''
	print("Scraping urls from the apartments.com robots.txt.")
	logging.info("Scraping urls from the apartments.com robots.txt.")
	urls = getUrls('https://www.apartments.com/sitemap_AllProfiles.xml.gz')
	print("Done scraping.")
	logging.info("Done scraping.")
	
	return urls
	