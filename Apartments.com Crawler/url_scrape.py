import requests
import gzip
import logging
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

def crawl_apartments():
	'''Returns a set containing all the links under the apartments.com robot.txt.'''
	print("Scraping urls from the apartments.com robots.txt.")
	logging.info("Scraping urls from the apartments.com robots.txt.")
	urls = getUrls('https://www.apartments.com/sitemap_AllProfiles.xml.gz')
	print("Done scraping.")
	logging.info("Done scraping.")
	
	return urls
	