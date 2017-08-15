import requests
import gzip
from bs4 import BeautifulSoup


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

results = set()
def main():
	# First download the robots.txt and get all profiles
	# TODO
	#global results
	#results = crawl_apartments()
	pass



def crawl_apartments():
	'''Returns a set containing all the links under the apartments.com robot.txt.'''
	return getUrls('https://www.apartments.com/sitemap_AllProfiles.xml.gz')

def crawl_booking():
	'''Returns a set containing all the links under the booking.com robot.txt.'''
	results = set()
	with open(getFile('https://www.booking.com/sitembk-index-https.xml'), 'rb') as f:
		soup = BeautifulSoup(f.read(), 'xml')
	for sitemap in soup.find_all('sitemap'):
		sitemap_url = sitemap.find('loc').text
		if 'en-gb' in sitemap_url and 'hotel' in sitemap_url:
			results = results | getUrls(sitemap_url)
	
	return results
			



if __name__ == '__main__':
	#main()
	pass
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
# Booking.com stuff		
		
		
def getRooms(html):
	start = "b_rooms_available_and_soldout: "
	html = html[html.find(start)+len(start):]
	
	return getList(html, 0)
	
def getList(listText, depth):
	if listText[0] == '[':
		depth = depth + 1
		return '[' + getList(listText[1:], depth)
	
	elif listText[0] == ']':
		depth = depth - 1
		if depth == 0:
			return ']'
		else:
			return ']' + getList(listText[1:], depth)
	
	else:
		return listText[0] + getList(listText[1:], depth)