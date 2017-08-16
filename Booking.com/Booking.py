import requests
import demjson
from bs4 import BeautifulSoup


def getSoup(url):
	req = requests.get(url)
	soup = BeautifulSoup(req.text, 'html5lib')
	return soup

def getFeatures(url):
	
	soup = getSoup(url)
	
	#
	title = soup.title.text
	
	#
	description = soup.find(class_='hp_desc_main_content').text.strip()
	description_marker = -(description[::-1].find("What would you like to know?"[::-1])+len("What would you like to know?"))
	description = description[:description_marker].strip()									# Cuts off the extra added at the tail
	
	#
	facilities_list = []
	facilities_sections = soup.find_all(class_='facilitiesChecklist')[-1]
	for section in facilities_sections.find_all(class_='facilitiesChecklistSection'):
		section_header = section.h5.text.strip()
		for subsection in section.ul.find_all('li'):
			facilities_list.append('('+section_header+')' + ': ' + subsection.text.strip())	# Takes the form of "(Header): Section text"
	
	
	
	# Extracts the first javascript header, which is parsable with demjson
	first_json = demjson.decode(soup.find_all('script')[1])
	#
	map = first_json['hasMap']
	type = first_json['@type']
	name = first_json['name']
	description_mini = first_json['description']
	priceRange_text = first_json['priceRange']
	url_again = first_json['url']
	address_json = first_json['address']
	rating_json = first_json['aggregateRating']
	
	second_json = soup.find_all('script')[2]
	
	
	
			
	return (title, description, facilities_list)
	
	