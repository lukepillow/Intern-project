import requests
from bs4 import BeautifulSoup



def getSoup(url):
	req = requests.get(url)
	soup = BeautifulSoup(req.text, 'html5lib')
	return soup

def getFeatures(url):
	
	soup = getSoup(url)
	
	
	title = soup.title.text
	
	description = soup.find(class_='hp_desc_main_content').text.strip()
	description_marker = -(description[::-1].find("What would you like to know?"[::-1])+len("What would you like to know?"))
	description = description[:description_marker].strip()									# Cuts off the extra added at the tail
	
	
	facilities_list = []
	facilities_sections = soup.find_all(class_='facilitiesChecklist')[-1]
	for section in facilities_sections.find_all(class_='facilitiesChecklistSection'):
		section_header = section.h5.text.strip()
		for subsection in section.ul.find_all('li'):
			facilities_list.append('('+section_header+')' + ': ' + subsection.text.strip())	# Takes the form of "(Header): Section text"
	
	
	
			
	return (title, description, facilities_list)
	
	