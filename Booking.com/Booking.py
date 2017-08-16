import requests
from bs4 import BeautifulSoup



def getSoup(url):
	req = requests.get(url)
	soup = BeautifulSoup(req.text, 'html5lib')
	return soup

def getFeatures(url):
	
	soup = getSoup(url)
	
	
	
	title = soup.title.text
	
	facilities_list = []
	
	facilities_sections = soup.find_all(class_='facilitiesChecklist')[-1]
	for section in facilities_sections.find_all(class_='facilitiesChecklistSection'):
		section_header = section.h5.text.strip()
		for subsection in section.ul.find_all('li'):
			facilities_list.append('('+section_header+')' + ': ' + subsection.text.strip())	# Takes the form of "(Header): Section text"
	
	description = soup.find(class_='hp_desc_main_content')
	
			
	return (title, facilities_list)
	
	