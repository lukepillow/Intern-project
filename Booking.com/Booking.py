import requests
import demjson
from bs4 import BeautifulSoup


def getSoup(url):
	'''Returns a soup of the given url.'''
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
	description = description[:description_marker].strip()								# Cuts off the extra added at the tail
	
	#
	facilities_list = []
	facilities_sections = soup.find_all(class_='facilitiesChecklist')[-1]
	for section in facilities_sections.find_all(class_='facilitiesChecklistSection'):
		section_header = section.h5.text.strip()
		for subsection in section.ul.find_all('li'):
			facilities_list.append('('+section_header+'): ' + subsection.text.strip())	# Takes the form of "(Header): Section text"
	
	
	
	# Extracts the first javascript header, which is parsable with demjson
	first_json = demjson.decode(soup.find_all('script')[1].text)
	#
	map = first_json['hasMap']
	type = first_json['@type']
	name = first_json['name']
	description_mini = first_json['description']
	priceRange_text = first_json['priceRange']
	url_again = first_json['url']
	address_json = first_json['address']
	rating_json = first_json['aggregateRating']
	
	# TODO
	#second_json = soup.find_all('script')[2]
	
	
	review_score_breakdown = []
	for score_range in soup.find(id="review_list_score_distribution").find_all(class_="clearfix one_col"):
		score_range_name = score_range.find(class_="review_score_name").text.strip()
		score_range_value = score_range.find(class_="review_score_value").text
		review_score_breakdown.append('('+score_range_name+'): ' + score_range_value)
	
	review_sub_score_breakdown = []
	for sub_score in soup.find(id="review_list_score_breakdown").find_all(class_="clearfix one_col"):
		sub_score_name = sub_score.find(class_="review_score_name").text
		sub_score_value = sub_score.find(class_="review_score_value").text
		review_sub_score_breakdown.append('('+sub_score_name+'): ' + sub_score_value)
	
	
	#recent_reviews = []
	#for review in soup.find(class_="review_list hp_recent_property_reviews_container").find_all(class_="review_item clearfix "):
	#	review_date = review.find(class_="review_item_date").text.strip()
	#	review_name = review.find(class_="review_item_reviewer").h4.text.strip()
	#	
	#	review_title = review.find(class_="review_item_header_content recent_property_review_title").text.strip()
	#	review_score = review.find(class_="review-score-badge").text.strip()
	#	#^Removes the bullet points
	#	review_text_negative = review.find(class_="review_neg").text.strip()[1:]
	#	review_text_positive = review.find(class_="review_pos").text.strip()[1:]
			
	return (title, description, facilities_list, review_score_breakdown, review_sub_score_breakdown)

def getReviewsRequest(url, offset=0):
	'''Returns a request object for the first 100 reviews after the offset.'''
	# Uses the booking.com reviewlist api
	headers={'Cookie':'lastSeen=1502925988969',
	'Host':'www.booking.com',
	'Upgrade-Insecure-Requests':'1',
	'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36'}
	parameters = {'cc1':'us', 'dist':1, 'pagename': None, 'r_lang':'en', 'type':'total', 'offset':offset, 'rows':100}
	newUrl = 'https://www.booking.com/reviewlist.en-gb.html'
	
	# For the request, you must set 'cc1' to the country code and 'pagename' to the hotel page's name
	parameters['pagename'] = getPagename(url)
	parameters['cc1'] = getCountryCode(url)
	
	return requests.get(newUrl, data=parameters, headers=headers)
	

def getPagename(url):
	return url[-url[::-1].find('/'):-(url[::-1].find('.en'[::-1])+3)]

def getCountryCode(url):
	# Assumes country code is always 2 letters
	start = -(url[::-1].find('/')+3)
	return url[start:start+2]