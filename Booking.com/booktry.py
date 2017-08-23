import requests
import demjson
import time
import psycopg2
import re
import json
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs

def find_match_parentheses(s):
		counts = 0
		for i in range(len(s)):
			if s[i] == '[':
				counts += 1
			if s[i] == ']':
				counts -= 1
				if counts == 0:
					return i+1
		return None

def getResponse(url):
	'''Takes a url and returns a response object.'''
	try:
		response = requests.get(url)
	except:									# This retries the url ONCE
		time.sleep(1)
		response = requests.get(url)
	if not response.status_code == 200:
		if response.status_code == 404: 	# This occurs when the listing is no longer on the site and the url gets redirected
			return
		else:
			print('Status code other than 200 received. Uh oh.')
			print(response.status_code)
			print(url)
	else:
		return response

def getSoup(response):
	'''Returns a soup of the given requests.Response object.'''
	soup = BeautifulSoup(response.text, 'lxml')
	return soup

def getFeatures(response):
	url = response.url

	soup = getSoup(response)
	
	#
	#try:
	title = soup.title.text.strip()
	#except:
	#title=' '

	#
	#try:
	description = soup.find(class_='hp_desc_main_content').text.strip()
	description_marker = -(description[::-1].find("What would you like to know?"[::-1])+len("What would you like to know?"))
	description = description[:description_marker].strip()								# Cuts off the extra added at the tail
	#except:
	#description=' '
	#
	#try:
	facilities_list = []
	facilities_sections = soup.find_all(class_='facilitiesChecklist')[-1]
	for section in facilities_sections.find_all(class_='facilitiesChecklistSection'):
		section_header = section.h5.text.strip()
		for subsection in section.ul.find_all('li'):
			facilities_list.append('('+section_header+'): ' + subsection.text.strip())	# Takes the form of "(Header): Section text"
	
	# Extracts the first javascript header, which is parsable with demjson
	first_json = demjson.decode(soup.find_all('script')[1].text)
	#
	hotelmap = first_json['hasMap']
	hoteltype = first_json['@type']
	hotelname = first_json['name']
	description_mini = first_json['description']
	priceRange_text = first_json['priceRange']
	url_again = first_json['url']
	address_json = first_json['address']
	rating_json = first_json.get('aggregateRating') ##return none

	# Breakdown address_json into details
	ad_type=address_json['@type']
	ad_country=address_json['addressCountry']
	ad_locality=address_json['addressLocality']
	ad_region=address_json['addressRegion']
	ad_postalcode=address_json['postalCode']
	ad_streetadress=address_json['streetAddress']

	# Breakdown rating_json into details
	if rating_json:
		best_rating=rating_json['bestRating']*10
		rating_value=rating_json['ratingValue']*10
		reviewCounts=rating_json['reviewCount']
	else:
		best_rating=None
		rating_value=None
		reviewCounts=None
	#lat/lng
	lat_lng=parse_qs(urlparse(hotelmap).query)['center'][0].split(',')
	lat=lat_lng[0]
	lng=lat_lng[1]

	# score breakdown
	review_score_breakdown = {}
	review_score_distribution=soup.find(id="review_list_score_distribution")
	if review_score_distribution:
		for score_range in review_score_distribution.find_all(class_="clearfix one_col"):
			score_range_name = score_range.find(class_="review_score_name").text.strip()
			score_range_value = score_range.find(class_="review_score_value").text
			review_score_breakdown[score_range_name] = score_range_value
		review_score_breakdown=json.dumps(review_score_breakdown)
	else:
		review_score_breakdown=None

	review_sub_score_breakdown = {}
	review_breakdown=soup.find(id="review_list_score_breakdown")
	if review_breakdown:
		for sub_score in review_breakdown.find_all(class_="clearfix one_col"):
			sub_score_name = sub_score.find(class_="review_score_name").text
			sub_score_value = sub_score.find(class_="review_score_value").text
			review_sub_score_breakdown[sub_score_name] = sub_score_value
		review_sub_score_breakdown=json.dumps(review_sub_score_breakdown)
	else:
		review_sub_score_breakdown=None

	#picture url
	x=soup.find_all('script')[2].text
	x = x.replace('\n','')
	idx = x.find('hotelPhotos')
	substring = x[idx+13:]
	catched = substring[:find_match_parentheses(substring)]
	temp = re.sub(r'([^\{,]*?):[^/]',r'"\1":',catched.replace('"',"quote_place_holder").replace("'",'"').replace("quote_place_holder","'"))
	picurl = json.loads(temp)

	#room types
	room_types=[]
	for room_tag in soup.find(id='maxotel_rooms').find_all(class_='room-info'):
		room_title = room_tag.find('a').text.strip()
		room_text = ' '.join(room_tag.text.strip().replace('\n', ' ').split())
		room_text = room_text[len(room_title)+1:]
		room_types.append((room_title, room_text))
	
	
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
	return(url,title,description,json.dumps(facilities_list),hotelmap,hoteltype,hotelname,description_mini,
			   priceRange_text, url_again, ad_type, ad_country, ad_locality, ad_region, ad_postalcode, ad_streetadress,
			   best_rating, rating_value, reviewCounts, lat, lng, review_score_breakdown,
			   review_sub_score_breakdown, json.dumps(picurl), json.dumps(room_types))

def login_to_database():
	'''Wrapper for connect_postgresql() that uses credentials stored in "credentials.py"'''
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


def create_table():
	'''Luke code. Creates a table for all the stuff we crawl.'''
	import time
	date = time.strftime("%x").replace('/', '_')
	create_cmds = '''CREATE TABLE book_data
						(
							url text NOT NULL,
							title TEXT ,
							description TEXT ,
							facilities_list TEXT ,
							hotelmap TEXT ,
							hoteltype TEXT ,
							hotelname TEXT ,
							description_mini TEXT ,
							priceRange_text TEXT ,
							url_again TEXT,
							ad_type TEXT,
							ad_country TEXT,
							ad_locality TEXT,
							ad_region TEXT,
							ad_postalcode TEXT,
							ad_streetadress TEXT,
							best_rating TEXT,
							rating_value TEXT,
							reviewCounts TEXT,
							lat double precision,
							lon double precision,
							review_score_breakdown TEXT,
							review_sub_score_breakdown TEXT,
							picurl TEXT,
							room_types TEXT
							
						)'''
	conn,cur = login_to_database()
	cur.execute(create_cmds)
	conn.commit()
	cur.close()
	conn.close()
	logging.info("Table created for url_data_crawler.")


def book_crawl(url):
	#refer to garrett's code
	response = getResponse(url)

	# Check for redirect
	if response == None:
		return
	elif not response.url == url:
		return

	data=getFeatures(response)
	conn, cur = login_to_database()
	query = 'INSERT INTO book_data VALUES (%s' + (', %s'*25) + ')'	# 25 total values to insert
	cur.execute(query, data)
	conn.commit()
	cur.close()
	conn.close()       
	
#####################################
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


#def main():


if __name__ == '__main__':
	#main()
	pass