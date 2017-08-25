import requests

from bs4 import BeautifulSoup





def getResponse(url):
	'''Returns the requests.Response object for the url.
	Checks for 200 status_code'''
	response = requests.get(url)
	
	# If it receives a bad status_code, then return None and print the problem
	if response.status_code != 200:
		print("Error: " + url)
		print(response.status_code)
		return None
	else:
		return response


def getSoup(url):
	'''Takes a string containing a url.
	Returns a BeautifulSoup object for it.'''
	response = getResponse(url)
	
	# Need to handle this better later...
	if response == None:
		return None
	
	return BeautifulSoup(response.text, 'html5lib')
	


def crawl(url):
	'''Crawl the given craiglist url.
	Returns a tuple containing all the features.'''
	
	# Make a soup to start extracting features
	soup = getSoup(url)
	
	# The url to return is just the url we started with
	url = url
	
	# The title of the webpage
	page_title = soup.title.text
	
	# The title of the listing
	title_tag = soup.find(class_='postingtitletext')
	
	# Use the title box to extract price and housing data.
	price = title_tag.find(class_='price').text
	housing = title_tag.find(class_='housing').text
	
	title = title_tag.text
	title = title[ : title.find('hide this posting')].strip()	# Cut off the hide/unhide posting chunk
	
	
	# Finds the breadcrumb path and cleans it into a string, then splits it into a list
	breadcrumbs = []
	for crumb in soup.find(class_='breadcrumbs').find_all('li'):
		breadcrumbs.append(crumb.text)
	breadcrumbs = ' '.join(' '.join(breadcrumbs).split()).split('>')
	
	# Finds the main description text
	description = soup.find('section', id='postingbody').text
	
	# Extract the image data from a javescript json
	raw_image_data = soup.find_all('script')[2].text
	image_json = raw_image_data[raw_image_data.find('[') : -raw_image_data[::-1].find(']')]		# Cut off the non-json parts
	image_json = json.loads(image_json)
	
	# Extract location info from the mapbox
	latitude = soup.find(class_='mapbox').find(id='map')['data-latitude']
	longitude = soup.find(class_='mapbox').find(id='map')['data-longitude']
	accuracy = soup.find(class_='mapbox').find(id='map')['data-accuracy']
	
	address = soup.find(class_='mapbox').find('div', class_='mapaddress').text
	google_map_link = soup.find(class_='mapbox').find('p', class_='mapaddress').find('a')['href']
	
	
	# Exract and build a list of the attribute tags
	attribute_group_1 = soup.find_all('p', class_='attrgroup')[0]
	attribute_group_1 = [tag.text for tag in attribute_group_1.find_all('span')]
	
	attribute_group_2 = soup.find_all('p', class_='attrgroup')[1]
	attribute_group_2 = [tag.text for tag in attribute_group_2.find_all('span')]
	
	# Extract the posting info: id, posted, updated
	posting_info = [tag.text for tag in soup.find(class_='postinginfos').find_all('p')[:3]]
	
	# TODO:
	return (url, 
	
	page_title, 
	price, 
	housing, 
	title, 
	
	breadcrumbs, 
	description, 
	image_json, 
	
	latitude, 
	longitude, 
	accuracy, 
	address, 
	google_map_link, 
	
	attribute_group_1, 
	attribute_group_2, 
	posting_info)
	












def create_table():
    create_cmds = '''CREATE TABLE ''' +table_name+ '''
						(
							url TEXT UNIQUE NOT NULL,
							
							page_title TEXT,
							price TEXT,
							housing TEXT,
							title TEXT,
							
							breadcrumbs TEXT,
							description TEXT,
							image_json TEXT,
							
							latitude DOUBLE PRECISION,
							longitude DOUBLE PRECISION,
							accuracy INTEGER,
							address TEXT,
							google_map_link TEXT,
							
							attribute_group_1 TEXT,
							attribute_group_2 TEXT,
							
							posting_info TEXT,
							
							CONSTRAINT '''+ table_name +'''_pkey PRIMARY KEY (url)
						)
						WITH (
							OIDS = FALSE
						)
						TABLESPACE pg_default'''
    conn,cur = login_to_database()
    cur.execute(create_cmds)
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Table created for url_data_crawler.")
