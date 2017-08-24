import psycopg2
import requests
import json
import pickle
import os
import logging
import shutil
import time
import apartment_property_infoCardData as info
from multiprocessing import Pool

import bs4
from bs4 import BeautifulSoup

def get_urls(ids):
	results = set()
	bad = set()
	
	with Pool(processes=24) as pool:
		urls = pool.map(get_url, ids)
	
	for url in urls:
		if url[0] != None:
			results.add(url[0])
		else:
			bad.add(url[1])
	
	return results, bad
	
def get_url(id):
	url = info.getUrl(id)
	return (url, id)
	
def get_pins():
	with open('data.pickle', 'rb') as f:
		pin_data = pickle.load(f)
	return {i[0] for i in pin_data}

def get_ids(url_list):
	results = set()
	for url in url_list:
		results.add(url[-8:-1])
	return results

def update_crawled_urls():
	'''Updates from the apartments_page_data db to get already crawled urls.'''
	conn, cur = login_to_database()
	
	#temp
	table_name = 'apartments_page_data'
	
	print('Updating crawled_urls using ' +table_name+ '. This may take a second...')
	urls = set()
	query = 'SELECT url FROM apartments_page_data'
	cur.execute(query)
	raw_urls = cur.fetchall()
	for url in raw_urls:
		urls.add(url[0])
	
	cur.close()
	conn.close()
	
	return urls

def login_to_database():
	'''Wrapper for connect_postgresql() that uses credentials stored in "credentials.py"'''
	#try:
	import credentials
	try:
		conn, cur = connect_postgresql(host=credentials.host, user=credentials.user, password=credentials.password)
	except:
		print('Retrying connection...')
		logging.debug('Retrying connection...')
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
        print("Unable to connect to the database.")# Error is ",e)
        logging.debug("Unable to connect to the database.")
        logging.debug(e)

def getResponse(url):
	'''Takes a url and returns a response object.'''
	try:
		response = requests.get(url)
	except:									# This retries the url ONCE
		time.sleep(1)
		response = requests.get(url)
	if not response.status_code == 200:
		if response.status_code == 404: 	# This occurs when the listing is no longer on the site and the url gets redirected
			return None
		else:
			print('Status code other than 200 received. Uh oh.')
			print(response.status_code)
			print(url)
	else:
		return response


def output(data):
	with open('output.pickle', 'wb') as f:
		pickle.dump(data, f)

if __name__ == '__main__':
	with open('data.pickle', 'rb') as f:
		to_do = list(pickle.load(f))
	urls, failed = get_urls(to_do)