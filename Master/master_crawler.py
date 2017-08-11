# This is the master crawling file for apartments.com
# It crawls in three steps:
#	1. Crawl the apartments.com geographic search page to collect all listing IDs quickly through calls to their search API.
#	2. Using their infoCard api, collect data from the ids, including URL.
#	3. Finally using the list of URLs, crawl normally and collect all the data we can.



# Table for the apartments.com pin data
pin_table = 'apartments_pin_data'
# Table for the apartments.com infoCard data
infoCard_table = 'apartments_infoCard_data'
# Table for the apartments.com page crawl data
page_table = 'apartments_page_data'




import psycopg2
import os
import shutil
import logging





# Crawl the pin data
def crawl_pin_data():
	import pin_data_crawler as pin_crawler
	
	# Check to see if table exists
	# Otherwise make the table
	if not doesTableExist(pin_table):
		print("Making new pin data table with name: " + pin_table)
		conn, cur = login_to_database()
		pin_crawler.create_table(conn, pin_table)
		cur.close()
		conn.close()
	
	# Get the data
	try:
		pin_crawler.scrapeIDs(pin_crawler.unitedStatesCoords)
	except:							# There isn't really a good way to save the crawl progress yet	
		pin_crawler.pickleData()	# On fail, save the data anyways
		
	# Save a pickled copy for debug
	pin_crawler.pickleData()
	
	# Upload it all to the database
	conn, cur = login_to_database()
	pin_crawler.fillTable(conn, pin_table)
	cur.close()
	conn.close()


def crawl_infoCard_data():
	import infoCard_data_crawler as infoCard_crawler
	
	# Check to see if table exists
	# Otherwise make the table
	if not doesTableExist(infoCard_table):
		print("Making new infoCard data table with name: " + infoCard_table)
		conn, cur = login_to_database()
		infoCard_crawler.create_table(conn, infoCard_table)
		cur.close()
		conn.close()
	
	# Compare tables to find which ids to crawl
	# This is currently redundant, as it already does this
	#conn, cur = login_to_database()
	#infoCard_crawler.updateFromDatabase(conn)
	#cur.close()
	#conn.close()
	
	# Crawl it all
	infoCard_crawler.goWrapper(numThreads = 20)

def crawl_url_data():
	import url_data_crawler as url_crawler
	
	# Check to see if table exists
	# Otherwise make the table
	if not doesTableExist(infoCard_table):
		print("Making new page data table with name: " + page_table)
		conn, cur = login_to_database()
		url_crawler.create_table(conn, page_table)
		cur.close()
		conn.close()
	
	# Compare tables to find which urls to crawl

	# Crawl it all
	
	
	return -1



def main():
	logging.basicConfig(filename='master.log',level=logging.DEBUG)
	crawl_pin_data()
	crawl_infoCard_data()
	crawl_url_data()



# Supporting functions

def doesTableExist(tableName):
	'''Returns True if table exists and false if it doesn't.'''
	query = "SELECT '{0}'::regclass".format(tableName)
	conn, cur = login_to_database()
	
	try:
		cur.execute(query)
		return True
	except psycopg2.ProgrammingError as e:
		return False
		

def login_to_database():
	'''Wrapper for connect_postgresql() that uses credentials stored in "credentials.py"'''
	try:
		import credentials
		conn, cur = connect_postgresql(host=credentials.host, user=credentials.user, password=credentials.password)
		return conn, cur
	except:
		print("Error reading credentials.py and connecting to server. Do you have credentials.py in the same directory?")

def connect_postgresql(
                       host='',
                       user='',
                       password=''):
    """Set up the connection to postgresql database"""
    try:
        conn = psycopg2.connect(
                "dbname ='postgres' host={} user={} \
                 password={}".format(host,user,password))
        cur = conn.cursor()
        return conn,cur
    except Exception as e:
        print("Unable to connect to the database Error is ",e)
		

# Untested
def cleanUp():
	if os.path.exists('__pycache__'):
		shutil.rmtree('__pycache__')
	
if __name__ == '__main__':
	crawl_infoCard_data()