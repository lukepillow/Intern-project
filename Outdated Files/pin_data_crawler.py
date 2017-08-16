import apartment_search as search
import apartment_property_infoCardData as info
import pickle
import os
from multiprocessing import Pool
import psycopg2

ids = set()
query = ''

# Tail-end recursive
def scrapeIDs(coordinateBox):
	responseJSON = search.getResponse(coordinateBox[0],coordinateBox[1])
	newIds = search.getPinData(responseJSON)
	
	global ids
	ids = ids|newIds
	
	if len(newIds) > 600:
		newCoordinateBoxes = splitCoordinateBox(coordinateBox)
		scrapeIDs(newCoordinateBoxes[0])
		scrapeIDs(newCoordinateBoxes[1])
		scrapeIDs(newCoordinateBoxes[2])
		scrapeIDs(newCoordinateBoxes[3])
	else:
		print(str(len(ids)) + ' pins crawled so far.')

# Parallelizable? -Unused
def parallel_scrape(coordinateBox):
	responseJSON = search.getResponse(coordinateBox[0],coordinateBox[1])
	newIds = search.getPinData(responseJSON)
	
	if len(newIds) > 600:
		newCoordinateBoxes = splitCoordinateBox(coordinateBox)
		newIds = newIds | scrapeIDs(newCoordinateBoxes[0])
		newIds = newIds | scrapeIDs(newCoordinateBoxes[1])
		newIds = newIds | scrapeIDs(newCoordinateBoxes[2])
		newIds = newIds | scrapeIDs(newCoordinateBoxes[3])
	else:
		print(str(len(ids)) + ' pins crawled so far.')
	
	return newIds

def splitCoordinateBox(coordinateBox):
	'''Takes a coordinate box'''
	latHeight = coordinateBox[0][0] - coordinateBox[1][0]
	lonWidth = coordinateBox[0][1] - coordinateBox[1][1]
	#a|b|c
	#d|e|f
	#g|h|i
	
	a = coordinateBox[0]
	b = (coordinateBox[0][0], coordinateBox[0][1] - (lonWidth/2))
	
	d = (coordinateBox[0][0] - (latHeight/2), coordinateBox[0][1])
	e = (coordinateBox[0][0] - (latHeight/2), coordinateBox[0][1] - (lonWidth/2))
	f = (coordinateBox[0][0] - (latHeight/2), coordinateBox[1][1])
	
	h = (coordinateBox[1][0], coordinateBox[0][1] - (lonWidth/2))
	i = coordinateBox[1]
	
	box1 = (a,e)
	box2 = (b,f)
	box3 = (d,h)
	box4 = (e,i)
	
	return [box1,box2,box3,box4]

def pickleData():
	
	# If the pin_data directory doesn't exist, make it
	if not os.path.exists('pin_data'):
		os.makedirs('pin_data')
		
	with open('pin_data/data.pickle', 'wb') as f:
		pickle.dump(ids, f)

def create_table(conn, tableName):
	query = '''CREATE TABLE {0} (
	id CHARACTER(7) UNIQUE NOT NULL,
	lat FLOAT8,
	lon FLOAT8);'''.format(tableName)
	
	cur = conn.cursor()
	try:
		cur.execute(query)
		conn.commit()
	except:
		print('Error creating pin database.')
	cur.close()

def fillTable(conn, tableName):
	cur = conn.cursor()
	
	# Start by checking what ids are already in the database
	cur.execute('SELECT id FROM {0}'.format(tableName))
	old_pins = set()
	for id in cur.fetchall():
		old_pins.add(id[0])
	
	# Parse the responses before entering them into the database
	global ids
	parsedIds = []
	for id in ids:
		if id[0] not in old_pins:		# Check for only new pins while parsing
			parsedIds.append((id[0], float(id[1]), float(id[2])))
	print(str(len(parsedIds)) + " new ids found.")
	
	query = """INSERT INTO {0} VALUES (%s, %s, %s)""".format(tableName)
	
	# Insert all the ids into the database
	while len(parsedIds) > 10000:
		temp = parsedIds[:10000]
		try:
			cur.executemany(query, temp)
			conn.commit()
			parsedIds = parsedIds[10000:]
			print(str(len(parsedIds)) + ' remaining entries to insert.')
		except:
			print("Error submitting last batch of data.")
			print(e)
			break
	
	if len(parsedIds) < 10000:
		try:
			cur.executemany(query, parsedIds)
			conn.commit()
			parsedIds = []
			print('Done.')
		except:
			print('Error submitting final batch of ids.')

	
westCoastTest = ((52.85586, -138.60352),(22.106, -103.62305)) #191,000 active
bayAreaTest = ((38.58682, -122.98645), (36.82028, -121.55823)) #19,698 active
unitedStatesTest = ((73.17589717422607, -177.71484375),(-31.80289258670675,-31.11328125))

unitedStatesCoords = ((51.177315, -134.031213),(16.755917, -53.814910))
sanFranCoords = ((37.811131, -122.585923), (37.659626, -122.301309))