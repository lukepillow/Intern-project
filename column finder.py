import apartment_property_infoCardData as info


def findColumns(infoJSON):
	'''Takes the JSON request from the infoCardData API and returns all the columns in the request.'''
	results = []
	if infoJSON == None:
		return results
	for key in infoJSON.keys():
		if not type(infoJSON[key]) is dict:
			results.append(key)
		
		else:
			subResults = findColumns(infoJSON[key])
			for subResult in subResults:
				results.append(key + '_' + subResult)
	return results
	
def crawl(ids):
	'''Takes a set of ids and returns a set of all columns for the database.'''
	results = set()
	for id in ids:
		infoJSON = info.getInfo(id).json()
		columns = set(findColumns(infoJSON))
		results = results | columns
	return results