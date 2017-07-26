import apartment_search as search
import apartment_property_infoCardData as info


IDs = set()

def scrapeIDs(coordinateBox):
	responseJSON = search.getResponse(coordinateBox[0],coordinateBox[1])
	newIDs = search.getPinIDs(responseJSON)
	
	global IDs
	IDs = IDs|newIDs
	
	if len(newIDs) > 600: #or search.getNumberOfListings(responseJSON) > 500:
		newCoordinateBoxes = splitCoordinateBox(coordinateBox)
		scrapeIDs(newCoordinateBoxes[0])
		scrapeIDs(newCoordinateBoxes[1])
		scrapeIDs(newCoordinateBoxes[2])
		scrapeIDs(newCoordinateBoxes[3])
	#else:
	#	print('One DFS Finished')
	if len(IDs)%5000 > 4500:
		print(len(IDs))
	


def splitCoordinateBox(coordinateBox):
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

westCoastTest = ((52.85586, -138.60352),(22.106, -103.62305)) #191,000 active
bayAreaTest = ((38.58682, -122.98645), (36.82028, -121.55823)) #19,698 active
unitedStatesTest = ((73.17589717422607, -177.71484375),(-31.80289258670675,-31.11328125))

unitedStatesCoords = ((51.177315, -134.031213),(16.755917, -53.814910))
sanFranCoords = ((37.811131, -122.585923), (37.659626, -122.301309))