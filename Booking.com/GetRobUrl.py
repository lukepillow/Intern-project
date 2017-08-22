def getRooms(html):
	start = "b_rooms_available_and_soldout: "
	html = html[html.find(start)+len(start):]
	
	return getList(html, 0)
	
def getList(listText, depth):
	if listText[0] == '[':
		depth = depth + 1
		return '[' + getList(listText[1:], depth)
	
	elif listText[0] == ']':
		depth = depth - 1
		if depth == 0:
			return ']'
		else:
			return ']' + getList(listText[1:], depth)
	
	else:
		return listText[0] + getList(listText[1:], depth