import utm


#
#Gpts are points in Gantry cordinate
#
def fromGantry2LatLon(Gpts):	
	#the coordinate for postion of gantry is with origin near SE (3.8, 0.0, 0.0), 
	#positive x diretion is to North and positive y direction is to West
	#			N(x)
	#			^
	#			|
	#			|
	#			|
	#W(y)<------SE
	#########################
	#SE (3.8,	0.0,	0.0)#
	#NW (207.3,	22.135,	5.5)#
	#########################
	SElon = -111.97475
	SElat = 33.0745
	#UTM coordinates
	SEutm = utm.from_latlon(SElat, SElon)
	ccR = []
	for pt in Gpts:
		#be careful
		sensorUTMx = SEutm[0] - pt[1]
		sensorUTMy = SEutm[1] + (pt[0] - 3.8)
		sensorLatLon = utm.to_latlon(sensorUTMx,sensorUTMy,SEutm[2],SEutm[3])
		sensorLat = sensorLatLon[0]
		sensorLon = sensorLatLon[1]
		ccR += [[sensorLat,sensorLon]]
	return ccR


#
#Gpts are Gantry positions
#Spts are Sensor positions in Camera Box cordinate
#
def fromGantry2LatLon(Gpts,Spts):	
	#the coordinate for postion of gantry is with origin near SE (3.8, 0.0, 0.0), 
	#positive x diretion is to North and positive y direction is to West
	#			N(x)
	#			^
	#			|
	#			|
	#			|
	#W(y)<------SE
	#########################
	#SE (3.8,	0.0,	0.0)#
	#NW (207.3,	22.135,	5.5)#
	#########################
	SElon = -111.97475
	SElat = 33.0745
	#UTM coordinates
	SEutm = utm.from_latlon(SElat, SElon)
	ccR = []
	for i in range(len(Gpts)):
		#be careful
		sensorUTMx = SEutm[0] - Gpts[i][1] - Spts[i][1]
		sensorUTMy = SEutm[1] + (Gpts[i][0] - 3.8) + (Spts[i][0] - 3.8)
		sensorLatLon = utm.to_latlon(sensorUTMx,sensorUTMy,SEutm[2],SEutm[3])
		sensorLat = sensorLatLon[0]
		sensorLon = sensorLatLon[1]
		ccR += [[sensorLat,sensorLon]]
	return ccR