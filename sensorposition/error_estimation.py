import utm

#########################
#SE (3.8,	0.0,	0.0)#
#NW (207.3,	22.135,	5.5)#
#########################

#############################################
#SE Corner. 33.0745			-111.97475		#
#SW Corner. 33.0745666667	-111.9750833333 #
#NW Corner. 33.0765333333	-111.9750833333 #
#NE Corner. 33.0765166667	-111.9747833333 #
#############################################

SElat = 33.0745
SElon = -111.97475



#Test1
#ll2xy->cal->xy2ll
SEutm = utm.from_latlon(SElat, SElon)
#(409017.7305875577, 3659968.4471026724, 12, 'S')	
latlon = utm.to_latlon(SEutm[0],SEutm[1],SEutm[2],SEutm[3])
xy = utm.from_latlon(latlon[0],latlon[1])
#(409017.7196342931, 3659968.446933632, 12, 'S')
#!!!
#error: SEutm-xy
#error: (0.010953264601994306,0.00016904063522815704)



#Test2
#ll2xy->cal->xy2ll
SEutm = utm.from_latlon(SElat, SElon)
newX = SEutm[0]-22.135
newY = SEutm[1]+(207.3-3.8)
#(408995.5955875577, 3660171.9471026724)
latlon = utm.to_latlon(newX,newY,SEutm[2],SEutm[3])
xy = utm.from_latlon(latlon[0],latlon[1])
#(408995.58462675463, 3660171.946933513, 12, 'S')
NWlat = 33.0765333333
NWlon = -111.9750833333
NWutm = utm.from_latlon(NWlat, NWlon)
#(408988.710283526, 3660194.1676153513, 12, 'S')
#!!!
#error1 newX,newY-NWutm
#error1(6.885304031718988,-22.220512678846717)
#error2 xy-NWutm
#error2(6.874343228642829,-22.220681838225573)



#Test3
SElat = 33.0745
SElon = -111.97475
SEutm = utm.from_latlon(SElat, SElon)
#(409017.7305875577, 3659968.4471026724, 12, 'S')
SWlat = 33.0745666667
SWlon = -111.9750833333
SWutm = utm.from_latlon(SWlat, SWlon)
#(408986.6849914966, 3659976.1272547124, 12, 'S')
NWlat = 33.0765333333
NWlon = -111.9750833333
NWutm = utm.from_latlon(NWlat, NWlon)
#(408988.710283526, 3660194.1676153513, 12, 'S')
NElat = 33.0765166667
NElon = -111.9747833333
NEutm = utm.from_latlon(NElat, NElon)
#(409016.6953067189, 3660192.059754602, 12, 'S')
#
#WE(S):31.045596061099786
#WE(N):27.98502319288673
#NS(W):218.0403606388718
#NS(E):223.61265192972496
#SE-NW
#(29.0203040317283,-225.72051267884672)