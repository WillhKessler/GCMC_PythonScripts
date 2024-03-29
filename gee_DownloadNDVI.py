import ee 
import datetime
import time
import json
ee.Initialize()

########### REQUIRED INPUTS ###########################################################################
NewEngland = ee.FeatureCollection("users/nhisa/NewEngland")
MiddleAtlantic = ee.FeatureCollection("users/nhisa/MiddleAtlantic")
Wisconsin = ee.FeatureCollection("users/nhisa/EastNorthCentral1")
Michigan = ee.FeatureCollection("users/nhisa/EastNorthCentral2")
Illinois = ee.FeatureCollection("users/nhisa/EastNorthCentral3")
IndianaOhio = ee.FeatureCollection("users/nhisa/EastNorthCentral4")
KentuckyTennessee = ee.FeatureCollection("users/nhisa/EastSouthCentral1")
MississippiAlabama = ee.FeatureCollection("users/nhisa/EastSouthCentral2")
NorthSouthDakota = ee.FeatureCollection("users/nhisa/WestNorthCentrala")
Minnesota = ee.FeatureCollection("users/nhisa/WestNorthCentralb")
Nebraska = ee.FeatureCollection("users/nhisa/WestNorthCentralc")
MissouriIowa = ee.FeatureCollection("users/nhisa/WestNorthCentrald")
Kansas = ee.FeatureCollection("users/nhisa/WestNorthCentrale")
SouthAtlantic1 = ee.FeatureCollection("users/nhisa/SouthAtlantic1a")
SouthCarolinaGeorgia = ee.FeatureCollection("users/nhisa/SouthAtlantic3")
Florida = ee.FeatureCollection("users/nhisa/SouthAtlantic4")
Idaho = ee.FeatureCollection("users/nhisa/Mountain2")
Wyoming = ee.FeatureCollection("users/nhisa/Mountain3")
Nevada = ee.FeatureCollection("users/nhisa/Mountain4")
Utah = ee.FeatureCollection("users/nhisa/Mountain5")
Colorado = ee.FeatureCollection("users/nhisa/Mountain6")
Arizona = ee.FeatureCollection("users/nhisa/Mountain7")
NewMexico = ee.FeatureCollection("users/nhisa/Mountain8")
Oklahoma = ee.FeatureCollection("users/nhisa/WestSouthCentral1")
ArkansasLouisiana = ee.FeatureCollection("users/nhisa/WestSouthCentral2")
WashingtonOregon = ee.FeatureCollection("users/nhisa/Pacific1")
MontanaPart2 = ee.FeatureCollection("users/nhisa/Montana_Part2")
MontanaPart1 = ee.FeatureCollection("users/nhisa/Montana_1")
NorthCarolina1 = ee.FeatureCollection(ee.Geometry.Polygon(
    [[[-79.87127913135589, 34.6475657094146],
      [-79.82733381885589, 36.7000345583985],
      [-81.56317366260589, 36.73526074759321],
      [-83.95819319385589, 35.77850267461113],
      [-84.57342756885589, 34.95428963577578]]]))
NorthCarolina2 = ee.FeatureCollection(ee.Geometry.Polygon(
    [[[-79.95916975635589, 36.68241540701664],
      [-79.98114241260589, 34.52093631007121],
      [-78.57489241260589, 33.64744450793628],
      [-75.27899397510589, 35.17011242695719],
      [-75.54266585010589, 36.68241540701664]]]))
Texas1 = ee.FeatureCollection(ee.Geometry.Polygon(
    [[[-106.95018365101828, 31.571191097909335],
      [-104.04407126926088, 29.02490694926815],
      [-102.76778856927473, 28.860640430484562],
      [-100.96243871579134, 29.140100562395375],
      [-100.58926678247121, 31.57612467416733],
      [-100.56234094836174, 32.74832231817455],
      [-98.5875197391764, 32.874743593476815],
      [-99.62520412839933, 33.558122258092425],
      [-101.37384063181275, 34.24031311583509],
      [-103.23085205724044, 35.08164585686119],
      [-103.52322199301977, 32.599948737593586],
      [-106.73029756525608, 32.763955944712755]]]))
Texas2 = ee.FeatureCollection(ee.Geometry.Polygon(
        [[[-101.82514415191838, 34.23181468294845],
          [-98.14270255402835, 32.54366653272459],
          [-96.41987071441838, 32.61812188224064],
          [-96.3011237452825, 32.94684136579612],
          [-96.41850274752244, 33.59108908144482],
          [-96.24408946441838, 34.34074188365456],
          [-99.58393321441838, 34.77503564842936],
          [-99.53998790191838, 36.73626923170851],
          [-103.36323008941838, 36.73626923170851],
          [-103.24335628841823, 34.83709347009601]]]))
Texas3 = ee.FeatureCollection(ee.Geometry.Polygon(
        [[[-100.73630963148452, 32.8264230021364],
          [-101.0880414609498, 28.598765047302464],
          [-99.28724949103554, 26.456491317158285],
          [-96.87131828995695, 25.429468427428645],
          [-97.0030947273849, 27.513291750832348],
          [-93.48922178647081, 29.52017148459367],
          [-93.13799709965656, 31.600175515662198],
          [-93.47628690349842, 32.61761488463087],
          [-93.65206005068751, 33.567229372010715],
          [-94.98365801829914, 34.14573226729637],
          [-96.50474241124965, 34.03544139797709],
          [-96.8711681657499, 33.041728559753345]]]))
CaliPart1 = ee.FeatureCollection(ee.Geometry.Polygon(
        [[[-124.28106008522997, 39.011877197055036],
          [-121.77617727272997, 35.872529247588176],
          [-118.17266164772997, 37.9454455156237],
          [-119.71074758522997, 39.011877197055036],
          [-119.79863821022997, 42.11569789655484],
          [-124.67656789772997, 42.14828779944612]]]))
CaliPart2 = ee.FeatureCollection(ee.Geometry.Polygon(
        [[[-121.1995373916721, 33.95836802805502],
          [-117.1565686416721, 32.19092509237336],
          [-114.4759045791721, 32.3395631495797],
          [-113.8167248916721, 34.10404365602724],
          [-114.2122327041721, 34.9728221294801],
          [-118.4309827041721, 38.183336938603134],
          [-122.0784436416721, 36.116993250699245]]]))


# Values to perform focal statistics. A value of 30 will return the native 30m resolution Landsat
# Other values include 270, 1230 i.e.
#focalstats = [30,270,1230]
focalstats = [270]

# Specify years to create an array (with years as columns).
yrarr = ["2019", "2020","2021","2022"]

#Landsat Collection Years
Landsatcollections = {'LANDSAT/LC09/C02/T1': [range(2022,int(datetime.date.today().year)+1),['B5','B4']],
               'LANDSAT/LC08/C02/T1':[range(2013,2023),['B5','B4']],
               'LANDSAT/LE07/C02/T1':[range(1999,2021),['B4','B3']],
               'LANDSAT/LT05/C02/T1':[range(1984,2012),['B4','B3']]}

#######################################################################################################
## Code to pull seasonal NDVI from Landsat 8 data with cloud mask for contiguous United States  #
# June 2020
geolist = [NewEngland,MiddleAtlantic,Wisconsin,Michigan,Illinois,IndianaOhio,KentuckyTennessee,MississippiAlabama,
    NorthSouthDakota,Minnesota,Nebraska,MissouriIowa,Kansas,SouthAtlantic1,SouthCarolinaGeorgia,Florida,Idaho,Wyoming,Nevada,Utah,
    Colorado,Arizona,NewMexico,Oklahoma,ArkansasLouisiana,WashingtonOregon,MontanaPart1,MontanaPart2,NorthCarolina1,NorthCarolina2,Texas1,
    Texas2,Texas3,CaliPart1,CaliPart2]

geonames = ["NewEngland","MiddleAtlantic","Wisconsin","Michigan","Illinois","IndianaOhio","KentuckyTennessee","MississippiAlabama",
"NorthSouthDakota","Minnesota","Nebraska","MissouriIowa","Kansas","SouthAtlantic1","SouthCarolinaGeorgia","Florida","Idaho","Wyoming","Nevada","Utah",
"Colorado","Arizona","NewMexico","Oklahoma","ArkansasLouisiana","WashingtonOregon","MontanaPart1","MontanaPart2","NorthCarolina1","NorthCarolina2","Texas1","Texas2",
"Texas3","CaliPart1","CaliPart2"]

geolist = geolist[33:35]

geonames = geonames[33:35]
print(geonames)


# Populate array with start dates in the format of year-mo-day by season.
def makeSt(yr):
  stArr = [yr + "-01-01", yr + "-04-01", yr + "-07-01", yr + "-10-01"]
#print(stArr)
  return(stArr)


# Populate array by with end dates by season.
def makeEd(yr):
    eArr = [yr + "-03-31", yr + "-06-30", yr + "-09-30", yr + "-12-31"]
    return(eArr)

# Function to determine the correct collection and parameters 
# def determineCol(dictionary,start,end):
  #colls_containing_dates = []
  #for key,value in dictionary.items():
    #if start in value[0]:
      #colls_containing_dates.append(key)
  #return colls_containing_dates

# Create a function to:
# Pull Landsat scenes between start and end dates as defined above.
def GetImage(bdt, edt,geo,fs,col):
  start = ee.Date(bdt)
  end = ee.Date(edt)
  
  #colkey = determineCol(col,start,end)[0]
  #colvalues = col[colkey]
  
  # Load a raw Landsat ImageCollection for a single year and filter temporally and spatially.
  #Change to match region of interest.
  
  #collection = ee.ImageCollection(colentry.key).filterDate(start, end).filterBounds(geo)
  collection = ee.ImageCollection('LANDSAT/LC08/C02/T1').filterDate(start, end).filterBounds(geo)

  # Check: How many images in each monthly Landsat image collection?
  #count = collection.size()
  #print('Number of images in collection:', count)

  # Create a cloud-free composite with default parameters. Now, instead of working with an image collection,
  #we are working with a single image. This should reduce the size of the downloads. #
  composite = ee.Algorithms.Landsat.simpleComposite(collection)

  # Check: Did the Landsat composite retain all bands present in the original image collection?
  #compBands = composite.bandNames()
  #print('Composite bands:', compBands)
  
  # Add NDVI to image. Be sure to change out bands when switching between Landsat collections.
  #NDVIcomp = composite.normalizedDifference(colvalues[1]).rename('NDVI')
  NDVIcomp = composite.normalizedDifference(['B5', 'B4']).rename('NDVI')
  if fs==30:
     return NDVIcomp
  else: 
    texture = NDVIcomp.reduceNeighborhood(reducer= ee.Reducer.mean(),
                                           kernel= ee.Kernel.circle(radius=fs,
                                                                    units='meters')) 
    return texture
  # Check: Do the NDVI layers now only contain the band labeled 'NDVI'?
  #bandNames = NDVIcomp.bandNames()
  #print('Band names:', bandNames)

  #return NDVIcomp


# Use loops over years, start, end dates to pull images (only nd band).
for f in range(0,len(focalstats),1):
  for h in range(0, len(geolist), 1):
    for i in range(0, len(yrarr), 1):
        for j in range(0, 4, 1):
          print("Date: ",makeSt(yrarr[i])[j])

          img = GetImage(makeSt(yrarr[i])[j], makeEd(yrarr[i])[j],geolist[h],focalstats[f],Landsatcollections)
          if focalstats[f] == 30:
            imgndvi = img.select(['NDVI'])
          else:
             imgndvi = img.select(['NDVI_mean'])
          #print("hij loop: ",h,i,j)
          task =ee.batch.Export.image.toDrive(
              image= imgndvi,
              description= geonames[h]+"_"+str(focalstats[f])+"_"+makeSt(yrarr[i])[j],
              folder = "NDVI",
              region= geolist[h].geometry(),
              crs= 'EPSG:4326',
              fileFormat= 'GeoTIFF',
              scale= 30,
              maxPixels= 1e13,
              )
          task.start()
          time.sleep(2.5)
          #print(task.status())



while len([td for td in ee.data.getTaskList() if td["state"] in {'RUNNING','READY'}])>0:
   print("There are currently {} tasks in the queue. The following tasks are running: ".format(len([td for td in ee.data.getTaskList() if td["state"] in {'RUNNING','READY'}])))
   print(json.dumps([td for td in ee.data.getTaskList() if td["state"] in {'RUNNING'}],sort_keys=False,indent=4))
   print("")
   time.sleep(30)





