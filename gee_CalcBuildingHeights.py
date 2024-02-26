import ee 
import time
import json
ee.Initialize()
######## INPUTS ############################################
#------Building Footprints

objects = ee.data.listAssets({'parent':'projects/sat-io/open-datasets/MSBuildings/US'})['assets']
print(objects)
#print(ee.FeatureCollection('projects/sat-io/open-datasets/MSBuildings/US/Alabama').size())

#------Raster Data
#--Bare Earth Elevation
NASADEM = ee.Image('USGS/3DEP/10m')
elevation = NASADEM.select('elevation')
NASADEM = ee.ImageCollection([elevation])
elev = NASADEM.mosaic().setDefaultProjection('EPSG:3857',None,30).set("system:time_start",ee.Date('2000-01-01').millis())

#--DSM - Building/Veg heights
glo30 = ee.ImageCollection("COPERNICUS/DEM/GLO30")
dsmelev = glo30.mosaic().setDefaultProjection('EPSG:3857',None,30).set("system:time_start",ee.Date('2000-01-01').millis())

#------Wrap the single image in an ImageCollection for use in the zonalStats function:
topo = ee.Image.cat(elev,dsmelev).set('system:time_start', ee.Date('2000-01-01').millis())
#topoCol = ee.ImageCollection([topo])
############################################################
#for index,state in enumerate(objects):
state=objects[0]
sname = state['name'].split("/")[-1]
print('Currently Processing: '+ sname)
chunksize = 100000
collectionsize = ee.FeatureCollection(state['id']).size()
chunklist = ee.List.sequence(
  start=0,
  end=ee.Number(collectionsize.divide(chunksize)).ceil().multiply(chunksize),
  step=chunksize)
chunklist = chunklist.replace(chunklist.get(-1),collectionsize)
print("Splitting the current State into "+str(chunklist.length())+" chunks")
#print("Starting index for each chunk: "+chunklist)
print('')

def reducefunction(fc,i):
  buildings = topo.reduceRegions(
    collection=fc, 
    reducer = ee.Reducer.mean())
    
  task = ee.batch.Export.table.toDrive(
    collection= buildings,
    description='MSBingBuildings_hEST'+"_"+sname+str(i),
    folder="BuildingFootprints",
    fileFormat= 'SHP')
  task.start()
  

for index,value in enumerate(chunklist):
  reducefunction(fc=value,i=index)
  time.sleep(0.5)
  
  # buildings = topo.reduceRegions(
  #     collection=ee.FeatureCollection(state['id']), 
  #     reducer = ee.Reducer.mean())
          

  #   task = ee.batch.Export.table.toDrive(
  #       collection= buildings,
  #       description='MSBingBuildings_hEST'+"_"+sname,
  #       folder="BuildingFootprints",
  #       fileFormat= 'SHP'
  #       )
  #   task.start()
  


######################################
"""
while len([td for td in ee.data.getTaskList() if td["state"] in {'RUNNING','READY'}])>0:
   print("There are currently {} tasks in the queue. The following tasks are running: ".format(len([td for td in ee.data.getTaskList() if td["state"] in {'RUNNING','READY'}])))
   print(json.dumps([td for td in ee.data.getTaskList() if td["state"] in {'RUNNING'}],sort_keys=False,indent=4))
   print("")
   time.sleep(30)
"""