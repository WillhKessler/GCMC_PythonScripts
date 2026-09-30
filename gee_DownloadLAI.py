## Script for calculating Leaf Area Index (LAI) using MODIS Aqua/Terra 
# sensors at the native 500m resolution

import ee
import datetime
import time
import json

ee.Authenticate()
ee.Initialize(project="earthengine-leafareaindex")

########### REQUIRED INPUTS ###########################################################################
# Split a geometry into western and eastern halves at the midpoint of its bounding box.
# Uses only the geometry passed in (no globals), and derives the bounds by min/max
# rather than by hardcoding vertex positions.
def split_geometry(geometry):
    coords = geometry.bounds().getInfo()["coordinates"][0]
    xs = [c[0] for c in coords]
    ys = [c[1] for c in coords]
    xmin, xmax, ymin, ymax = min(xs), max(xs), min(ys), max(ys)
    xmid = (xmin + xmax) / 2
 
    west = ee.Geometry.Rectangle([xmin, ymin, xmid, ymax])
    east = ee.Geometry.Rectangle([xmid, ymin, xmax, ymax])
    return geometry.intersection(west), geometry.intersection(east)


Maine = ee.FeatureCollection("TIGER/2018/States").filter("NAME == 'Maine'")
Massachusetts = ee.FeatureCollection("TIGER/2018/States").filter(
    "NAME == 'Massachusetts'"
)
Michigan = ee.FeatureCollection("TIGER/2018/States").filter("NAME == 'Michigan'")
MiddleAtlantic = ee.FeatureCollection("TIGER/2018/States").filter(
    (ee.Filter.inList("NAME", ee.List(["New York", "Pennsylvania", "New Jersey"])))
)
NewEngland = ee.FeatureCollection("TIGER/2018/States").filter(
    (
        ee.Filter.inList(
            "NAME",
            ee.List(
                [
                    "Rhode Island",
                    "Connecticut",
                ]
            ),
        )
    )
)
NewHampshire = ee.FeatureCollection("TIGER/2018/States").filter(
    "NAME == 'New Hampshire'"
)
Vermont = ee.FeatureCollection("TIGER/2018/States").filter("NAME == 'Vermont'")
Wisconsin = ee.FeatureCollection("TIGER/2018/States").filter("NAME == 'Wisconsin'")
Illinois = ee.FeatureCollection("TIGER/2018/States").filter("NAME == 'Illinois'")
IndianaOhio = ee.FeatureCollection("TIGER/2018/States").filter(
    (ee.Filter.inList("NAME", ee.List(["Indiana", "Ohio"])))
)
KentuckyTennessee = ee.FeatureCollection("TIGER/2018/States").filter(
    (ee.Filter.inList("NAME", ee.List(["Kentucky", "Tennessee"])))
)
MississippiAlabama = ee.FeatureCollection("TIGER/2018/States").filter(
    (ee.Filter.inList("NAME", ee.List(["Mississippi", "Alabama"])))
)
NorthSouthDakota = ee.FeatureCollection("TIGER/2018/States").filter(
    (ee.Filter.inList("NAME", ee.List(["North Dakota", "South Dakota"])))
)
Minnesota = ee.FeatureCollection("TIGER/2018/States").filter("NAME == 'Minnesota'")
Nebraska = ee.FeatureCollection("TIGER/2018/States").filter("NAME == 'Nebraska'")
MissouriIowa = ee.FeatureCollection("TIGER/2018/States").filter(
    (ee.Filter.inList("NAME", ee.List(["Missouri", "Iowa"])))
)
Kansas = ee.FeatureCollection("TIGER/2018/States").filter("NAME == 'Kansas'")
SouthAtlantic1 = ee.FeatureCollection("TIGER/2018/States").filter(
    (
        ee.Filter.inList(
            "NAME",
            ee.List(
                [
                    "Delaware",
                    "Maryland",
                    "District of Columbia",
                    "Virginia",
                    "West Virginia",
                ]
            ),
        )
    )
)
SouthCarolinaGeorgia = ee.FeatureCollection("TIGER/2018/States").filter(
    (ee.Filter.inList("NAME", ee.List(["South Carolina", "Georgia"])))
)
Florida = ee.FeatureCollection("TIGER/2018/States").filter("NAME == 'Florida'")
Idaho = ee.FeatureCollection("TIGER/2018/States").filter("NAME == 'Idaho'")
Wyoming = ee.FeatureCollection("TIGER/2018/States").filter("NAME == 'Wyoming'")
Nevada = ee.FeatureCollection("TIGER/2018/States").filter("NAME == 'Nevada'")
Utah = ee.FeatureCollection("TIGER/2018/States").filter("NAME == 'Utah'")
Colorado = ee.FeatureCollection("TIGER/2018/States").filter("NAME == 'Colorado'")
Arizona = ee.FeatureCollection("TIGER/2018/States").filter("NAME == 'Arizona'")
NewMexico = ee.FeatureCollection("TIGER/2018/States").filter("NAME == 'New Mexico'")
Oklahoma = ee.FeatureCollection("TIGER/2018/States").filter("NAME == 'Oklahoma'")
ArkansasLouisiana = ee.FeatureCollection("TIGER/2018/States").filter(
    (ee.Filter.inList("NAME", ee.List(["Arkansas", "Louisiana"])))
)
WashingtonOregon = ee.FeatureCollection("TIGER/2018/States").filter(
    (ee.Filter.inList("NAME", ee.List(["Washington", "Oregon"])))
)

Montana = ee.FeatureCollection("TIGER/2018/States").filter("NAME=='Montana'")
# Get the geometry of Montana
montana_geometry = Montana.geometry()
# Apply the split function to the Montana geometry
mtpart1_geometry, mtpart2_geometry = split_geometry(montana_geometry)
# Create FeatureCollections for the two parts
MontanaPart1 = ee.FeatureCollection(ee.Feature(mtpart1_geometry))
MontanaPart2 = ee.FeatureCollection(ee.Feature(mtpart2_geometry))


NorthCarolina = ee.FeatureCollection("TIGER/2018/States").filter("NAME=='North Carolina'")
northcarolina_geometry = NorthCarolina.geometry()
# Apply the split function to the Montana geometry
ncpart1_geometry, ncpart2_geometry = split_geometry(northcarolina_geometry)
# Create FeatureCollections for the two parts
NorthCarolina1 = ee.FeatureCollection(ee.Feature(ncpart1_geometry))
NorthCarolina2 = ee.FeatureCollection(ee.Feature(ncpart2_geometry))

Texas = ee.FeatureCollection("TIGER/2018/States").filter("NAME=='Texas'")
texas_geometry = Texas.geometry()
# Apply the split function to the Montana geometry
txpart1_geometry, txpart2_geometry = split_geometry(texas_geometry)
# Create FeatureCollections for the two parts
Texas1 = ee.FeatureCollection(ee.Feature(txpart1_geometry))
Texas2 = ee.FeatureCollection(ee.Feature(txpart2_geometry))

CaliPart1 = ee.FeatureCollection(
    ee.Geometry.Polygon(
        [
            [
                [-124.28106008522997, 39.011877197055036],
                [-121.77617727272997, 35.872529247588176],
                [-118.17266164772997, 37.9454455156237],
                [-119.71074758522997, 39.011877197055036],
                [-119.79863821022997, 42.11569789655484],
                [-124.67656789772997, 42.14828779944612],
            ]
        ]
    )
)
CaliPart2 = ee.FeatureCollection(
    ee.Geometry.Polygon(
        [
            [
                [-121.1995373916721, 33.95836802805502],
                [-117.1565686416721, 32.19092509237336],
                [-114.4759045791721, 32.3395631495797],
                [-113.8167248916721, 34.10404365602724],
                [-114.2122327041721, 34.9728221294801],
                [-118.4309827041721, 38.183336938603134],
                [-122.0784436416721, 36.116993250699245],
            ]
        ]
    )
)


# Values to perform focal statistics. A value of 500 will return the native 500m resolution MODIS imagery
# focalstats = [500,1000]
focalstats = [500,1000]

# Specify years to create an array (with years as columns).
yrarr = ["2005"]

# yrarr = [
#     "2005",
#     "2006",
#     "2007",
#     "2008",
#     "2009",
#     "2010",
#     "2011",
#     "2012",
#     "2013",
#     "2014",
#     "2015",
#     "2016",
#     "2017",
#     "2018",
#     "2019",
#     "2020",
#     "2021",
#     "2022",
#     "2023",
# ]

# MODIS LAI Collection Years
MODIScollections = {
    "MODIS/061/MCD15A3H": [
        list(range(2004, int(datetime.date.today().year) + 1)),
        ["Lai"]],
    "MODIS/061/MOD15A2H":[
        list(range(2002, int(datetime.date.today().year) + 1)),
        ["Lai"]]
}

# Collection to use when a year is covered by more than one collection.
#   MCD15A3H = 4-day composite combining Terra + Aqua (more observations, fewer gaps)
#   MOD15A2H = 8-day Terra-only
# Do not merge the two: MCD15A3H already contains Terra data.
PREFERRED_COLLECTION = "MODIS/061/MCD15A3H"
 
# Optional stricter QC: also require CloudState == 0 (no significant clouds).
# This leaves more gaps, especially in winter at northern latitudes.
REQUIRE_CLEAR_SKY = False
 
# Optional: value to write in place of masked pixels in the exported GeoTIFFs.
# Leave as None to keep masked pixels as no-data. If you set a value (e.g. -9999),
# set the same value as the nodata value when reading the files.
NODATA_VALUE = None

#######################################################################################################
## Code to pull seasonal NDVI from MODIS 8 data with cloud mask for contiguous United States
geolist = [
    Maine,
    Massachusetts,
    Vermont,
    NewHampshire,
    NewEngland,
    MiddleAtlantic,
    Wisconsin,
    Michigan,
    Illinois,
    IndianaOhio,
    KentuckyTennessee,
    MississippiAlabama,
    NorthSouthDakota,
    Minnesota,
    Nebraska,
    MissouriIowa,
    Kansas,
    SouthAtlantic1,
    SouthCarolinaGeorgia,
    Florida,
    Idaho,
    Wyoming,
    Nevada,
    Utah,
    Colorado,
    Arizona,
    NewMexico,
    Oklahoma,
    ArkansasLouisiana,
    WashingtonOregon,
    MontanaPart1,
    MontanaPart2,
    NorthCarolina1,
    NorthCarolina2,
    Texas1,
    Texas2,
    CaliPart1,
    CaliPart2,
]

geonames = [
    "Maine",
    "Massachusetts",
    "Vermont",
    "NewHampshire",
    "NewEngland",
    "MiddleAtlantic",
    "Wisconsin",
    "Michigan",
    "Illinois",
    "IndianaOhio",
    "KentuckyTennessee",
    "MississippiAlabama",
    "NorthSouthDakota",
    "Minnesota",
    "Nebraska",
    "MissouriIowa",
    "Kansas",
    "SouthAtlantic1",
    "SouthCarolinaGeorgia",
    "Florida",
    "Idaho",
    "Wyoming",
    "Nevada",
    "Utah",
    "Colorado",
    "Arizona",
    "NewMexico",
    "Oklahoma",
    "ArkansasLouisiana",
    "WashingtonOregon",
    "MontanaPart1",
    "MontanaPart2",
    "NorthCarolina1",
    "NorthCarolina2",
    "Texas1",
    "Texas2",
    "CaliPart1",
    "CaliPart2",
]
geoindex=list(range(0,1))
#geoindex = list(range(0, 39))  # 0:38
geolist = [geolist[i] for i in geoindex]
geonames = [geonames[i] for i in geoindex]



# Populate array with start dates in the format of year-mo-day by season.
def makeSt(yr):
    stArr = [yr + "-01-01", yr + "-04-01", yr + "-07-01", yr + "-10-01"]
    return stArr


# Populate array by with end dates by season.
def makeEd(yr):
    eArr = [yr + "-03-31", yr + "-06-30", yr + "-09-30", yr + "-12-31"]
    return eArr


# Function to determine the correct collection and parameters
def determineCol(dictionary, start, end):
    colls_containing_dates = []

    for key, value in dictionary.items():
        if start.year in value[0]:
            colls_containing_dates.append(key)
    return colls_containing_dates

# Explicitly choose a collection when more than one covers the requested year.
def pick_collection(dictionary, start, end, prefer=PREFERRED_COLLECTION):
    candidates = determineCol(dictionary, start, end)
    if not candidates:
        raise ValueError("No MODIS LAI collection covers year {}".format(start.year))
    return prefer if prefer in candidates else candidates[0]
 

# Per-image preparation: select Lai, mask fill values and low-quality retrievals,
# apply the 0.1 scale factor, and name the band "LAI".
def prep_lai(img):
    lai = img.select("Lai")
    qc = img.select("FparLai_QC")
 
    # Valid Lai is 0-100; 249-255 are fill values (water, barren, urban, snow/ice, no retrieval)
    valid = lai.lte(100)
 
    # FparLai_QC bits 5-7 = SCF_QC:
    # 0 = main method, best result; 1 = main method, saturated
    # (2+ = backup algorithm or no retrieval)
    main_method = qc.rightShift(5).bitwiseAnd(7).lte(1)
 
    mask = valid.And(main_method)
 
    if REQUIRE_CLEAR_SKY:
        # FparLai_QC bits 3-4 = CloudState; 0 = no significant clouds
        clear = qc.rightShift(3).bitwiseAnd(3).eq(0)
        mask = mask.And(clear)
 
    return (
        lai.updateMask(mask)
        .multiply(0.1)  # scale factor -> m2/m2
        .rename("LAI")
        .copyProperties(img, ["system:time_start"])
    )
 

# Create a function to:
# Pull MODIS scenes between start and end dates as defined above.
def GetImage(bdt, edt, geo, fs, col):
    start = datetime.datetime.strptime(bdt, "%Y-%m-%d").date()
    end = datetime.datetime.strptime(edt, "%Y-%m-%d").date()
    colkey = pick_collection(col, start, end, prefer=PREFERRED_COLLECTION)


    # Load a raw MODIS ImageCollection for a single year and filter temporally and spatially.
    # Change to match region of interest.
    collection = (
        ee.ImageCollection(colkey)
        .filterDate(ee.Date(bdt), ee.Date(edt))
        .filterBounds(geo)
        .select(["Lai","FparLai_QC"])
    )

    
        # Mask/scale each image, then average to a single seasonal LAI composite.
    # Masked pixels are ignored by mean(). The band is already named "LAI".
    LAIcomp = collection.map(prep_lai).mean()
    if fs == 500:
        return LAIcomp
    else:
        # reduceNeighborhood appends "_mean" to band names -> "LAI_mean"
        texture = LAIcomp.reduceNeighborhood(
            reducer=ee.Reducer.mean(),
            kernel=ee.Kernel.circle(radius=fs, units="meters"),
        )
        return texture


export_folder='LAI'
# Use loops over years, start, end dates to pull images (only nd band).
for f in range(0, len(focalstats), 1):
    for h in range(0, len(geolist), 1):
        for i in range(0, len(yrarr), 1):
            for j in range(0, 4, 1):
                print("Date: ", makeSt(yrarr[i])[j])

                img = GetImage(
                    makeSt(yrarr[i])[j],
                    makeEd(yrarr[i])[j],
                    geolist[h],
                    focalstats[f],
                    MODIScollections,
                )
                if focalstats[f] == 500:
                    imglai = img.select(["LAI"])
                else:
                    imglai = img.select(["LAI_mean"])

                if NODATA_VALUE is not None:
                    imglai = imglai.unmask(NODATA_VALUE)

                #print("hij loop: ", h, i, j)
                task = ee.batch.Export.image.toDrive(
                    image=imglai,
                    description=geonames[h]
                    + "_"
                    + str(focalstats[f])
                    + "_"
                    + makeSt(yrarr[i])[j],
                    folder=export_folder,
                    region=geolist[h].geometry(),
                    crs="EPSG:4326",
                    fileFormat="GeoTIFF",
                    scale=500,
                    maxPixels=1e13,
                )
                task.start()
                time.sleep(0.5)


while True:
    active = [td for td in ee.data.getTaskList() if td["state"] in {"READY", "RUNNING"}]
    if not active:
        break
    print("{} tasks still queued or running".format(len(active)))
    time.sleep(300)
