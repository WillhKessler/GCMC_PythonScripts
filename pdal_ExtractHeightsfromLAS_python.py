""" Using the preformated JSON pipeline, 
loop through a python script to download chunked LAS files based on a fishnet geometry file """

import geopandas as gpd
import pandas as pd
import numpy as np
import shapely.geometry
import pdal, json, requests, urllib.parse, geojson, mercantile, tempfile
from tqdm import tqdm
from urllib.request import urlopen


#############################################################################################
# AOIName
name = "Boston"
print(name)
# Define Output Directory
outputdirectory = r"S:\GCMC\Data\LiDAR\{}LAS".format(name)

## Define Lidar Source
lidarsource = "http://usgs-lidar-public.s3.amazonaws.com/MA_CentralEastern_1_2021/ept.json"  # Boston

# ## 3DEP Lidar Footprints
# projecturl = "https://index.nationalmap.gov/arcgis/rest/services/3DEPElevationIndex/MapServer/24/query?"
# params = {"workunit": "MA_CentralEastern_1_2021"}
# projecturl = projecturl + urllib.parse.urlencode(params)
# response = requests.get(url=projecturl)
# data = response.text
# gdf_temp = gpd.read_file(data)
bounds = [
    [
        [-71.19045, 42.3799],
        [-71.19, 42.38069],
        [-71.18870, 42.38068],
        [-71.18901, 42.37966],
    ]
]


######--------------- Read in the Building Footprints
#########Define our area of interest, a LIDAR project (AOI)
aoi_geom = {
    "coordinates": bounds,
    "type": "Polygon",
}
aoi_shape = shapely.geometry.shape(aoi_geom)
minx, miny, maxx, maxy = aoi_shape.bounds
aoi_gdf = gpd.GeoSeries(aoi_shape, crs=4326)
output_fn = "example_building_footprints.geojson"

# ###############Determine which tiles intersect our AOI
quad_keys = set()
for tile in list(mercantile.tiles(minx, miny, maxx, maxy, zooms=9)):
    quad_keys.add(int(mercantile.quadkey(tile)))
quad_keys = list(quad_keys)
print(f"The input area spans {len(quad_keys)} tiles: {quad_keys}")

# ########Step 3 - Download the building footprints for each tile that intersects our AOI and crop the results
df = pd.read_csv(
    "https://minedbuildings.blob.core.windows.net/global-buildings/dataset-links.csv"
)

idx = 0
combined_rows = []
geodf = gpd.GeoDataFrame(columns=["id", "geometry"], geometry="geometry", crs=4326)

with tempfile.TemporaryDirectory() as tmpdir:
    # Download the GeoJSON files for each tile that intersects the input geometry
    tmp_fns = []
    for quad_key in tqdm(quad_keys):
        rows = df[df["QuadKey"] == quad_key]
        if rows.shape[0] == 1:
            url = rows.iloc[0]["Url"]

            df2 = pd.read_json(url, lines=True)
            df2["geometry"] = df2["geometry"].apply(shapely.geometry.shape)

            gdf = gpd.GeoDataFrame(df2, crs=4326)

        elif rows.shape[0] > 1:
            raise ValueError(f"Multiple rows found for QuadKey: {quad_key}")
        else:
            raise ValueError(f"QuadKey not found in dataset: {quad_key}")
        geodf = pd.concat([geodf, gdf])

### Clip Footprints to AOI
geodf = geodf.cx[
    aoi_gdf.total_bounds[0] : aoi_gdf.total_bounds[2],
    aoi_gdf.total_bounds[1] : aoi_gdf.total_bounds[3],
]

# #Build the GeoDataFrame

geodf["properties"].apply(
    lambda x: x.update(
        {
            "minHAG": np.nan,
            "minHAG25": np.nan,
            "maxHAG": np.nan,
            "maxHAG25": np.nan,
            "meanHAG": np.nan,
            "meanHAG25": np.nan,
            "medHAG": np.nan,
            "medHAG25": np.nan,
            "stdevHAG25": np.nan,
            "q1HAG": np.nan,
            "q1HAG25": np.nan,
            "q3HAG": np.nan,
            "q3HAG25": np.nan,
            "ground": np.nan,
            "heightobs": np.nan,
            "heightobs25": np.nan,
        }
    )
)
# print(geodf)
gdf = geodf.to_crs(3857)
print(gdf)


##################
gdf["innerbuffer"] = gdf.geometry.buffer(-1)
gdf["fpbuffer"] = gdf.geometry.buffer(3)
#############################################################################################
# change the global options that Geopandas inherits from
pd.set_option("display.max_columns", None)


# row = gpd.GeoDataFrame(gdf.iloc[[0]],crs=3857)
# for count, row in gdf.iterfeatures():
def calcHeight(x):
    row = gpd.GeoDataFrame(pd.DataFrame(x).transpose(), crs=3857)
    row["fpbuffer"] = gpd.GeoSeries(row["fpbuffer"], crs=3857)
    row["innerbuffer"] = gpd.GeoSeries(row["innerbuffer"], crs=3857)

    footprintWKT = row.geometry.to_wkt()
    # print("FootprintWKT",footprintWKT)
    # print("FootprintWKT: ",str(footprintWKT))

    bufferbounds = (
        [row["fpbuffer"].total_bounds[0], row["fpbuffer"].total_bounds[2]],
        [row["fpbuffer"].total_bounds[1], row["fpbuffer"].total_bounds[3]],
    )

    innerbounds = (
        [row["innerbuffer"].total_bounds[0], row["innerbuffer"].total_bounds[2]],
        [row["innerbuffer"].total_bounds[1], row["innerbuffer"].total_bounds[3]],
    )

    # print("The buffer bounds: ", bufferbounds)

    ## Using the standard EPT LAS pipeline format, reprojecting the output to the same EPSG as the AOI
    rawLAS = json.dumps(
        [
            {
                "type": "readers.ept",
                "filename": lidarsource,
                "bounds": str(bufferbounds),
            },
            # {
            #     "filename":outputdirectory+"\\"+name+"rawtestlasAOI_{}.las".format("a"+str(1))
            # }
        ]
    )

    rawpipeline = pdal.Pipeline(rawLAS)
    rawpipeline.execute()
    rawarray = rawpipeline.arrays
    # print(rawarray)
    rawarray = np.ma.masked_where(
        rawarray[0][["Classification"]]["Classification"] != 2, rawarray[0][["Z"]]["Z"]
    ).filled(np.nan)

    ## Run Pipeline on feature

    LAStiles = json.dumps(
        [
            {
                "type": "readers.ept",
                "filename": lidarsource,
                "bounds": str(bufferbounds),
            },
            {
                "type": "filters.hag_nn",
                "allow_extrapolation": "true",
            },
            {
                "type": "filters.expression",
                "expression": "(!(Classification ==2 || Classification ==3 || Classification ==4 || Classification ==5 || Classification ==7 || Classification ==18) && HeightAboveGround>1)",
            },
            {"type": "filters.crop", "polygon": footprintWKT.iloc[0]},
            # {
            #     "filename":outputdirectory+"\\"+name+"testlasAOI_{}.las".format("a"+str(count))
            # }
        ]
    )

    pipeline = pdal.Pipeline(LAStiles)
    pipeline.execute()
    arrays = pipeline.arrays

    ############Perform calculations on array and append to feature
    if arrays[0][["HeightAboveGround"]]["HeightAboveGround"].size > 0:
        top25array = np.ma.masked_where(
            arrays[0][["HeightAboveGround"]]["HeightAboveGround"]
            < np.percentile(arrays[0][["HeightAboveGround"]]["HeightAboveGround"], 25),
            arrays[0][["HeightAboveGround"]]["HeightAboveGround"],
        ).filled(np.nan)
        heightarray = arrays[0][["HeightAboveGround"]]["HeightAboveGround"]
        groundarray = rawarray

        # Calculate Sample Size
        heightobs = np.sum(~np.isnan(heightarray))
        heightobs25 = np.sum(~np.isnan(top25array))

        # Calculate Min/Max Values
        meanground = np.nanmean(groundarray)
        minHAG = np.nanmin(heightarray)
        min25HAG = np.nanmin(top25array)
        maxHAG = np.nanmax(heightarray)
        max25HAG = np.nanmax(top25array)

        # Calculate Mean Values
        meanHAG = np.nanmean(heightarray)
        mean25HAG = np.nanmean(top25array)
        meanZ = np.nanmean(arrays[0][["Z"]]["Z"])

        # Calculate Standard Deviations
        stdHAG = np.nanstd(heightarray)
        std25HAG = np.nanstd(top25array)

        # Calculate Median Values
        medHAG = np.nanmedian(heightarray)
        med25HAG = np.nanmedian(top25array)

        # Calculate Quartile 1
        q3HAG, q1HAG = np.nanpercentile(heightarray, [75, 25])
        q325HAG, q125HAG = np.nanpercentile(top25array, [75, 25])

    else:
        heightobs = np.nan
        heightobs25 = np.nan
        meanground = np.nan
        minHAG = np.nan
        min25HAG = np.nan
        maxHAG = np.nan
        max25HAG = np.nan
        meanHAG = np.nan
        mean25HAG = np.nan
        medHAG = np.nan
        med25HAG = np.nan
        stdHAG = np.nan
        std25HAG = np.nan
        q1HAG = np.nan
        q125HAG = np.nan
        q3HAG = np.nan
        q325HAG = np.nan

    return {
        "minHAG": minHAG,
        "min25HAG": min25HAG,
        "maxHAG": maxHAG,
        "max25HAG": max25HAG,
        "meanHAG": meanHAG,
        "mean25HAG": mean25HAG,
        "medHAG": medHAG,
        "med25HAG": med25HAG,
        "stdHAG": stdHAG,
        "std25HAG": std25HAG,
        "q1HAG": q1HAG,
        "q125HAG": q125HAG,
        "q3HAG": q3HAG,
        "q325HAG": q325HAG,
        "meanground": meanground,
        "heightobs": heightobs,
        "heightobs25": heightobs25,
    }


gdf.apply(lambda x: x["properties"].update(calcHeight(x)), axis=1)
gdf.reset_index(inplace=True)
gdf = gdf.join(pd.json_normalize(gdf.pop("properties")))
gdf = gdf.drop(columns=["innerbuffer", "fpbuffer"])
gdf.to_file(r"S:\GCMC\_Code\temp\building.shp")
