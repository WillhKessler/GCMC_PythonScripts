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
####### INPUTS
name = "Boston"
print(name)
## Define Output Directory
# outputdirectory = r"S:\GCMC\Data\LiDAR\{}LAS".format(name)
## Define Lidar Source
lidarsource = "http://usgs-lidar-public.s3.amazonaws.com/MA_CentralEastern_1_2021/ept.json"  # Boston
## Read Building Footprints:
buildingFootprints_path = r"{}_ParcelBuildings.shp".format(name)

outputfile = r"{}_ParcelBuilding_Heights.shp"

#########################
buildingFootprints = gpd.read_file(buildingFootprints_path)


buildingFootprints.head()
buildingFootprints.crs

# #Build the GeoDataFrame
geodf = buildingFootprints

geodf = geodf.assign(
    minHAG=lambda x: np.nan,
    minHAG25=lambda x: np.nan,
    maxHAG=lambda x: np.nan,
    maxHAG25=lambda x: np.nan,
    meanHAG=lambda x: np.nan,
    meanHAG25=lambda x: np.nan,
    medHAG=lambda x: np.nan,
    medHAG25=lambda x: np.nan,
    stddevHAG=lambda x: np.nan,
    stdevHAG25=lambda x: np.nan,
    q1HAG=lambda x: np.nan,
    q1HAG25=lambda x: np.nan,
    q3HAG=lambda x: np.nan,
    q3HAG25=lambda x: np.nan,
    ground=lambda x: np.nan,
    heightobs=lambda x: np.nan,
    heightobs25=lambda x: np.nan,
    lidarderived=lambda x: "failure",
)


gdf = geodf.to_crs(3857)
print(gdf)


gdf = geodf.to_crs(3857)
# print(gdf)
##################
gdf["innerbuffer"] = gdf.geometry.buffer(-1)
gdf["fpbuffer"] = gdf.geometry.buffer(3)
#############################################################################################
# change the global options that Geopandas inherits from
pd.set_option("display.max_columns", None)


def handleError(x):
    try:
        return calcHeight(x)
    except pdal.pdal_error:
        return {
            "minHAG": np.nan,
            "minHAG25": np.nan,
            "maxHAG": np.nan,
            "maxHAG25": np.nan,
            "meanHAG": np.nan,
            "meanHAG25": np.nan,
            "medHAG": np.nan,
            "medHAG25": np.nan,
            "stdHAG": np.nan,
            "stdevHAG25": np.nan,
            "q1HAG": np.nan,
            "q1HAG25": np.nan,
            "q3HAG": np.nan,
            "q3HAG25": np.nan,
            "ground": np.nan,
            "heightobs": np.nan,
            "heightobs25": np.nan,
            "lidarderived": pdal.pdal_error,
        }


#
def calcHeight(x):
    print("running feature: ", x)
    row = gpd.GeoDataFrame(pd.DataFrame(x).transpose(), crs=3857)
    row["fpbuffer"] = gpd.GeoSeries(row["fpbuffer"], crs=3857)
    row["innerbuffer"] = gpd.GeoSeries(row["innerbuffer"], crs=3857)

    footprintWKT = row.geometry.to_wkt()

    bufferbounds = (
        [row["fpbuffer"].total_bounds[0], row["fpbuffer"].total_bounds[2]],
        [row["fpbuffer"].total_bounds[1], row["fpbuffer"].total_bounds[3]],
    )

    # Using the standard EPT LAS pipeline format, reprojecting the output to the same EPSG as the AOI
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
        "minHAG25": min25HAG,
        "maxHAG": maxHAG,
        "maxHAG25": max25HAG,
        "meanHAG": meanHAG,
        "meanHAG25": mean25HAG,
        "medHAG": medHAG,
        "medHAG25": med25HAG,
        "stdHAG": stdHAG,
        "stdevHAG25": std25HAG,
        "q1HAG": q1HAG,
        "q1HAG25": q125HAG,
        "q3HAG": q3HAG,
        "q3HAG25": q325HAG,
        "ground": meanground,
        "heightobs": heightobs,
        "heightobs25": heightobs25,
        "lidarderived": "success",
    }


print("beginning the apply and update loop")
gdf.update(gdf.apply(lambda x: pd.Series(calcHeight(x)), axis=1), overwrite=True)
gdf.reset_index(inplace=True)

gdf = gdf.drop(columns=["innerbuffer", "fpbuffer"])
gdf.to_file(outputfile)
