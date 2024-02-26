""" Using the preformated JSON pipeline, 
loop through a python script to download chunked LAS files based on a fishnet geometry file """


import pandas as pd
import json
import pdal
import arcpy
import numpy

#############################################################################################
# AOIName
name = "Austin"
# Define Output Directory
outputdirectory = r"S:\GCMC\Data\LiDAR\{}LAS".format(name)
## Define Lidar Source
# lidarsource = "http://usgs-lidar-public.s3.amazonaws.com/MA_CentralEastern_1_2021/ept.json"  # Boston
lidarsource = "http://usgs-lidar-public.s3.amazonaws.com/USGS_LPC_TX_Central_B1_2017_LAS_2019/ept.json"  # Austin
# lidarsource = "http://usgs-lidar-public.s3.amazonaws.com/CT_Statewide_B1_2016/ept.json" # New Haven


# Read in the shapefile of the AOI
buildingFootprints = r"C:\Users\wik191\OneDrive - Harvard University\Projects\Haber\Buildings_Parcels.gdb\{}_ParcelBuildings".format(
    name
)
# buildingFootprints = r'C:\Users\wik191\OneDrive - Harvard University\Projects\Haber\Buildings_Parcels.gdb\{}_ParcelBuildings_WebMerc'.format(name)
#############################################################################################
webmercfc = buildingFootprints

# Pull the input projection system. This is used as the output epsg
epsg = arcpy.Describe(buildingFootprints).spatialReference.factoryCode

print("epsg: " + str(epsg))
outputfc = r"C:\Users\wik191\OneDrive - Harvard University\Projects\Haber\Buildings_Parcels.gdb\{}_ParcelBuildings_WebMerc".format(
    name
)
if epsg != 3857 & arcpy.Exists(outputfc) == False:
    arcpy.Project_management(
        in_dataset=buildingFootprints, out_dataset=outputfc, out_coor_system="3857"
    )

    webmercfc = outputfc
elif arcpy.Exists(outputfc):
    webmercfc = outputfc

arcpy.management.AddField(webmercfc, "minHAG", "FLOAT")
arcpy.management.AddField(webmercfc, "minHAG25", "FLOAT")
arcpy.management.AddField(webmercfc, "maxHAG", "FLOAT")
arcpy.management.AddField(webmercfc, "maxHAG25", "FLOAT")
arcpy.management.AddField(webmercfc, "meanHAG", "FLOAT")
arcpy.management.AddField(webmercfc, "meanHAG25", "FLOAT")
arcpy.management.AddField(webmercfc, "medHAG", "FLOAT")
arcpy.management.AddField(webmercfc, "medHAG25", "FLOAT")
arcpy.management.AddField(webmercfc, "stdevHAG", "FLOAT")
arcpy.management.AddField(webmercfc, "stdevHAG25", "FLOAT")
arcpy.management.AddField(webmercfc, "q1HAG", "FLOAT")
arcpy.management.AddField(webmercfc, "q1HAG25", "FLOAT")
arcpy.management.AddField(webmercfc, "q3HAG", "FLOAT")
arcpy.management.AddField(webmercfc, "q3HAG25", "FLOAT")
arcpy.management.AddField(webmercfc, "ground", "FLOAT")
arcpy.management.AddField(webmercfc, "heightobs", "FLOAT")
arcpy.management.AddField(webmercfc, "heightobs25", "FLOAT")


## Loop through each cell in the input featureclass grid. Create a PDAL EPT pipeline and execute.

rowcount = len(list(i for i in arcpy.da.SearchCursor(webmercfc, "SHAPE@")))
# with arcpy.da.UpdateCursor(webmercfc,["SHAPE@","SHAPE@WKT","HAG","HAG10"],'"OBJECTID" IN (422,423,424)') as cursor:
# with arcpy.da.UpdateCursor(webmercfc,["SHAPE@","SHAPE@WKT","HAG","HAG10"],'"OBJECTID" IN ({})'.format(specfeatures)) as cursor:
with arcpy.da.UpdateCursor(
    webmercfc,
    [
        "SHAPE@",
        "SHAPE@WKT",
        "minHAG",
        "minHAG25",
        "maxHAG",
        "maxHAG25",
        "meanHAG",
        "meanHAG25",
        "medHAG",
        "medHAG25",
        "stdevHAG",
        "stdevHAG25",
        "q1HAG",
        "q1HAG25",
        "q3HAG",
        "q3HAG25",
        "ground",
        "heightobs",
        "heightobs25",
    ],
) as cursor:
    # print(rowcount)
    # row = cursor.next()
    for count, row in enumerate(cursor):
        print("Processing Feature ", count + 1, " of ", rowcount)
        footprintbuffer = row[0].buffer(-1)

        footprintWKT = row[1]
        footprintWKT = footprintbuffer.WKT

        fpbuffer = row[0].buffer(3)
        bufferbounds = (
            [fpbuffer.extent.XMin, fpbuffer.extent.XMax],
            [fpbuffer.extent.YMin, fpbuffer.extent.YMax],
        )
        # print("The bounds: ", bounds)

        ## Using the standard EPT LAS pipeline format, reprojecting the output to the same EPSG as the AOI
        rawLAS = json.dumps(
            [
                {
                    "type": "readers.ept",
                    "filename": lidarsource,
                    "bounds": str(bufferbounds),
                },
                {
                    "filename": outputdirectory
                    + "\\"
                    + name
                    + "rawtestlasAOI_{}.las".format("a" + str(count))
                },
            ]
        )

        rawpipeline = pdal.Pipeline(rawLAS)
        rawpipeline.execute()
        rawarray = rawpipeline.arrays
        # print(rawarray)
        rawarray = numpy.ma.masked_where(
            rawarray[0][["Classification"]]["Classification"] != 2,
            rawarray[0][["Z"]]["Z"],
        ).filled(numpy.nan)

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
                {"type": "filters.crop", "polygon": footprintWKT},
                {
                    "filename": outputdirectory
                    + "\\"
                    + name
                    + "testlasAOI_{}.las".format("a" + str(count))
                },
            ]
        )

        pipeline = pdal.Pipeline(LAStiles)
        pipeline.execute()
        arrays = pipeline.arrays

        if arrays[0][["HeightAboveGround"]]["HeightAboveGround"].size > 0:
            top25array = numpy.ma.masked_where(
                arrays[0][["HeightAboveGround"]]["HeightAboveGround"]
                < numpy.percentile(
                    arrays[0][["HeightAboveGround"]]["HeightAboveGround"], 25
                ),
                arrays[0][["HeightAboveGround"]]["HeightAboveGround"],
            ).filled(numpy.nan)
            heightarray = arrays[0][["HeightAboveGround"]]["HeightAboveGround"]
            groundarray = rawarray

            # Calculate Sample Size
            heightobs = numpy.sum(~numpy.isnan(heightarray))
            heightobs25 = numpy.sum(~numpy.isnan(top25array))

            # Calculate Min/Max Values
            meanground = numpy.nanmean(groundarray)
            minHAG = numpy.nanmin(heightarray)
            min25HAG = numpy.nanmin(top25array)
            maxHAG = numpy.nanmax(heightarray)
            max25HAG = numpy.nanmax(top25array)

            # Calculate Mean Values
            meanHAG = numpy.nanmean(heightarray)
            mean25HAG = numpy.nanmean(top25array)
            meanZ = numpy.nanmean(arrays[0][["Z"]]["Z"])

            # Calculate Standard Deviations
            stdHAG = numpy.nanstd(heightarray)
            std25HAG = numpy.nanstd(top25array)

            # Calculate Median Values
            medHAG = numpy.nanmedian(heightarray)
            med25HAG = numpy.nanmedian(top25array)

            # Calculate Quartile 1
            q3HAG, q1HAG = numpy.nanpercentile(heightarray, [75, 25])
            q325HAG, q125HAG = numpy.nanpercentile(top25array, [75, 25])

        else:
            heightobs = numpy.nan
            heightobs25 = numpy.nan
            meanground = numpy.nan
            minHAG = numpy.nan
            min25HAG = numpy.nan
            maxHAG = numpy.nan
            max25HAG = numpy.nan
            meanHAG = numpy.nan
            mean25HAG = numpy.nan
            medHAG = numpy.nan
            med25HAG = numpy.nan
            stdHAG = numpy.nan
            std25HAG = numpy.nan
            q1HAG = numpy.nan
            q125HAG = numpy.nan
            q3HAG = numpy.nan
            q325HAG = numpy.nan

        row[2] = minHAG
        row[3] = min25HAG
        row[4] = maxHAG
        row[5] = max25HAG
        row[6] = meanHAG
        row[7] = mean25HAG
        row[8] = medHAG
        row[9] = med25HAG
        row[10] = stdHAG
        row[11] = std25HAG
        row[12] = q1HAG
        row[13] = q125HAG
        row[14] = q3HAG
        row[15] = q325HAG
        row[16] = meanground
        row[17] = heightobs
        row[18] = heightobs25

        cursor.updateRow(row)
