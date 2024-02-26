import arcpy, itertools
from itertools import product
import numpy as np
import os.path

db = r"C:\Users\wik191\OneDrive - Harvard University\Projects\Delaney- HUC12 CSOs\HUC12_CSOs\HUC12_CSOs.gdb"
tracenet = r"C:\Users\wik191\OneDrive - Harvard University\Projects\Delaney- HUC12 CSOs\HUC12_CSOs\HUC12_CSOs.gdb\MA_TraceNetwork\NHDTrace"
CSOs = r"C:\Users\wik191\OneDrive - Harvard University\Projects\Delaney- HUC12 CSOs\HUC12_CSOs\HUC12_CSOs.gdb\CSOOutfalls_MAStatePlane"
direction = ["UPSTREAM", "DOWNSTREAM"]
distance = [5000, 10000]
combos = list(product(direction, distance))

arcpy.env.overwriteOutput = True

print(
    os.path.join(
        r"C:\Users\wik191\OneDrive - Harvard University\Projects\Delaney- HUC12 CSOs\MyProject\MyProject.gdb",
        "startingpoint",
    )
)


# arcpy.tn.Trace(
#     in_trace_network=tracenet,
#     trace_type="UPSTREAM",
#     starting_points=os.path.join(db, "teststart"),
#     barriers=r"C:\Users\wik191\OneDrive - Harvard University\Projects\Delaney- HUC12 CSOs\HUC12_CSOs\HUC12_CSOs.gdb\TN_Temp_Barriers",
#     path_direction="NO_DIRECTION",
#     shortest_path_network_attribute_name="",
#     include_barriers="INCLUDE_BARRIERS",
#     validate_consistency="VALIDATE_CONSISTENCY",
#     ignore_barriers_at_starting_points="DO_NOT_IGNORE_BARRIERS_AT_STARTING_POINTS",
#     allow_indeterminate_flow="IGNORE_INDETERMINATE_FLOW",
#     condition_barriers=None,
#     function_barriers="ADD 'Shape length' IS_GREATER_THAN 5000 true",
#     traversability_scope="BOTH_JUNCTIONS_AND_EDGES",
#     functions=None,
#     output_conditions=None,
#     result_types="AGGREGATED_GEOMETRY",
#     selection_type="NEW_SELECTION",
#     clear_all_previous_trace_results="CLEAR_ALL_PREVIOUS_TRACE_RESULTS",
#     trace_name="",
#     aggregated_points=os.path.join(db, "test_Results_Aggregated_Points"),
#     aggregated_lines=os.path.join(db, "test_Results_Aggregated_Lines"),
#     out_network_layer=None,
#     use_trace_config="DO_NOT_USE_TRACE_CONFIGURATION",
#     trace_config_name="",
#     out_json_file=None,
# )
##############################
for comb in combos:
    print(comb)

    ## Create an empty FeatureClass to hold the traces
    if arcpy.Exists(
        os.path.join(db, "trace_{dir}_{dist}km").format(dir=comb[0], dist=str(comb[1]))
    ):
        print("TRUE")
    else:
        traceoutputs = arcpy.management.CreateFeatureclass(
            out_path=db,
            out_name="trace_{dir}_{dist}km".format(dir=comb[0], dist=str(comb[1])),
            geometry_type="POLYLINE",
            template=CSOs,
            spatial_reference=CSOs,
        )

    ## Loop through each feature in the CSO dataset
    with arcpy.da.SearchCursor(CSOs, field_names="ObjectID") as cursor:
        # row = cursor.next()
        for row in cursor:
            print("ObjectID: ", row)
            whereclause = """OBJECTID = {}""".format(row[0])
            tnetwork = tracenet

            ## Generate Starting Point for Trace
            startingpoint = arcpy.analysis.Select(
                in_features=CSOs,
                out_feature_class=os.path.join(db, "tmp_startingpoint"),
                where_clause=whereclause,
            )

            ## Run the Trace, collect the output trace
            arcpy.tn.Trace(
                in_trace_network=tracenet,
                trace_type=comb[0],
                starting_points=os.path.join(db, "tmp_startingpoint"),
                barriers=r"C:\Users\wik191\OneDrive - Harvard University\Projects\Delaney- HUC12 CSOs\HUC12_CSOs\HUC12_CSOs.gdb\TN_Temp_Barriers",
                path_direction="NO_DIRECTION",
                shortest_path_network_attribute_name="",
                include_barriers="INCLUDE_BARRIERS",
                validate_consistency="VALIDATE_CONSISTENCY",
                ignore_barriers_at_starting_points="DO_NOT_IGNORE_BARRIERS_AT_STARTING_POINTS",
                allow_indeterminate_flow="IGNORE_INDETERMINATE_FLOW",
                condition_barriers=None,
                function_barriers="ADD 'Shape length' IS_GREATER_THAN {dist} true".format(
                    dist=comb[1]
                ),
                traversability_scope="BOTH_JUNCTIONS_AND_EDGES",
                functions=None,
                output_conditions=None,
                result_types="AGGREGATED_GEOMETRY",
                selection_type="NEW_SELECTION",
                clear_all_previous_trace_results="CLEAR_ALL_PREVIOUS_TRACE_RESULTS",
                trace_name="",
                aggregated_points=os.path.join(db, "test_Results_Aggregated_Points"),
                aggregated_lines=os.path.join(db, "test_Results_Aggregated_Lines"),
                out_network_layer=None,
                use_trace_config="DO_NOT_USE_TRACE_CONFIGURATION",
                trace_config_name="",
                out_json_file=None,
            )

            arcpy.analysis.SpatialJoin(
                target_features=os.path.join(db, "test_Results_Aggregated_Lines"),
                join_features=os.path.join(db, "tmp_startingpoint"),
                out_feature_class=r"C:\Users\wik191\OneDrive - Harvard University\Projects\Delaney- HUC12 CSOs\HUC12_CSOs\HUC12_CSOs.gdb\tmp_startingpoin_SpatialJoin",
                join_operation="JOIN_ONE_TO_ONE",
                join_type="KEEP_ALL",
                field_mapping='TRACENAME "TRACENAME" true true false 255 Text 0 0,First,#,test_Results_Aggregated_Lines,TRACENAME,0,254;Shape_Length "Shape_Length" false true true 8 Double 0 0,First,#,test_Results_Aggregated_Lines,Shape_Length,-1,-1;Permittee_Name "Permittee Name" true true false 255 Text 0 0,First,#,tmp_startingpoint,Permittee_Name,0,254;Permittee_ID "Permittee ID" true true false 255 Text 0 0,First,#,tmp_startingpoint,Permittee_ID,0,254;Outfall_ID "Outfall ID" true true false 255 Text 0 0,First,#,tmp_startingpoint,Outfall_ID,0,254;Water_Body "Water Body" true true false 255 Text 0 0,First,#,tmp_startingpoint,Water_Body,0,254;Location "Location" true true false 255 Text 0 0,First,#,tmp_startingpoint,Location,0,254;Municipality "Municipality" true true false 255 Text 0 0,First,#,tmp_startingpoint,Municipality,0,254;Lat "Lat" true true false 8 Double 0 0,First,#,tmp_startingpoint,Lat,-1,-1;Long "Long" true true false 8 Double 0 0,First,#,tmp_startingpoint,Long,-1,-1;Facility_Name__City_First "Facility Name, City First" true true false 255 Text 0 0,First,#,tmp_startingpoint,Facility_Name__City_First,0,254',
                match_option="INTERSECT",
                search_radius=None,
                distance_field_name="",
                match_fields=None,
            )

            arcpy.management.Append(
                inputs=os.path.join(db, "tmp_startingpoin_SpatialJoin"),
                target=os.path.join(
                    db, "trace_{dir}_{dist}km".format(dir=comb[0], dist=str(comb[1]))
                ),
                schema_type="NO_TEST",
            )
#         arcpy.management.CalculateField(
#             in_table=traceoutputs,
#             field="Direction",
#             field_type="TEXT",
#             expression=comb[0],
#         )
#         arcpy.management.CalculateField(
#             in_table=traceoutputs,
#             field="Distance",
#             field_type="SHORT",
#             expression=comb[1],
#         )
