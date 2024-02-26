import geemap

js_to_convert = "S:/GCMC/_Code/ExtractBuildingstoPoints.js"
py_output = "S:/GCMC/_Code/gee_ExtractBuildingstoPoints.py"




geemap.js_to_python(in_file=js_to_convert,out_file=py_output)