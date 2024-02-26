import zipfile, re, os, os.path, glob

vars = ["ppt", "tdmean", "tmax", "tmin", "tmean", "vpdmax", "vpdmin"]
destination = "S:\\GCMC\\Data\\Climate\\PRISM"


def extract_nested_zip(zippedFile, toFolder):
    """Extract a zip file including any nested zip files
    Delete the zip file(s) after extraction
    """
    with zipfile.ZipFile(zippedFile, "r") as zfile:
        zfile.extractall(path=toFolder)
    os.remove(zippedFile)
    for root, dirs, files in os.walk(toFolder):
        for filename in files:
            if re.search(r"\.zip$", filename):
                fileSpec = os.path.join(root, filename)
                extract_nested_zip(fileSpec, root)


for i in vars:
    files = glob.glob(os.path.join(destination, i, "daily_4km", "*.zip"))
    for ii in files:
        extract_nested_zip(
            zippedFile=ii, toFolder=os.path.join(destination, i, "daily_4km")
        )
