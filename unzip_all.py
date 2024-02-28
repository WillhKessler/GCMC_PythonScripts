import zipfile, re, os, os.path, glob

vars = ["tdmean", "tmax", "tmin", "tmean", "vpdmax", "vpdmin"]

destination = "S:\\GCMC\\Data\\Climate\\PRISM"


def extract_nested_zip(zippedFile, toFolder):
    """Extract a zip file including any nested zip files
    Delete the zip file(s) after extraction
    """
    with zipfile.ZipFile(zippedFile, "r") as zfile:
        zfile.extractall(path=toFolder)
    os.remove(zippedFile)
    # for root, dirs, files in os.walk(toFolder):
    #     print(root)
    #     for filename in files:
    #         print(files)
    # if re.search(r"\.zip$", filename):
    #     fileSpec = os.path.join(root, filename)
    #     extract_nested_zip(fileSpec, root)


for i in vars:
    print(i)
    files = glob.glob(os.path.join(destination, i, "daily_4km", "*"))
    # files = glob.glob(os.path.join(destination, i, "daily_4km", "*.zip"))
    # print(files)
    for ii in files:
        print(ii)
        zips = glob.glob(os.path.join(ii, "*.zip"))
        # print(os.path.join(destination, i, "daily_4km", "*"))
        # extract_nested_zip(zippedFile=ii, toFolder=os.path.join(ii))
        for iii in zips:
            print(iii)
            extract_nested_zip(iii, os.path.join(ii))
