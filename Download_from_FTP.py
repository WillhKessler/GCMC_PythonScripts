import sys, ftplib, os, time

server = "prism.oregonstate.edu"
source = "/daily/tmean/"
destination = "S:\\GCMC\\Data\\Climate\\PRISM/tmean/daily_4km/"
interval = 0.05

ftp = ftplib.FTP(server)
ftp.login()

ftp.cwd(source)
varlist = ftp.nlst()
print(varlist)


def downloadFiles(path, destination):
    try:
        ftp.cwd(path)
        os.chdir(destination)
        mkdir_p(destination[0 : len(destination) - 1] + "/" + str.split(path, "/")[-2])
        print(
            "Created: "
            + destination[0 : len(destination) - 1]
            + "/"
            + str.split(path, "/")[-2]
        )
    except OSError:
        pass
    except ftplib.error_perm:
        print("Error: could not change FTP path to " + path)
        sys.exit("Ending Application")

    filelist = ftp.nlst()

    for file in filelist:
        time.sleep(interval)
        try:
            ftp.cwd(path + file + "/")
            downloadFiles(path + file + "/", destination)
        except ftplib.error_perm:
            os.chdir(
                destination[0 : len(destination) - 1] + "/" + str.split(path, "/")[-2]
            )

            try:
                ftp.retrbinary(
                    "RETR " + file,
                    open(
                        os.path.join(
                            destination + "/" + str.split(path, "/")[-2], file
                        ),
                        "wb",
                    ).write,
                )
                print("Downloaded: " + file)
            except:
                print("Error: File could not be downloaded " + file)
    return


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


downloadFiles(source, destination)
