from collections import namedtuple


def remove_file(remotefilepath):
    command = " ".join(['hdfs', 'dfs', '-rm',  "\"{0}\"".format(remotefilepath)])
    print(command)
    print("Removing file " + remotefilepath + " ...")
    p = subprocess.call(command,shell=True)
    print("File removed!") if p == 0 else print("Fail to remove file from: " + remotefilepath)
    return p


def hdfs_get_filelist(blob_path, delimiter="_"):
    import subprocess
    from os.path import basename, splitext
    from collections import namedtuple
    """ Lists hdfs dir and returns named tuples with information of file based on its filename. """
    def hdfs_listdir(blob_path):
        command = 'hdfs dfs -ls ' + blob_path
        p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        p.wait()
        files = [item.rstrip("\n").split()[-1] for item in p.stdout.readlines()]
        if len(files) > 0:
            files.pop(0)  # remove summary from ls: "found n items".
        qty_files = len(files)
        return files, qty_files
    files, qty_files = hdfs_listdir(blob_path)
    kpis = []
    # If there are items in dir.
    if qty_files > 0:
        KPI = namedtuple('KPI', ["filepath", "filename", "kpi_name", "initial_date", "final_date", "key","extension"])
        for file in files:
            filename, ext = basename(file), splitext(basename(file))[1]
            if ext == ".json":
                splits = 3
                kpi = KPI(
                    filepath=file
                    , filename=filename
                    , kpi_name=filename.rsplit(delimiter, splits)[0]
                    , initial_date=filename.rsplit(delimiter, splits)[1]
                    , final_date=filename.rsplit(delimiter, splits)[2]
                    , key=splitext(filename.rsplit(delimiter, splits)[3])[0]
                    , extension=ext
                )
            else:  # ext != ".json":
                splits = 1
                kpi = KPI(
                    filepath=file
                    , filename=filename
                    , kpi_name=filename.rsplit(delimiter, splits)[0]
                    , initial_date=None
                    , final_date=None
                    , key=splitext(filename.rsplit(delimiter, splits)[1])[0]
                    , extension=ext
                )
            kpis.append(kpi)
    return kpis, len(kpis)

kpis, files = hdfs_get_filelist("wasbs://hdiprojsupplydatalake-2018-07-12t15-58-09-078z@hdiprojsupplydatalake.blob.core.windows.net/estrutura_final/")
