from collections import namedtuple
#from operator import attrgetter
from os.path import basename

# Função para listar arquivos do blob no caminho passado no blob_path.
def hdfs_listdir(blob_path):
    command = 'hdfs dfs -ls ' + blob_path
    print("Listing files: " + remotefilepath + " ...")
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    p.wait()
    files = [item.rstrip("\n").split()[-1] for item in p.stdout.readlines()]
    if len(files) > 0:
        files.pop(0)  # remove summary from ls: "found n items".
    qty_files = len(files)
    return files, qty_files


def get_file(remotefilepath, localpath):
    command = " ".join(['hdfs', 'dfs', '-get', "\"{0}\"".format(remotefilepath), "\"{0}\"".format(localpath)])
    print("Downloading file: " + remotefilepath + " ...")
    p = subprocess.call(command,shell=True)
    p.wait()
    print("File downloaded!") if p == 0 else print("Fail to download file: " + remotefilepath)
    return p


def put_file(localfilepath, remotefilepath):
    command = " ".join(['hdfs', 'dfs', '-put',  "\"{0}\"".format(localfilepath), "\"{0}\"".format(remotefilepath)])
    print(command)
    print("Sending file " + localfilepath + " ...")
    p = subprocess.call(command,shell=True)
    p.wait()
    print("File sent!") if p == 0 else print("Fail to send file from: " + localfilepath + " to " + remotefilepath)
    return p


def remove_file(remotefilepath):
    command = " ".join(['hdfs', 'dfs', '-rm',  "\"{0}\"".format(remotefilepath)])
    print(command)
    print("Removing previous file " + remotefilepath + " ...")
    p = subprocess.call(command,shell=True)
    p.wait()
    print("File removed!") if p == 0 else print("Fail to remove file from: " + remotefilepath)
    return p


def move_file(_from_,_to_):
    command = " ".join(['hdfs', 'dfs', '-mv',  "\"{0}\"".format(_from_),  "\"{0}\"".format(_to_)])
    print(command)
    print("Moving previous file from " + _from_ + " to: " + _to_ + " ...")
    p = subprocess.call(command,shell=True)
    p.wait()
    print("File moved!") if p == 0 else print("Fail to move file from: " + _from_ + " to: " + _to_ + " ...")
    return p


def verify(p):
    """
    if p:
        print("Fail during process!")
    return
    """
    pass

# Recebendo lista de arquivos. Obs.: Verificar caso onde nenhum arquivo é encontrado.
files = hdfs_listdir("wasbs://hdiprojsupplydatalake-2018-07-12t15-58-09-078z@hdiprojsupplydatalake.blob.core.windows.net/jasons/")
