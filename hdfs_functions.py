import subprocess


# Função para listar arquivos do blob no caminho passado no blob_path.
def hdfs_listdir(blob_path):
    command = 'hdfs dfs -ls ' + blob_path
    print("Listing files: " + blob_path + " ...")
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    p.wait()
    files = [item.rstrip("\n").split()[-1] for item in p.stdout.readlines()]
    if len(files) > 0:
        files.pop(0)  # remove summary from ls: "found n items".
    qty_files = len(files)
    return files, qty_files


def get_file(remote_file_path, local_path):
    command = " ".join(['hdfs', 'dfs', '-get', "\"{0}\"".format(remote_file_path), "\"{0}\"".format(local_path)])
    print("Downloading file: " + remote_file_path + " ...")
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    p.wait()
    # print("File downloaded!") if p == 0 else print("Fail to download file: " + remote_file_path)
    return p


def put_file(local_file_path, remote_file_path):
    command = " ".join(['hdfs', 'dfs', '-put', "\"{0}\"".format(local_file_path), "\"{0}\"".format(remote_file_path)])
    # print(command)
    print("Sending file " + local_file_path + " ...")
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    p.wait()
    # print("File sent!") if p == 0 else print("Fail to send file from: " + local_file_path + " to " + remote_file_path)
    return p


def remove_file(remote_file_path):
    command = " ".join(['hdfs', 'dfs', '-rm', "\"{0}\"".format(remote_file_path)])
    # print(command)
    print("Removing previous file " + remote_file_path + " ...")
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    p.wait()
    # print("File removed!") if p == 0 else print("Fail to remove file from: " + remote_file_path)
    return p


def move_file(src, dest):
    command = 'hdfs dfs -mv "{from}" "{to}"'.format(from=src, to=dest)
    # print(command)
    print("Moving previous file from {from} to: {to} ...".format(src, dest))
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    p.wait()
    # print("File moved!") if p == 0 else print("Fail to move file from: " + _from_ + " to: " + _to_ + " ...")
    return p


def verify(p):
    """
    if p:
        print("Fail during process!")
    return
    """
    pass
