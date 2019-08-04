# -*- coding: utf-8 -*-
from python.validation.DATATYPE import gly
from python.validation.MULTIPLY import multiply
from python.validation.STATISTIC import statistic
from config.configuration import GlobalConfiguration
from utility import log
from os.path import basename
from os import remove
from sys import exit
import argparse
import subprocess


def parameters():
    """ receive parameters. """
    parser = argparse.ArgumentParser(description="DATA QUALITY.")
    parser.add_argument("-mode", "-m",
                        help="Type of dataquality test, Datatype, Multiplicity or Statistic ( D, M or S).",
                        required=False)
    parser.add_argument("-input", "-i", help="Blob input file_path.", required=False)
    parser.add_argument("-output", "-o", help="Blob output file_path.", required=False)
    parser.add_argument("-faillines", "-fl", help="Blob fail lines file_path.", required=False)
    parser.add_argument("-faillinesdescription", "-fld", help="Blob fail lines description file_path.", required=False)
    parser.add_argument("-origin", "-ori", help="Source database, ex.: \"CORP\".", required=False)
    parser.add_argument("-hivetable", "-ht", help="Hive external table path.", required=False)
    parser.add_argument("-donefilepath", "-d", help="Blob done file path.", required=False)

    args = parser.parse_args()
    return args.mode, args.input, args.output, args.faillines, args.faillinesdescription, args.origin, args.hivetable, args.donefilepath


def write_status_file(status_dict: dict, donefile: str):
    status_line = ";".join([
        status_dict['execution_status']
        , status_dict['initial_lines']
        , status_dict['exit_lines']
        , status_dict['start_date']
        , status_dict['end_date']
    ])
    with open(donefile, "w+", encoding="utf-8") as f:
        f.write(status_line + "\n")
        f.close()


def remove_temp_files(local_file="", qafaillines="", qafaillineslog="", hive_file="", done_file=""):
    temp_files = sorted(list(filter(None, set([local_file, qafaillines, qafaillineslog, hive_file, done_file]))))
    print("Removing temporary files ...")
    for temp_file in temp_files:
        print("Removing: " + temp_file + " ...")
        log.remove_file(temp_file)
    print("Temporary files removed!")


def get_file(remotefilepath, localpath):
    command = " ".join(['hdfs', 'dfs', '-get', "\"{0}\"".format(remotefilepath), "\"{0}\"".format(localpath)])
    print("Downloading file: " + remotefilepath + " ...")
    p = subprocess.call(command, shell=True)
    print("File downloaded!") if p == 0 else print("Fail to download file: " + remotefilepath)
    return p


def put_file(localfilepath, remotefilepath):
    command = " ".join(['hdfs', 'dfs', '-put', "\"{0}\"".format(localfilepath), "\"{0}\"".format(remotefilepath)])
    print(command)
    print("Sending file " + localfilepath + " ...")
    p = subprocess.call(command, shell=True)
    print("File sent!") if p == 0 else print("Fail to send file from: " + localfilepath + " to " + remotefilepath)
    return p


def remove_file(remotefilepath):
    command = " ".join(['hdfs', 'dfs', '-rm', "\"{0}\"".format(remotefilepath)])
    print(command)
    print("Removing previous file " + remotefilepath + " ...")
    p = subprocess.call(command, shell=True)
    print("File removed!") if p == 0 else print("Fail to remove file from: " + remotefilepath)
    return p


def move_file(_from_, _to_):
    command = " ".join(['hdfs', 'dfs', '-mv', "\"{0}\"".format(_from_), "\"{0}\"".format(_to_)])
    print(command)
    print("Moving previous file from " + _from_ + " to: " + _to_ + " ...")
    p = subprocess.call(command, shell=True)
    print("File moved!") if p == 0 else print("Fail to move file from: " + _from_ + " to: " + _to_ + " ...")
    return p


def verify(p):
    """
    if p:
        print("Fail during process!")
    return
    """
    pass


def main():
    # Get parameters.
    mode, input_file, output_path, faillines_path, faillinesdescription_path, origin, hivetable, donefile_path = parameters()

    # Initial filepath vars values.
    localfile, qafaillines, qafaillineslog, donefile, hivefile = "", "", "", "", ""

    # Config file.
    config = GlobalConfiguration("gly")
    file_path = config.get_value("path", default_section=True).replace("\\", "/")
    encoding = config.get_value("encoding", default_section=True)

    # Paths.
    input_file = input_file.replace("\\", "/")
    output_path = output_path.replace("\\", "/")
    input_filename = basename(input_file)
    localfile = file_path + input_filename
    donefile = file_path + input_filename + "_status.csv"

    # Dictionary used to create status lines.
    status = dict()

    # Remove previous execution files and logs from local and create new logs.
    qafaillines, qafaillineslog, start_date = log.create_log_if_not_exists(file_path, input_filename, encoding=encoding)

    # Full path to log files.
    qafaillines = file_path + qafaillines
    qafaillineslog = file_path + qafaillineslog

    # Execution status record.
    status['execution_status'] = ''
    status['initial_lines'] = ''
    status['exit_lines'] = ''
    status['start_date'] = start_date
    status['end_date'] = ''

    try:
        # Datatype validation.
        if mode == "D":
            # Download file for treatment.
            get_file(input_file, file_path)
            flg_fail, flg_warning, flg_success, finalfile_path, status = gly.main(input_filename, qafaillines,
                                                                                  qafaillineslog, status)
            write_status_file(status, donefile)
            if flg_fail:
                print("F")
            elif flg_warning:
                print("W")
                verify(put_file(finalfile_path, output_path))
                verify(put_file(qafaillines, faillines_path))
                verify(put_file(qafaillineslog, faillinesdescription_path))
                verify(remove_file(input_file))
            else:  # flg_success:
                print("S")
                verify(remove_file(input_file))
            verify(put_file(donefile, donefile_path))

        # Multiplicity validation.
        elif mode == "M":
            # Download file for treatment.
            get_file(input_file, file_path)
            flg_fail, flg_warning, flg_success, finalfile_path, status = multiply.main(input_filename, qafaillines,
                                                                                       qafaillineslog, status)
            write_status_file(status, donefile)
            if flg_fail:
                print("F")
            elif flg_warning:
                print("W")
                verify(put_file(finalfile_path, output_path))
                verify(put_file(qafaillines, faillines_path))
                verify(put_file(qafaillineslog, faillinesdescription_path))
                verify(remove_file(input_file))
            else:  # flg_success:
                print("S")
                verify(remove_file(input_file))
            verify(put_file(donefile, donefile_path))

        # Statistic validation.
        elif mode == "S":
            # Download file for treatment.
            get_file(input_file, file_path)
            flg_fail, flg_warning, flg_success, finalfile_path, status, hivefile = statistic.main(input_filename,
                                                                                                  qafaillines,
                                                                                                  qafaillineslog,
                                                                                                  status, origin)
            write_status_file(status, donefile)
            if flg_fail:
                print("F")
            elif flg_warning:
                print("W")
                # verify(put_file(finalfile_path, output_path))
                verify(put_file(qafaillines, faillines_path))
                verify(put_file(qafaillineslog, faillinesdescription_path))
                verify(put_file(hive_file, hivetable))
                verify(remove_file(input_file))
            else:  # flg_success:
                print("S")
                verify(put_file(hive_file, hivetable))
                verify(remove_file(input_file))
            verify(put_file(donefile, donefile_path))

        # Wrong validation argument.
        else:
            raise Exception("Invalid Parameter!")

    except Exception as ex:
        print("Exception ocurred: " + str(ex))

    finally:
        remove_temp_files(local_file=localfile, qafaillines=qafaillines, qafaillineslog=qafaillineslog,
                          done_file=donefile, hive_file=hivefile)
        print("Done")
