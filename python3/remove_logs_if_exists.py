# -*- coding: utf-8 -*-

def remove_logs_if_exists(log_path, log_extensions=None):
    """ Remove log files. 
    If only log_path is passed is defined, only file in logpath will be removed,
    If log path and log extensions are defined, all log files in that folder will be removed.
    Obs.: Log extensions can be one string to remove only the files
          with that extenson or a string list of extensions to remove.
    """
    from os import listdir, remove
    from os.path import exists
    
    def remove_file(file, show=False):
        """ Remove file. """
        file = file.replace("\\","/")
        file_path = file
        file = file.rsplit("/",1)[-1]
        try:
            if exists(file_path):
                remove(file_path)
                message = file + " Removed."
            else:
                message = "File: " + file_path + " not found."
        except Exception as ex:
            message = str(ex)
        if show:
            print(message)
        return message
    
    # If log_extensions equals None, then arg is not set and removes a single log file in log_path, else, remove all logfiles in path
    if log_extensions == None:  
        remove_file(log_path, show=True)
    else:
        # If log extension equals string, add to list of search, else, list is passed as argument.
        if type(log_extensions) == str:
            log_extensions = [log_extensions]
        # Files filter.
        files = [fn for fn in listdir(log_path) if any(fn.endswith(ext) for ext in log_extensions)]
        for file in files:
            remove_file(log_path + "/" + file, show=True)
