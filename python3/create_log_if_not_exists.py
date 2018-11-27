# -*- coding: utf-8 -*-

def create_log_if_not_exists(log_path, log_filename):
    """ Create log files. """
    from datetime import datetime
    from os import makedirs
    from os.path import exists
    
    # Create directory tree if not exists.
    log_path = log_path.replace("\\","/")
    if not exists(log_path):
        makedirs(log_path)
    
    # Create log files if they not exists.
    time = datetime.now().strftime('%Y%m%d%H%M%S')
    log_filename_1 = "{0}/{1}_{2}.{3}".format(log_path, log_filename, time, "qafaillines")
    log_filename_2 = "{0}/{1}_{2}.{3}".format(log_path, log_filename, time, "qafaillineslogs")
    with open(log_filename_1, "a+", encoding="latin-1") as log1, open(log_filename_2,"a+", encoding="latin-1") as log2:
        log1.close()
        log2.close()
        
    return log_filename_1, log_filename_2
