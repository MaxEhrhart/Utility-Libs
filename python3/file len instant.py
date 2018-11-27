
def file_len(file1,encoding):
    with open(file1,"r",encoding=encoding) as f:
        return len(f.readlines())

def instant():
    from datetime import datetime
    now = datetime.now()
    time1, time2 = now.strftime('%Y%m%d%H%M%S'), now.strftime('%Y-%m-%d %H:%M:%S')
    return time1, time2

def file_name(path,ext):
    relevant_path = path
    included_extensions = [ext]
    file = [fn for fn in os.listdir(relevant_path) if any(fn.endswith(ext) for ext in included_extensions)]
    return file
