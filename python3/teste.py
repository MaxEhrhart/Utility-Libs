import csv
from collections import OrderedDict

arq = r"C:\Users\maximilian.erhard\Desktop\processar\20170801kpiLEF_20170728.csv"
delimiter = ";"
encoding = "latin-1"
# colunas = "asd0|asd1".split("|")


with open(arq, "r", encoding=encoding) as f:
    reader = csv.reader(f, delimiter=delimiter)
    keys = next(reader)
    dataframe = [OrderedDict(zip(keys, row)) for row in reader]


def multiplicity_line(dict_list):
    lines_seen = list()
    dup_lines = list()
    for line in dict_list:
        lines_seen.append(line) if line not in lines_seen else dup_lines.append(line)
    return lines_seen, dup_lines


ok, dup = multiplicity_line(dataframe)

print(ok)
