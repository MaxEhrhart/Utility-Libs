# -*- coding: utf-8 -*-
import argparser
import pandas as pd


def recebe_argumentos():
    """ receive parameters. """
    parser = argparse.ArgumentParser(description="MULTIPLICITY TEST.")
    parser.add_argument("-file", "-f", help="Full csv file path.", required=True)
    parser.add_argument("-delimiter", "-d", help="Csv file delimiter.", required=False, const=",")
    
    args = parser.parse_args()
    return args.file, args.delimiter
            

def multiplicity_test():
    """
    formato:
        1: caminho arquivo / arquivo.extensao      
        2: FILE NOT PASSED IN THE MULTIPLICITY PASS 2 OF 3 
        3: timestamp comecou
        4: timestamp fim
        5: Num linha ( valor col row_number)
        6: Codigo do erro (M no meu)
        7: MULTIPLICITY Error
    """
    """
    with open('file') as f:
        seen = set()
        dups = list()
        for line in f:
            line_lower = line.lower()
            if line_lower in seen:
                dups.add(line)
            else:
                seen.add(line_lower)
    """
    pass


file, delimiter = recebe_argumentos()
print(file,delimiter)

#df = pd.read_csv(file, encoding="utf8")
