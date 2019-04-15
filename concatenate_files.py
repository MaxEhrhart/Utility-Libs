# -*- encoding: utf-8 -*-
from itertools import islice
from os import listdir
from os.path import basename, abspath
from glob import glob

#PEGAR AGOSTO DE 2014 ARQUIVO 03 08 2014
PARTICAO = 150000  # qtd de linhas a serem alteradas de cada vez


def read_in_chunks(file_object, chunk_size=PARTICAO):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


if __name__ == "__main__":
    caminho_raiz = r"C:\Users\tc015789\Downloads\BACKUPS - BANCO DE LOJAS"
    raiz = [caminho_raiz + "\\" + diretorio for diretorio in listdir(caminho_raiz)][::-1]
    """
        2014  -MERCEARIA
        2015 - MERCEARIA
        2016 - MERCEARIA
        2017 - MERCEARIA
        2018 - MERCEARIA
    """

    for diretorio_ano in raiz:
        ano = basename(diretorio_ano)[0:4]
        diretorio_mes = [diretorio_ano + "\\" + diretorio for diretorio in listdir(diretorio_ano)]
        """
            01_JAN
            02_FEV
            03_MAR
            04_ABR
            05_MAI
            06_JUN
            07_JUL
            08_AGO
            09_SET
            10_OUT
            11_NOV
            12_DEZ
        """

        for diretorio_arquivos in diretorio_mes:            
            caminho_arquivos = [diretorio_arquivos + "\\" + nome_arquivo for nome_arquivo in listdir(diretorio_arquivos)]
            
            for mes, arquivo in zip(diretorio_mes, caminho_arquivos):
                
                """
                    01_JAN, MERCEARIA_CONS (2).txt
                    01_JAN, MERCEARIA_CONS (3).txt
                    01_JAN, MERCEARIA_CONS (4).txt
                    01_JAN, MERCEARIA_CONS.txt
                    ...
                """
                arquivo_destino = diretorio_arquivos + "\\" + ano + " - " + basename(mes).replace("_"," ") + ".csv"
                f_out = open(arquivo_destino, "a+", encoding="utf-8")
                f_in = open(arquivo,"r",encoding="utf-8")
                next(f_in)  # pula header

                for lines in read_in_chunks(f_in):
                    f_out.writelines([line.replace("\t",";") for line in lines])
                    f_out.flush()

                f_in.close()
            f_out.close()
