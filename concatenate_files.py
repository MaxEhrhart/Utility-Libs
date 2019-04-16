# -*- encoding: utf-8 -*-
from os import listdir
from os.path import basename
from itertools import islice

#PEGAR AGOSTO DE 2014 ARQUIVO 03 08 2014
LINHAS = 10000  # qtd de linhas a serem alteradas de cada vez
DELIMITER_IN = "\t"
DELIMITER_OUT = ";"

def read_in_chunks(file_object, chunk_size=1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.readlines(chunk_size)
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
        diretorio_mes = [diretorio_ano + "\\" + diretorio for diretorio in listdir(diretorio_ano)][::-1]
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
            mes = basename(diretorio_arquivos).replace("_"," ")
            arquivo_destino = "{}\{} - {}.csv".format(diretorio_arquivos, ano, mes)
            print("Escrevendo arquivo: {}".format(arquivo_destino))

            f_out = open(arquivo_destino, "a+", encoding="utf-8")

            for arquivo in caminho_arquivos:
                """
                    01_JAN, MERCEARIA_CONS (2).txt
                    01_JAN, MERCEARIA_CONS (3).txt
                    01_JAN, MERCEARIA_CONS (4).txt
                    01_JAN, MERCEARIA_CONS.txt
                    ...
                """
                print("    Lendo arquivo: {}".format(arquivo))
                with open(arquivo,"r",encoding="utf-8") as f_in:
                    next(f_in)  # pula header

                    while True:
                        treated_lines = []
                        raw_lines = list(islice(f_in, LINHAS))

                        if not raw_lines:
                            break

                        for line in raw_lines:
                            line = line.split(DELIMITER_IN)
                            line = DELIMITER_OUT.join(line[:7] + [col.replace(",",".").strip() for col in line[7:]])
                            treated_lines.append(line + "\n")

                        f_out.writelines(treated_lines)
                        f_out.flush()

                f_in.close()
            f_out.close()
