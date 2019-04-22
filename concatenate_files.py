# -*- encoding: utf-8 -*-
from os import listdir
from os.path import basename
from itertools import islice

"""
/*
#PEGAR AGOSTO DE 2014 ARQUIVO 03 08 2014
*/
"""

DELIMITER_IN = "\t"
LINHAS = 10000  # qtd de linhas a serem alteradas de cada vez
DELIMITER_OUT = ";"


if __name__ == "__main__":
    caminho_raiz = r"C:\Users\tc015789\Downloads\BACKUPS - BANCO DE LOJAS"
    raiz = [caminho_raiz + "\\" + diretorio for diretorio in listdir(caminho_raiz)][::-1]  # remover o [:3] apos processamento
    """
        2014 - MERCEARIA
        2015 - MERCEARIA
        2016 - MERCEARIA <-
        2017 - MERCEARIA
        2018 - MERCEARIA
    """
    print(raiz)
    for diretorio_ano in raiz:
        ano = basename(diretorio_ano)[0:4]
        diretorio_mes = [diretorio_ano + "\\" + diretorio for diretorio in listdir(diretorio_ano)][::-1]
        """
            01_JAN
            02_FEV
            03_MAR
            04_ABR 9
            05_MAI 8
            06_JUN 7
            07_JUL 6
            08_AGO 5
            09_SET 4
            10_OUT 3
            11_NOV 2
            12_DEZ 1
        """
        for diretorio_arquivos in diretorio_mes:
            caminho_arquivos = [diretorio_arquivos + "\\" + nome_arquivo for nome_arquivo in listdir(diretorio_arquivos)]
            mes = basename(diretorio_arquivos).replace("_"," ")
            arquivo_destino = "{}\{} - {}.csv".format(diretorio_arquivos, ano, mes)
            print("Escrevendo arquivo: {}".format(arquivo_destino))

            with open(arquivo_destino, "a+", encoding="utf-8") as f_out:
                for arquivo in caminho_arquivos:
                    """
                        MERCEARIA_CONS (2).txt
                        MERCEARIA_CONS (3).txt
                        MERCEARIA_CONS (4).txt
                        MERCEARIA_CONS.txt
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
