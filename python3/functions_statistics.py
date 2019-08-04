# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime
import numpy as np
import csv
import codecs


def fileLen(file):
    with open(file) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


# Passo H
def step_h(df, campos_a_validar):
    v = []
    for campo in campos_a_validar:
        for row_ind in df[df[campo].isnull()].index:
            v.append(row_ind)

    v = list(set(v))

    df_errorlines = df.loc[v]
    df.drop(v, inplace=True)
    return df, df_errorlines


def step_g(df, vpk):
    codigo_da_linha = "Código da Linha"
    print(codigo_da_linha)
    df_error = df.reset_index().groupby(codigo_da_linha).filter(lambda x: len(x) < 3).groupby(codigo_da_linha).first()
    return df_error.reset_index().set_index('Row_num').reset_index()


def step_i(df, vpk2):
    v = []
    for campo in vpk2:
        for row_ind in df[df[campo].isnull()].index:
            v.append(row_ind)

    v = list(set(v))
    df_error_i = df.loc[v]

    return df, df_error_i
