#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Requisitos:

    curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py" && python get-pip.py && rm -f get-pip.py
    sudo yum -y install \
        python-devel \
        libxslt-devel \
        libffi-devel \
        openssl-devel \
        python-pip \
        gcc \
        gcc-c++ \
        python-virtualenv \
        cyrus-sasl \
        cyrus-sasl-devel \
        cyrus-sasl-plain

    pip install pandas numpy pyhive[hive] matplotlib reportlab
"""
import argparse
import codecs
import re
from os import makedirs, remove
from os.path import abspath, dirname, exists
import numpy as np
import pandas as pd
from sys import exit
from datetime import datetime, date
from pyhive import hive
from reportlab.lib.pagesizes import letter, landscape, A4
from reportlab.lib.units import inch, cm
from reportlab.pdfgen import canvas
import matplotlib

matplotlib.use('agg')  # troco tkinter por agg
import matplotlib.pyplot as plt

# Global
# Paths
MAIN_PATH = abspath(dirname(__file__)).replace("\\", "/")
STATEMENTS_OUTPUT_PATH = MAIN_PATH + "/reports"
CSV_OUTPUT_PATH = MAIN_PATH + "/comission_CSVs"
RESOURCES_PATH = MAIN_PATH + "/resources"
# Background
BG_PREVIEW = RESOURCES_PATH + "/bg_previa_v6.png"
BG_OFFICIAL = RESOURCES_PATH + "/bg_oficial_v6.png"
BG_ADJUSTMENT = RESOURCES_PATH + "/bg_ajuste_v6.png"
# Processing date
NOW = date.today()
DAY, MONTH, YEAR = NOW.day, NOW.month, NOW.year
PROCESSING_DATE = "{0:02d}/{1:02d}/{2:04d}".format(DAY, MONTH, YEAR)
# Connection
HIVESERVER2_IP = "172.31.33.92"
HIVESERVER2_PORT = "10000"
HIVESERVER2_AUTH = None  # "LDAP" | "CUSTOM" | "KERBEROS" se somente se kerberos param estiver ativo
HIVESERVER2_USER = None  # Usar somente se AUTH estiver ativo
HIVESERVER2_PASS = None  # Usar somente se AUTH estiver ativo
KERBEROS_SERVICE_NAME = None  # Usar somente se AUTH for "KERBEROS"
HIVE_CONFIGURATION = {
    "hive.vectorized.execution.enabled": "true",
    "hive.vectorized.execution.reduce.enabled": "true"
}
# SMTP
SMTP_IP = "123.123.123.132"
SMTP_PORT = "123"
SMTP_AUTH = ""
SMTP_USER = ""
SMTP_PASS = ""
# TEMPORARIO ( MUDAR DEPOIS PARA UMA FORMA DINAMICA)
LIMITE_COMISSAO = 15000.00


def makedirs_if_not_exists(dirs):
    if not exists(dirs):
        makedirs(dirs)


def console_print(string):
    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S') + " # " + unicode(string))
    return


def imprime_chave_valor_dicionario(dicionario):
    """ Imprime as chaves e os valores dos itens do dicionario. """
    for key in dicionario:
        print(key, dicionario[key])
    print("\n\n")
    return


def valor_real(valor):
    """ Formata valor numerico (float) para REAL (R$ 1.000,00). """
    valor = float(valor) if valor != "-" else 0.0
    return "R$ {:,.2f}".format(valor).replace(',', 'x').replace('.', ',').replace('x', '.')


def real_valor(valor):
    """ Recebe string e transforma para valor. """
    return float(valor.replace(".", "").replace(",", ".").lstrip("R$").strip(" "))


def formata_valor_venda(venda):
    """
        Corta a casa decimal se o valor da venda for maior ou igual a 1000
        :venda: string "R$ 1.000,00"
    """
    return venda if venda == "-" else venda.split(",")[0] if real_valor(venda) >= 1000 else venda


def nome_arquivo(arquivo):
    """ Transforma nome do arquivo para um nome valido. """
    arquivo = arquivo.strip().replace(' ', '_').strip("_")
    return re.sub(r'(?u)[^-\w.]', '', arquivo)


def ano_comercial(ano_mes):
    """ Retorna lista indicando os meses e o ano do ano comercial atual """
    ano, mes = map(int, ano_mes.split("/"))
    meses_ano_comercial = []
    if (mes >= 9) and (mes <= 12):
        meses_ano_comercial += ["{0}/{1:02d}".format(ano, i) for i in range(9, 12 + 1)]
        meses_ano_comercial += ["{0}/{1:02d}".format(ano + 1, i) for i in range(1, 8 + 1)]
    else:
        meses_ano_comercial += ["{0}/{1:02d}".format(ano - 1, i) for i in range(9, 12 + 1)]
        meses_ano_comercial += ["{0}/{1:02d}".format(ano, i) for i in range(1, 8 + 1)]
    return meses_ano_comercial


def cria_grafico(meses, valores, filename):
    """ Cria arquivo grafico de barras com o progresso do consultor/vendedor nos ultimos meses. """
    file_path = RESOURCES_PATH + "/" + filename
    plt.figure(figsize=(14.2, 2.3))
    plt.xticks(range(12), meses, size='xx-large')
    plt.bar(meses, valores, align='center', width=0.9)
    plt.box(on=None)
    plt.tick_params(axis='x', labeltop=False, left=False, top=False, right=False, bottom=False, labelleft=False,
                    labelright=False, labelbottom=False)
    plt.tick_params(axis='y', labeltop=False, left=False, top=False, right=False, bottom=False, labelleft=False,
                    labelright=False, labelbottom=False)
    plt.ylim([0.0, LIMITE_COMISSAO])
    plt.savefig(unicode(file_path), bbox_inches='tight')
    plt.close()
    return file_path


def trata_dados(tipo_execucao, ano_mes, selecao_recente=False):
    """ Conecta no Hive e trata os dados para criar os extratos. """
    # --- HIVE: Resgata dados para gerar os extratos --- #
    # Conecta no hive.
    console_print(u"Conectando ao hive ...")
    cursor = hive.connect(
        host=HIVESERVER2_IP
        , port=HIVESERVER2_PORT
        , auth=HIVESERVER2_AUTH
        , username=HIVESERVER2_USER
        , password=HIVESERVER2_PASS
        , kerberos_service_name=KERBEROS_SERVICE_NAME
        , configuration=HIVE_CONFIGURATION
    ).cursor()

    # Filtra os meses do começo do ano comercial e apaga meses futuros em relação ao parametro ano_mes, pois nao ha necessidade de tê-los.
    meses_ano_comercial = ano_comercial(ano_mes)
    filtro_meses_passados = '"{}"'.format('", "'.join(meses_ano_comercial[:meses_ano_comercial.index(ano_mes)]))

    # Perfis a serem filtrados para receber extratos
    perfis = '"{}"'.format('", "'.join(["FARMER", "HUNTER", "HUNTER - GIS", "HUNTER - VT"]))

    # Views que contêm os consultores que podem receber extratos.
    views = [
        "lab_comissao_vendas.vw_sodexo_dtm_ccv_consolidado_comissao_final_consultor"
        , "lab_comissao_vendas.vw_sodexo_dtm_ccv_consolidado_comissao_final_hunter_gis"
        , "lab_comissao_vendas.vw_sodexo_dtm_ccv_consolidado_comissao_final_hunter_vt"
    ]

    # Colunas utilizadas para gerar o extrato.
    colunas = ", ".join([
        "tp_execucao"
        , "ano_mes_venda"
        , "dt_carga"
        , "nu_matricula_rh"
        , "cd_execucao"
        , "cd_colaborador"
        , "nm_consultor"
        , "nm_unidade"
        , "nm_familia"
        , "nm_cargo"
        , "nm_gerente"
        , "email_gestor"
        , "vl_venda"
        , "vl_calc_comissao_final"
        , "vl_atingimento_taxa"
        , "vl_atingimento_prazo"
        , "vl_calc_ri"
        , "total_vidas"
    ])

    # Prepara query para busca dos dados. Dada a data passada no parametro, seleciona os valores dos ultimos meses do ano comercial até a data ano_mes.
    console_print(u"Preparando query ...")
    queries = []
    for view in views:
        tmp = " ".join([
            # MES ATUAL
            "SELECT"
            , colunas
            , "FROM {} as c".format(view)
            , "WHERE ano_mes_venda = \"{}\"".format(ano_mes)
            , "AND nm_perfil in ({})".format(perfis)  # Vendas publico n recebe
            , "AND tp_execucao == \"{}\"".format(tipo_execucao.lower().replace(u"é", "e"))
            , "AND isnotnull(nu_matricula_rh)"
            # HISTORICO
            , "UNION ALL"
            , "SELECT"
            , colunas
            , "FROM {} as x".format(view)
            , "WHERE ano_mes_venda in ({})".format(filtro_meses_passados)
            , "AND isnotnull(nu_matricula_rh)"
            , "AND nm_perfil in ({})".format(perfis)
            , "AND x.cd_execucao in ("
            , "SELECT max(cd_execucao) as cd_execucao"
            , "FROM {}".format(view)
            , "WHERE tp_execucao in (\"oficial\",\"ajuste\")"
            , "AND ano_mes_venda in ({})".format(filtro_meses_passados)
            , "GROUP BY ano_mes_venda, tp_execucao"
            , ")"
        ])
        queries.append(tmp)

    # Junta queries union all
    query = "\nUNION ALL\n".join(queries)

    # Executa query para puxar os dados da tabela.
    console_print(u"Solictando dados do hive ...")
    cursor.execute(query)

    # Remove nome da tabelas do nome das colunas.
    console_print(u"Tratando nomes das colunas do dataframe ...")
    nomes_colunas = [col_info[0].split(".")[-1] for col_info in cursor.description]

    # Pega todos os registros retornados e armazena em variavel.
    console_print(u"Guardando registros na memoria ...")
    registros = cursor.fetchall()

    # Fecha cursor na tabela.
    console_print(u"Finalizando conexão ao hive ...")
    cursor.close()

    # --- PANDAS --- #
    pd.set_option('display.max_columns', None)
    pd.set_option('mode.chained_assignment', None)

    # Colunas que identificam o registro como unico.
    colunas_chave = ["tp_execucao", "cd_execucao", "ano_mes_venda", "cd_colaborador"]

    # Transforma em dataframe pandas os registros retornados pelo pyhive.
    console_print(u"Transformando dados do hive em dataframe pandas ...")

    # Dataframe que contem a estrtura dos campos para qndo acontecer a transformação da tabela pivot, garantir a presença das colunas.
    df_struct = pd.DataFrame({
        "ano_mes_venda": 4 * ["0000/00"]
        , "dt_carga": 4 * ["0000-00-00"]
        , "nu_matricula_rh": 4 * [-1]
        , "cd_colaborador": 4 * [-1]
        , "nm_consultor": 4 * ["-1"]
        , "nm_unidade": 4 * ["-1"]
        , "nm_familia": ["beneficio", "gd", "ir", "gis"]
        , "nm_cargo": 4 * ["-1"]
        , "nm_gerente": 4 * ["-1"]
        , "vl_venda": 4 * [-1]
        , "vl_calc_comissao_final": 4 * [-1]
        , "vl_atingimento_taxa": 4 * [-1]
        , "vl_atingimento_prazo": 4 * [-1]
        , "vl_calc_ri": 4 * [-1]
        , "total_vidas": 4 * [-1]
        , "cd_execucao": 4 * [-1]
        , "tp_execucao": 4 * ["coringa"]
    })

    campos_valor = ["vl_venda", "vl_calc_comissao_final", "vl_atingimento_taxa", "vl_atingimento_prazo", "vl_calc_ri"]
    df = pd.DataFrame(data=registros, columns=nomes_colunas)
    df = df.append(df_struct, sort=True)
    df[campos_valor].fillna(0.0, inplace=True)
    del registros, nomes_colunas, campos_valor

    # Prepara lista de execucoes para selecionar a mais recente ou uma execucao especifica.
    df_ex = df[["ano_mes_venda", "cd_execucao", "tp_execucao", "dt_carga"]].drop_duplicates()
    df_ex["dt_carga"] = df_ex["dt_carga"].apply(lambda x: x[0:4] + "/" + x[5:7] + "/" + x[8:10])
    df_ex = df_ex.drop_duplicates()
    df_ex["cd_tp_exec"] = df_ex[["cd_execucao", "dt_carga"]].apply(lambda x: " - ".join(["{0:03d}".format(x[0]), x[1]]),
                                                                   axis="columns")
    df_ex.drop(columns=["dt_carga"], inplace=True)
    df_ex = df_ex[df_ex.cd_execucao != -1]

    # Remove coluna dt_carga.
    df.drop(columns="dt_carga", inplace=True)

    # Filtra execucoes do mes atual.
    codigos_exec = pd.unique(
        df_ex[(df_ex["tp_execucao"] == tipo_execucao.lower().replace(u"é", "e")) & (df_ex["ano_mes_venda"] == ano_mes)][
            "cd_execucao"])

    if len(codigos_exec) < 1:
        console_print(u"Nenhuma execução encontrada com os parametros recebidos")
        exit(0)

    # Seleciona execucao.
    if not selecao_recente:
        codigos_exec = map(str, codigos_exec)
        sel_exec = ""
        while (sel_exec not in codigos_exec) or (len(sel_exec) < 1):
            console_print(
                u"Selecione entre os seguintes codigos de execução:\n" + df_ex["cd_tp_exec"].sort_values().to_string(
                    index=False) + "\n")
            sel_exec = raw_input("Digite o codigo da execucao: ").strip().lstrip("0")
    else:
        sel_exec = max(codigos_exec)

    # Remove do dataframe de execucoes as execucoes do mes atual para fazer o historico.
    df_ex = df_ex[df_ex.ano_mes_venda != ano_mes]

    # Remove registros desnecessariso para o mes atual
    console_print(u"Removendo registros desnecessários do mes atual (diferentes do tipo especificado)")
    linhas_a_remover = df[(df["cd_execucao"] != int(sel_exec)) & (df["ano_mes_venda"] == ano_mes)].index
    if len(linhas_a_remover) > 0:
        df.drop(index=linhas_a_remover, inplace=True)

    # Transforma NaN para string "null" para não perder registros por estarem NaN quando transformar em pivot.
    console_print(
        u"Preenchendo campos nulos do dataframe com registros brutos pela string \"null\" para não perder registros na transformação em pivot ...")
    df.fillna("null", inplace=True)

    # POG - Insere uma linha FAKE para cada tipo de familia de produto
    # isso garantira que o pivot_table gere todos os campos necessarios
    last_index = df.index.max()
    beneficio_fake = [
        u"2018/01"
        , 123456
        , 3
        , u"GESTOR.FAKE@SODEXO.COM"
        , u"CONSULTOR FAKE"
        , u"FULANO FAKE"
        , u"beneficio"
        , u"GESTOR FAKE"
        , u"UNIDADE FAKE"
        , 123456
        , 0
        , 'ajuste'
        , 1.0
        , 1.0
        , 0.1
        , 0.1
        , 0.1
    ]
    gd_fake = [
        u"2018/01"
        , 123456
        , 3
        , u"GESTOR.FAKE@SODEXO.COM"
        , u"CONSULTOR FAKE"
        , u"FULANO FAKE"
        , u"gd"
        , u"GESTOR FAKE"
        , u"UNIDADE FAKE"
        , 123456
        , 0
        , 'ajuste'
        , 1.0
        , 1.0
        , 0.1
        , 0.1
        , 0.1
    ]
    ir_fake = [
        u"2018/01"
        , 123456
        , 3
        , u"GESTOR.FAKE@SODEXO.COM"
        , u"CONSULTOR FAKE"
        , u"FULANO FAKE"
        , u"ir"
        , u"GESTOR FAKE"
        , u"UNIDADE FAKE"
        , 123456
        , 0
        , 'ajuste'
        , 1.0
        , 1.0
        , 0.1
        , 0.1
        , 0.1
    ]
    gis_fake = [
        u"2018/01"
        , 123456
        , 3
        , u"GESTOR.FAKE@SODEXO.COM"
        , u"CONSULTOR FAKE"
        , u"FULANO FAKE"
        , u"gis"
        , u"GESTOR FAKE"
        , u"UNIDADE FAKE"
        , 123456
        , 1
        , 'ajuste'
        , 1.0
        , 1.0
        , 0.0
        , 0.0
        , 0.0
    ]

    df.loc[last_index + 1] = beneficio_fake;
    df.loc[last_index + 2] = gd_fake;
    df.loc[last_index + 3] = ir_fake;
    df.loc[last_index + 4] = gis_fake;


# final da POG

# Transforma dataframe com registros brutos em pivot para agrupar conforme o formato necessário.
console_print(u"Transformando dataframe com registros brutos em pivot para agrupar conforme o formato necessário ...")
pivot_df = pd.pivot_table(df,
                          index=colunas_chave + ["nm_consultor", "nu_matricula_rh", "nm_cargo", "nm_unidade",
                                                 "nm_gerente", "email_gestor"],
                          columns=["nm_familia"],
                          values=["vl_venda", "vl_calc_comissao_final", "vl_atingimento_taxa", "vl_atingimento_prazo",
                                  "vl_calc_ri", "total_vidas"],
                          aggfunc=np.sum
                          )

# Horizontaliza tabela pivot em um novo dataframe.
console_print(u"Transformando dataframe pivot em dataframe comum ...")
df = pd.DataFrame(pivot_df.to_records())
del pivot_df

# Renomeia colunas.
console_print(u"Renomeando colunas ...")
df.columns = [
    u"tp_execucao"
    , u"cd_execucao"
    , u"ano_mes_venda"
    , u"cd_colaborador"
    , u"nm_consultor"
    , u"nu_matricula_rh"
    , u"nm_cargo"
    , u"nm_unidade"
    , u"nm_gerente"
    , u"email_gestor"
    , u"vidas_beneficio_remover1"
    , u"vidas_gd_remover2"
    , u"vidas_gis"
    , u"vidas_ir_remover3"
    , u"flg_prazo_beneficio"
    , u"flg_prazo_gd"
    , u"flg_prazo_gis_remover4"
    , u"flg_prazo_ir"
    , u"flg_taxa_beneficio"
    , u"flg_taxa_gd"
    , u"flg_taxa_gis_remover5"
    , u"flg_taxa_ir"
    , u"comissao_beneficio"
    , u"comissao_gd"
    , u"comissao_gis"
    , u"comissao_ir"
    , u"comissao_ri_beneficio_remover6"
    , u"comissao_ri"
    , u"comissao_ri_gis_remover7"
    , u"comissao_ri_ir_remover8"
    , u"venda_beneficio"
    , u"venda_gd"
    , u"venda_gis"
    , u"venda_ir"
]

# Organiza colunas necessárias.
console_print(u"Selecionando colunas necessárias ...")
df = df[[
    u"tp_execucao"
    , u"cd_execucao"
    , u"ano_mes_venda"
    , u"cd_colaborador"
    , u"nm_consultor"
    , u"nu_matricula_rh"
    , u"nm_cargo"
    , u"nm_unidade"
    , u"nm_gerente"
    , u"email_gestor"
    , u"flg_prazo_beneficio"
    , u"flg_prazo_gd"
    , u"flg_prazo_ir"
    , u"flg_taxa_beneficio"
    , u"flg_taxa_gd"
    , u"flg_taxa_ir"
    , u"comissao_beneficio"
    , u"comissao_gd"
    , u"comissao_gis"
    , u"comissao_ir"
    , u"comissao_ri"
    , u"venda_beneficio"
    , u"venda_gd"
    , u"venda_ir"
    , u"vidas_gis"
]]

# Remove linhas coringa.
df = df[df.cd_execucao != -1]

# Converte colunas flag para string com o fim de formatar para porcentagem.
df.replace("null", np.nan, inplace=True)

# Calcula comissao recebida MUDAR FORMA PARA FAZER HISTORICO
df["comissao"] = df[["comissao_beneficio", "comissao_gd", "comissao_ir", "comissao_gis", "comissao_ri"]].sum(axis=1)
df["comissao"] = df["comissao"].apply(
    lambda x: x if x <= LIMITE_COMISSAO else LIMITE_COMISSAO)  # PENSAR NUMA VERSAO HISTORICO FUTURAMENTE

# IF int(ano_mes.replace("/","")) > 201809 ... (Caso mude a forma de calculo a partir de um mes,
# definir valor de limite de comissao a partir de ano mes

# Filtra dados para o mes solicitado.
console_print(u"Filtrando dados para o mes solicitado ...")
df_mes = df[df["ano_mes_venda"] == ano_mes]

# Gera historico.
console_print(u"Gerando historico de comissao ...")
df_mes["historico"] = ""

# Cria df historico, para cada mes do ano comercial, verifica se colaborador tem registro para aquele mes quando o cd colaborador é ajuste ou oficial.
df_historico = df[df["ano_mes_venda"] != ano_mes].sort_values(by=colunas_chave)[colunas_chave + ["comissao"]]

# Verifica todos os meses corridos até agora a partir do inicio do ano comercial até o mês passado, pois o atual esta sendo gerado.
for mes in meses_ano_comercial[:meses_ano_comercial.index(ano_mes)]:
    # Filtra execucoes desse mes.
    df_historico_mes = df_historico[df_historico["ano_mes_venda"] == mes]

    tp_execucao = "ajuste" if \
        not pd.isnull(
            df_historico_mes[df_historico_mes["tp_execucao"] == "ajuste"]["cd_execucao"].drop_duplicates().max()) \
        else "oficial" if not pd.isnull(
        df_historico_mes[df_historico_mes["tp_execucao"] == "oficial"]["cd_execucao"].drop_duplicates().max()) \
        else "vazio"

    # Verifica se Nesse mes teve execucao de ajuste e retorna a ultima execucao de ajuste.
    if tp_execucao != "vazio":
        console_print("Execucao {0} no mes: {1}".format(tp_execucao, mes))

        # Após identificado a ultima execucao, guarda codigo de execucao, que é unico.
        codigo_execucao = df_historico_mes[df_historico_mes["tp_execucao"] == tp_execucao][
            "cd_execucao"].drop_duplicates().max()

        # Armazena registros da execução especifica para comparar com outro dataframe e atribuir o valor comissao historico.
        df_historico_mes = df_historico[df_historico["cd_execucao"] == codigo_execucao][["cd_colaborador", "comissao"]]

        # Atribui para cada colaborador.
        for i in df_mes.index:
            # Armazena cod colaboradro da linha atual.
            colaborador = df_mes.at[i, "cd_colaborador"]

            # Procura por comissao do mes especifico para esse codigo.
            comissao_mes = df_historico_mes[df_historico_mes["cd_colaborador"] == colaborador]["comissao"]

            # Se encontrou colaborador, adiciona o valor da comissao.
            if len(comissao_mes) == 1:
                df_mes.at[i, "historico"] = df_mes.at[i, "historico"] + ";{0:.2f}".format(comissao_mes.values[0])

            # Senao, adiciona 0
            else:
                df_mes.at[i, "historico"] += ";0"
    else:
        df_mes["historico"] += ";0"
# Pega valor do mes atual e poe no historico de comissao
df_mes["historico"] = df_mes[["historico", "comissao"]].apply(
    lambda l: l["historico"].lstrip(";") + ";{0:.2f}".format(l["comissao"]), axis=1)

# Preenche com 0 os meses restantes (Futuros)
meses_restantes = 12 - len(meses_ano_comercial[:meses_ano_comercial.index(ano_mes) + 1])
if meses_restantes > 0:
    preenche_0 = ";0" * meses_restantes
    df_mes["historico"] = df_mes["historico"].apply(lambda x: x + preenche_0)

# Remvoe da memoria itens nao mais utilizados
del df_historico, df_historico_mes, df

# Formata gis para texto e remove .0 se valor for diferente de nulo, VENDA GIS É CAMPO Quantidade, e não moeda, se mudar no futuro, alterar aqui também
df_mes["vidas_gis"] = df_mes["vidas_gis"].astype(str).apply(lambda x: "-" if x == "nan" else x.rstrip(".0"))
df_mes["cd_execucao"] = sel_exec

# Tratamentos
campos_flag = ["flg_prazo_beneficio", "flg_taxa_beneficio", "flg_prazo_gd", "flg_taxa_gd", "flg_prazo_ir",
               "flg_taxa_ir"]
comissoes = ["comissao", "comissao_beneficio", "comissao_gd", "comissao_gis", "comissao_ir", "comissao_ri"]
vendas = ["venda_beneficio", "venda_gd", "venda_ir"]
campos_moeda = comissoes + vendas

# Trata campos moeda para formato (R$ 1.000,00) e "-".
for campo in campos_moeda:
    df_mes[campo] = df_mes[campo].astype(str).replace("nan", "-").apply(
        lambda x: "-" if x in ("-", "0.0") else valor_real(float(x)))

# Trata campos porcentagem para preencher coluna vazia com "-".
for campo in campos_flag:
    df_mes[campo] = df_mes[campo].astype(str).apply(
        lambda x: "{0}%".format(100 * (float(x) - 1)) if x not in ("nan", "0.0") else "-")

return df_mes


def cria_extrato(tipo_execucao, ano_mes, consultor):
    """
    Cria extrato consultor com valores do dicionario.
    :tipo_execucao: string, u"Prévia", u"Oficial", u"Ajuste"
    :ano_mes: string, "2018/05"
    :consultor: dicionario
    """

    def cabecalho(c, consultor, font="Times-Roman", fontsize=12):
        """ Cabeçalho extrato, c: canvas, consultor: dicionario ,font: fonte ,fontsize: tamanho fonte. """
        c.setFont(font, fontsize)
        c.setStrokeColorRGB(0.9, 0.9, 0.9)
        c.setFillColorRGB(0.9, 0.9, 0.9)
        c.drawString(7, 422, unicode(consultor[u"nm_consultor"]))
        c.drawString(32, 399, unicode(consultor[u"nu_matricula_rh"]))
        c.drawString(5, 378, unicode(consultor[u"nm_cargo"]))
        c.drawString(24, 358, unicode(consultor[u"nm_unidade"]))
        return c

    def valor_venda(c, consultor, font="Times-Roman", fontsize=12):
        """ Valores de venda, c: canvas, consultor: dicionario ,font: fonte ,fontsize: tamanho fonte. """
        c.setFont(font, fontsize)
        c.drawCentredString(11, 308, unicode(formata_valor_venda(consultor[u"venda_beneficio"])))
        c.drawCentredString(185, 308, unicode(formata_valor_venda(consultor[u"venda_gd"])))
        c.drawCentredString(363, 308, unicode(formata_valor_venda(consultor[u"venda_ir"])))
        c.drawCentredString(528, 308, unicode(consultor[u"vidas_gis"]))
        # c.drawCentredString( 685, 308, unicode(consultor[u"venda_ri"]))
        # TEMPORARIO
        c.drawCentredString(685, 308,
                            unicode(formata_valor_venda(valor_real(100 * real_valor(consultor[u"comissao_ri"])))) if
                            consultor[u"comissao_ri"] != "-" else "-")
        return c

    def comissoes(c, consultor, font="Times-Roman", fontsize=12):
        """ Valores de comissao, c: canvas, consultor: dicionario ,font: fonte ,fontsize: tamanho fonte. """
        c.setFont(font, fontsize)
        c.drawCentredString(11, 237, unicode(consultor[u"comissao_beneficio"]))
        c.setFont(font, fontsize - 5)
        c.drawCentredString(-34, 197, unicode(consultor[u"flg_taxa_beneficio"]))
        c.drawCentredString(54, 197, unicode(consultor[u"flg_prazo_beneficio"]))
        c.setFont(font, fontsize)
        c.drawCentredString(185, 237, unicode(consultor[u"comissao_gd"]))
        c.setFont(font, fontsize - 5)
        c.drawCentredString(142, 197, unicode(consultor[u"flg_taxa_gd"]))
        c.setFont(font, fontsize)
        c.drawCentredString(363, 237, unicode(consultor[u"comissao_ir"]))
        c.setFont(font, fontsize - 5)
        c.drawCentredString(319, 197, unicode(consultor[u"flg_taxa_ir"]))
        c.drawCentredString(407, 197, unicode(consultor[u"flg_prazo_ir"]))
        c.setFont(font, fontsize)
        c.drawCentredString(528, 237, unicode(consultor[u"comissao_gis"]))
        c.drawCentredString(685, 237, unicode(consultor[u"comissao_ri"]))
        return c

    def rodape(c, consultor, font="Times-Roman", fontsize=12):
        """ Informações do rodapé, c: canvas, consultor: dicionario ,font: fonte ,fontsize: tamanho fonte. """
        c.setFont(font, fontsize)
        c.drawString(87, -24, unicode(ano_mes))
        c.drawString(70, -50, unicode(PROCESSING_DATE))
        c.drawString(348, -23, unicode(tipo_execucao))
        c.drawString(364, -50, "{0:03d}".format(int(consultor[u"cd_execucao"])))
        return c

    def comissao_final(c, consultor, font, fontsize=12):
        """ Valor de comissao final, c: canvas, consultor: dicionario ,font: fonte ,fontsize: tamanho fonte. """
        c.setFont(font, fontsize)
        c.setFillColorRGB(0, 1, 0.4)
        c.drawCentredString(615, 100, unicode(consultor[u"comissao"]))
        return c

    def grafico(c, consultor):
        """ Posiciona gráfico no extrato, c: canvas, consultor: dicionario ,font: fonte ,fontsize: tamanho fonte. """
        c.drawImage(consultor[u"grafico"], -62, 55, 504, 68)
        return c

    tipo = tipo_execucao[0].upper()
    email_gestor = consultor[u"email_gestor"].strip() if len(consultor[u"email_gestor"].strip()) > 2 else "Excecoes"
    caminho_pdf = u"{0}/{1}_{2}_{3:03d}/{4}/{1}_{5}_{6}_{2}_{3:03d}.pdf".format(
        STATEMENTS_OUTPUT_PATH  # 0
        , ano_mes.replace("/", "")  # 1
        , tipo  # 2
        , int(cod_execucao)  # 3
        , email_gestor  # 4
        , consultor[u"nu_matricula_rh"]  # 5
        , nome_arquivo(consultor[u"nm_consultor"]).upper()  # 6
    )
    nome_pdf = caminho_pdf.split("/")[-1].rstrip(".pdf")
    # Cria diretorios se n existirem
    makedirs_if_not_exists(caminho_pdf.rsplit("/", 1)[0])
    c = canvas.Canvas(caminho_pdf, pagesize=landscape(A4))
    c.setTitle(nome_pdf)
    # Background
    img_base = BG_PREVIEW if tipo == "P" else BG_OFFICIAL if tipo == "O" else BG_ADJUSTMENT
    c.drawImage(img_base, 0, 0, 842, 596)
    c.translate(inch, inch)
    # Preenche PDF
    c = cabecalho(c, consultor, font="Helvetica", fontsize=16)
    c = valor_venda(c, consultor, font="Helvetica", fontsize=11)
    c = comissoes(c, consultor, font="Helvetica", fontsize=16)
    c = rodape(c, consultor, font="Courier-Bold", fontsize=14)
    c = comissao_final(c, consultor, font="Helvetica", fontsize=20)
    c = grafico(c, consultor)
    # Salva arquivo
    c.showPage()
    c.save()
    console_print(u"Extrato gerado: " + nome_pdf)


def valida_email(email):
    return re.match(r"[^@]+@[^@]+\.[^@]+", email)


def recebe_argumentos():
    """ Recebe parametros e trata valores. """
    # Recebe parametros
    parser = argparse.ArgumentParser(description=u"Geração e envio de extratos.")
    # Recebe ano mes
    parser.add_argument("-mes", "-m", help=u"Mes base calculo, formato (AAAAMM), ex.: 201804.", required=True)
    # Recebe tipo de extrato
    group_type = parser.add_mutually_exclusive_group(required=True)
    group_type.add_argument("--previa", "--p", help=u"Usa layout prévia.", action=u"store_true")
    group_type.add_argument("--oficial", "--o", help=u"Usa layout oficial.", action=u"store_true")
    group_type.add_argument("--ajuste", "--a", help=u"Usa layout ajustes.", action=u"store_true")
    # Selecao dos dados
    parser.add_argument("--recente", "--r", help=u"Seleciona a execucao mais recente automaticamente.", required=False,
                        action=u"store_true")
    # Lê argumentos
    args = parser.parse_args()
    # Trata Ano e Mes para formato AAAA/MM
    if len(args.mes) != 6:
        exit("Parametro mes incorreto!")
    ano_mes = args.mes[0:4] + "/" + args.mes[4:]
    # Redireciona execução para tipo especifico de extrato
    if args.previa:
        tipo = u"Prévia"
    elif args.oficial:
        tipo = u"Oficial"
    else:  # args.ajuste:
        tipo = u"Ajuste"
    return ano_mes, tipo, args.recente


if __name__ == "__main__":
    """ """
    makedirs_if_not_exists(STATEMENTS_OUTPUT_PATH)
    makedirs_if_not_exists(CSV_OUTPUT_PATH)

    ano_mes, tipo_execucao, flg_recente = recebe_argumentos()
    console_print(u"Tipo de execução: " + tipo_execucao)

    # Lê dados para gerar extratos
    console_print(u"Lendo base de dados ...")
    consultores_vendedores = trata_dados(tipo_execucao, ano_mes, flg_recente).to_dict("records")
    console_print(u"Estão sendo gerados " + str(len(consultores_vendedores)) + " extratos.")

    # Prepara meses gráfico
    meses_ano_comercial = ["Set", "Out", "Nov", "Dez", "Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago"]

    cod_execucao = int(consultores_vendedores[0]["cd_execucao"])
    t_execucao = tipo_execucao[0].upper()

    # Gerar extratos
    for i, consultor in enumerate(consultores_vendedores):
        valores = map(float, consultor[u"historico"].split(";"))
        # Trata nome do consultor para: Letra Maiúscula Depois De Espaço
        consultor[u"grafico"] = cria_grafico(meses_ano_comer
        cial, valores, filename = "{0}.png".format(i))
        # Cria PDF
        cria_extrato(tipo_execucao, ano_mes, consultor)
        # Remove gráfico apos criação do pdf
        remove(consultor[u"grafico"])

    if tipo_execucao == "Oficial":
        csv_filename = "{0}_{1}_{2:03d}.txt".format(ano_mes.replace("/", ""), t_execucao, cod_execucao)
        try:
            console_print(u"Criando arquivo txt ...")
            with codecs.open(CSV_OUTPUT_PATH + "/" + csv_filename, "w", "utf-8") as f:
                for consultor in consultores_vendedores:
                    # Numero empresa: 7 (fixo)
                    # Matricula RH: x
                    # Codigo evento: 0025 (fixo)
                    # Valor comissão: y
                    f.write(";".join(
                        ["7"
                            , str(consultor["nu_matricula_rh"])
                            , "0025"
                            , "0,00" if consultor["comissao"] == "-" else consultor["comissao"].lstrip("R$").strip(
                            " ").replace(".", "")])
                            + "\n"
                            )
                f.flush()
                f.close()
            console_print(u"CSV criado em {0}: {1}".format(CSV_OUTPUT_PATH, csv_filename))
        except Exception as ex:
            print(ex)

    elif tipo_execucao == "Ajuste":
        csv_filename = "{0}_{1}_{2:03d}.txt".format(ano_mes.replace("/", ""), t_execucao, cod_execucao)
        try:
            console_print(u"Criando arquivo txt ...")
            with codecs.open(CSV_OUTPUT_PATH + "/" + csv_filename, "w", "utf-8") as f:
                for consultor in consultores_vendedores:
                    # Numero empresa: 7 (fixo)
                    # Matricula RH: x
                    # Codigo evento: 0025 (fixo)
                    # Valor comissão: y
                    f.write(";".join(
                        ["7"
                            , str(consultor["nu_matricula_rh"])
                            , "0025"
                            , "0,00" if consultor["comissao"] == "-" else consultor["comissao"].lstrip("R$").strip(
                            " ").replace(".", "")])
                            + "\n"
                            )
                f.flush()
                f.close()
            console_print(u"CSV criado em {0}: {1}".format(CSV_OUTPUT_PATH, csv_filename))
        except Exception as ex:
            print(ex)
        # todo Fazer subtracao da ultima execucao do mesmo mes com a execucao de agora
    else:  # prévia
        pass
