#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Requisitos: #
"""
    pip install pywin32
"""
import argparse
import win32com.client as win32
import time
from sys import exit
from os import listdir, walk
from os.path import abspath, dirname, basename, exists, isdir
from zipfile import ZipFile, ZIP_DEFLATED
from six import PY2, PY3

def recebe_argumentos():
    """ Recebe argumentos como parametro para execução. """
    parser = argparse.ArgumentParser(description='Envio de extratos.')
    parser.add_argument("--recente", "--r", help="Seleciona a execução mais recente automaticamente.", required=False,action="store_true")
    pasta = parser.add_mutually_exclusive_group(required=False)
    pasta.add_argument("--pasta_base", "--pb", help="Se esse parametro for passado, muda o local padrão onde são buscados os anexos para essa execução. Passar caminho todo, ex.: c:/.../...", required=False,type=str)
    pasta.add_argument("--pasta_execucao", "--pe", help="Se esse parametro for passado, recebe como parametro diretamente a pasta \"AAAAMM_T_000\" e envia os extratos para os gestores nela contidos.", required=False,type=str)
    args = parser.parse_args()
    return args.recente, args.pasta_base, args.pasta_execucao


def mes_do_ano(ano_mes):
    meses = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
    return meses[int(ano_mes[4:])-1]


def cria_zip(lista_arquivos,diretorio_zip):
    """ Cria arquivo zip na pasta especificada. """
    # Cria ZIP para anexar
    #nome_zip = u"[{}-{}-{}] - Extratos de comissão de ".format(ano_mes, tipo_execucao.upper(),codigo_execucao)
    #anexo = cria_zip(arquivos)
    with ZipFile(diretorio_zip+"/"+nome_zip, 'w', ZIP_DEFLATED) as zip:
        for arquivo in lista_arquivos:
            zip.write(basename(arquivo))
        zip.close()
    return diretorio_zip + "/" + nome_zip
    

if __name__ == "__main__":

    # Recebe Parâmetros
    p_recente, p_pasta_base, p_pasta_execucao = recebe_argumentos()
    
    # Verifica se parametro pasta_execucao foi passado e se é valido, se sim, pula para envio de email
    if p_pasta_execucao != None:
        if isdir(p_pasta_execucao):
            pasta_execucao = p_pasta_execucao.replace("\\", "/")
        else:
            print(u"Pasta execução invalida")
            exit()
    else:
        # Verifica se parametro pasta_base foi passado e se é valido, se sim, prossegue com o valor especificado, senao, utiliza pasta atual do script
        if p_pasta_base != None:
            if isdir(p_pasta_base):
                pasta_base = p_pasta_base.replace("\\", "/")
            else:
                print(u"Pasta base invalida")
                exit()
        else:
            pasta_base = abspath(dirname(__file__)).replace("\\", "/")
            
        pasta_execucoes = sorted(walk(pasta_base).next()[1]) if PY2 else sorted(walk(pasta_base).__next__()[1])
        pasta_execucoes = [(int(pasta_execucao.split("_")[2]),pasta_execucao) for pasta_execucao in pasta_execucoes if len(pasta_execucao) == 12]

        # Verifica se tem execucoes para enviar
        if len(pasta_execucoes) == 0:
            print(u"Nenhuma pasta de execução encontrada")
            exit()

        # Lista códigos de execução encontrados
        selecao = list(zip(*pasta_execucoes))[0]
        print(selecao)
        # Seleção de execuções
        if not p_recente:
        
            opcoes = ["{:03d} - {}\n".format(pasta_execucao[0],pasta_execucao[1]) for pasta_execucao in pasta_execucoes]
            codigos_exec = list(map(str,selecao))
            sel_exec = ""
            
            while (sel_exec not in codigos_exec) or (len(sel_exec) < 1):
                print(u"Selecione uma pasta entre os seguintes códigos de execução:\n")
                print(u"".join(opcoes))
                texto_input = "Digite o codigo da execucao: "
                sel_exec = raw_input(texto_input).strip().lstrip("0") if PY2 else input(texto_input).strip().lstrip("0")
        else:
            sel_exec = max(selecao)

        # Pasta execução com os emails dos gestores no nome
        pasta_execucao = pasta_base + "/" + list(filter(lambda t: t[0] == int(sel_exec), pasta_execucoes))[0][1]
    
    ano_mes, tipo_execucao, codigo_execucao = pasta_execucao.rsplit("/",1)[-1].split("_")
    tipo_execucao = "Prévia" if tipo_execucao.upper() == "P" else "Oficial" if tipo_execucao.upper() == "O" else "Ajuste"

    # Verifica se existem pasta com @ no nome
    pastas_gestores = [x for x in listdir(pasta_execucao) if x.find("@") != -1]
    
    if len(pastas_gestores) == 0:
        print(u"Nenhuma pasta contendo email do gestor foi encontrada")
        exit()
    
    # Abre Outlook para envios
    outlook = win32.Dispatch('outlook.application')

    # Pasta com nomes dos gestores e extratos
    for pasta_gestor in listdir(pasta_execucao):

        # Email gestor no nome da pasta
        endereco_email = pasta_gestor.strip()

        # Adiciona Caminho da pasta à pasta gestor
        pasta_gestor = pasta_execucao+"/"+pasta_gestor

        # Lista de PDFs na pasta
        arquivos = [pasta_gestor + "/" + s for s in listdir(pasta_gestor)]
        
        # Se a pasta possui arquivos para anexar, envia, senao, pula para o proximo
        if len(arquivos) > 0:
            
            # EMAIL
            email = outlook.CreateItem(0)
            email.To = 'gabriel.roque@sodexo.com'#endereco_email
            email.Subject = "[{0}_{1}] Comissão de Vendas".format(ano_mes, tipo_execucao.upper())
            email.HTMLBody = 'Prezado(a),'
            email.HTMLBody += '<br>'
            email.HTMLBody += '<br>'
            email.HTMLBody += 'Segue(m) extrato(s) de Comissão de Vendas referente ao mês base <b>Junho</b>.'
            email.HTMLBody += '<br>'
            email.HTMLBody += '<br>'
            email.HTMLBody += '<hr>'
            email.HTMLBody += 'Em caso de dúvidas sobre o pagamento e as regras de Comissão de Vendas:'
            email.HTMLBody += '<br>'
            email.HTMLBody += '---: Bruno Godoi'
            email.HTMLBody += '<br>'
            email.HTMLBody += '---: +55 11 3594-7958'
            email.HTMLBody += '<br>'
            email.HTMLBody += '---: bruno.godoi@sodexo.com'
            email.HTMLBody += '<hr>'
            email.HTMLBody += 'Em caso de dúvidas sobre o cálculo e os extratos:'
            email.HTMLBody += '<br>'
            email.HTMLBody += '---: Gabriel Roque'
            email.HTMLBody += '<br>'
            email.HTMLBody += '---: +55 11 3594-7541'
            email.HTMLBody += '<br>'
            email.HTMLBody += '---: gabriel.roque@sodexo.com'
            email.HTMLBody += '<br>'
            email.HTMLBody += '<hr>'
            email.HTMLBody += '<i>e-mail gerado automaticamente em {0}'.format(time.strftime("%Y-%m-%d %H:%M:%S"))
            email.HTMLBody += '<br>'
            email.HTMLBody += 'equipe de Dados.</i>'

            # Anexa arquivos
            for anexo in arquivos:
                email.Attachments.Add(anexo)

            # Envia
            try:
                print(u"Enviando extratos para: " + endereco_email)
                email.Send()
                #email.Display()
                print(u"Email enviado!")
            except Exception as ex:
                print(u"Falha ao enviar extratos para: " + endereco_email + "\n{}\n\n".format(str(ex)))

        else:
            print(u"Nenhum anexo encontrado para o seguinte gestor: {}".format(endereco_email))

    # Fecha Outlook após envio
    #outlook.Quit()
