# -*- encoding: utf-8 -*-
"""
    Requisitos:

    pip install paramiko
"""
from sys import exit
import argparse
import paramiko
from os.path import abspath, dirname, basename, exists, isdir
from os import makedirs
from six import PY2, PY3
import stat

INFO_CONEXAO = {
    'hostname': "52.201.20.187"
    , 'username': "centos"
    , 'key_filename': abspath(dirname(__file__)).replace("\\", "/") + "/rsa_key_servidor.pem"
    , 'port': 22
}

DIRETORIO_EXTRATOS = "/home/centos/Projects/Comissao_Vendas/reports"


def cria_diretorios_se_nao_existirem(dirs):
    if not exists(dirs):
        makedirs(dirs)


def recebe_argumentos():
    parser = argparse.ArgumentParser(description='Transferencia de arquivos de extratos.')
    parser.add_argument("--pasta_destino", "--pd",
                        help=u"Define a pasta destino para os diretórios a serem baixados. Passar caminho completo.",
                        required=False, type=str)
    params = parser.add_mutually_exclusive_group(required=False)
    params.add_argument("--recente", "--r", help=u"Seleciona a execução mais recente automaticamente.", required=False,
                        action="store_true")
    params.add_argument("--todos", "--t",
                        help=u"Baixa todas as pastas de execuções se existir para a pasta onde o script está localizado atualmente.",
                        required=False, action="store_true")
    args = parser.parse_args()
    return args.recente, args.todos, args.pasta_destino


def conecta_servidor():
    """ Conecta-se ao servidor. """
    # logging.getLogger('paramiko').setLevel(logging.DEBUG)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # ssh.connect(HOST, port=PORT, username=USERNAME, key_filename=SSH_KEY)
    ssh.connect(**INFO_CONEXAO)
    return ssh


if __name__ == "__main__":
    # Recebe argumentos.
    p_recente, p_todos, p_pasta_destino = recebe_argumentos()

    # Verifica se parametro pasta destino passado é valido.
    if p_pasta_destino != None:
        if isdir(p_pasta_destino):
            pasta_destino = p_pasta_destino.replace("\\", "/")
        else:
            print(u"Pasta destino invalida!")
            exit()
    else:
        pasta_destino = abspath(dirname(__file__)).replace("\\", "/")

    # Conecta-se ao servidor.
    ssh = conecta_servidor()

    # PEGAR APENAS PASTAS QUE TEM PDFS PARA MATNER A CONSISTENCIA COM O PROGRAMA
    # Executa comando para listar pastas de execução no servidor.

    # Resgata diretorio de todos os pdfs na pasta e filtra logo após
    comando = "find {0} -type f | grep pdf".format(DIRETORIO_EXTRATOS)
    stdin, stdout, stderr = ssh.exec_command(comando)

    t_dir_extratos = len(DIRETORIO_EXTRATOS)
    arquivos = [x.rstrip("\n") for x in stdout.readlines()]

    # Verifica se existe uma pasta válida.
    pasta_execucoes = list(set([arquivo.rsplit("/", 2)[0][t_dir_extratos + 1:] for arquivo in arquivos if
                                len(arquivo.rsplit("/", 2)[0][t_dir_extratos + 1:]) == 12]))

    # Se não existe pasta válida, sai do programa.
    if len(pasta_execucoes) == 0:
        print(u"Nenhuma pasta de execução localizada.")
        ssh.close()
        exit()

    # Se parametro todos for passado, baixa todas as pastas de execução e ignora parâmetro --recente.
    if not p_todos:

        # Formata como tupla (codigo exec, pasta exec)
        pasta_execucoes = [(int(pasta_execucao.split("_")[2]), pasta_execucao) for pasta_execucao in pasta_execucoes]

        # Lista códigos de execução encontrados.
        selecao = list(zip(*pasta_execucoes))[0]

        # Seleção de execuções
        if not p_recente:

            opcoes = ["{0:03d} - {1}\n".format(pasta_execucao[0], pasta_execucao[1]) for pasta_execucao in
                      pasta_execucoes]
            codigos_exec = list(map(str, selecao))
            sel_exec = ""

            while (sel_exec not in codigos_exec) or (len(sel_exec) < 1):
                print(u"Selecione uma pasta para copiar entre os seguintes códigos de execução:\n")
                print(u"".join(opcoes))
                texto_input = "Digite o codigo da execucao: "
                sel_exec = raw_input(texto_input).strip().lstrip("0") if PY2 else input(texto_input).strip().lstrip("0")
        else:
            sel_exec = max(selecao)

        # Filtra o código selecionado.
        pasta_execucao = list(filter(lambda t: t[0] == int(sel_exec), pasta_execucoes))[0][1]

        # Filtra arquivos com a execução selecionada.
        arquivos = [x for x in arquivos if pasta_execucao in x]

    # Inicia FTP
    with ssh.open_sftp() as sftp:

        qtd_arquivos = len(arquivos)
        if qtd_arquivos < 1:
            print("Nenhum arquivo encontrado para transferir.")
            sftp.close()
            ssh.close()
            exit()

        # Pega pastas execução base únicas.
        # print(list(set([arquivo.rsplit("/",2)[0] for arquivo in arquivos])))

        # Cria todas as pastas necessárias.
        pastas_destino = list(set([pasta_destino + arquivo.rsplit("/", 1)[0][t_dir_extratos:] for arquivo in arquivos]))

        for pasta in pastas_destino:
            cria_diretorios_se_nao_existirem(pasta)

        arquivos_baixados = 0
        for arquivo in arquivos:
            arquivos_baixados = arquivos_baixados + 1
            print("Baixando arquivo {} de {}: {}".format(arquivos_baixados, qtd_arquivos, arquivo.rsplit("/", 1)[1]))
            arquivo_destino = pasta_destino + arquivo.rsplit("/", 1)[0][t_dir_extratos:] + "/" + arquivo.rsplit("/", 1)[
                1]
            sftp.get(arquivo, arquivo_destino)

        # Fecha conexão do SFTP.
        sftp.close()

    # Fecha conexão ao SSH.
    ssh.close()
