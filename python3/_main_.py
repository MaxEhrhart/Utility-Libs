def imprime_lista(lista):
    for item in lista:
        print(item)


def indenta_creates(arquivo):
    with open(arquivo,"r") as f0:
        creates = list(map(lambda x: x.strip(),f0.readlines()))
        creates = list(filter(None,creates))
        f0.close()

    flg_campos = 0
    contagem_parenteses = 0
    l = []
    for linha in creates:
        if "create" in linha.lower() and "(" in creates[creates.index(linha) + 1]:
            flg_campos = 1
            ind_create = creates.index(linha)
        if ";" in linha[-1]:
            linha += "\n"
        contagem_parenteses = flg_campos + linha.count("(") - linha.count(")")
        if contagem_parenteses > 0 and flg_campos:
            if not "create" in linha.lower() and not "(" in linha.lower():
                aux = linha.split()
                if aux[0][0] != ",":
                    aux[0] = " " + aux[0]
                linha = "    {0}{1}{2}".format(aux[0]," "*(40-len(aux[0]))," ".join(aux[1:]))
        else:
            flg_campos = False
        print(linha)
    imprime_lista(l)


def lista_conteudo_diretorio():
    from os import walk
    from os.path import join

    for root, directories, filenames in walk(pasta_geral):
        # pastas
        for directory in directories:
            print(join(root, directory).replace("\\","/"))

        # arquivos
        for filename in filenames:
            print(join(root,filename).replace("\\","/"))


def concatena_creates_v2(pasta_geral):
    from os import walk, listdir, remove
    from os.path import join, exists, isfile, isdir

    create_files = []
    filler = "\n" + "-"*100 + "\n"
    header = "-"*100+"\n"+"set hive.exec.dynamic.partition=true;\n" + \
             "set hive.exec.dynamic.partition.mode=nonstrict;\n" + \
             "set hive.exec.max.dynamic.partitions.pernode=10000;" + filler

    caminho_base_pasta,pasta_base = pasta_geral.rsplit("/",1)
    arquivo_destino = "{0}/rh_create_{1}.hql".format(caminho_base_pasta,pasta_base)

    for root, directories, filenames in walk(pasta_geral):
        #for directory in directories:
        #    print(join(root, directory).replace("\\","/"))
        for filename in filenames:
            file = join(root,filename).replace("\\","/")
            if "create" in file.lower():
                create_files.append(file)

    if exists(arquivo_destino):
        remove(arquivo_destino)

    with open(arquivo_destino,"w",encoding="utf-8",newline="\n") as f0:
        f0.write(header)
        for file in create_files:
            diretorio_arquivo_hql = file.split("/")[-2]
            with open(file,"r",encoding="utf-8-sig") as f1:
                f0.write("{0}-- ##### CREATE {1} #####{0}".format(filler,diretorio_arquivo_hql))
                f0.writelines(f1.readlines())
                f1.close()
            print("Adicionado create da pasta {0} com sucesso!".format(diretorio_arquivo_hql))
        f0.close()


"""
def concatena_creates_v1(pasta_geral):
    from os import listdir,remove
    from os.path import exists, isfile, isdir

    header = "set hive.exec.dynamic.partition=true;\n" + \
             "set hive.exec.dynamic.partition.mode=nonstrict;\n" + \
             "set hive.exec.max.dynamic.partitions.pernode=10000;\n"

    filler = "\n" + "-"*100 + "\n"

    caminho_base_pasta,pasta_base = pasta_geral.rsplit("/",1)
    arquivo_destino = "{0}/rh_create_{1}.hql".format(caminho_base_pasta,pasta_base)
    if exists(arquivo_destino):
        remove(arquivo_destino)

    with open(arquivo_destino,"w",encoding="utf-8",newline="\n") as f0:
        f0.write(header)

        lista_pastas = list(map(lambda x: pasta_geral + "/" + x,listdir(pasta_geral)))
        lista_pastas = [p for p in lista_pastas if isdir(p)]

        for pasta in lista_pastas:
            arquivos_hql = list(map(lambda x: pasta + "/" + x,listdir(pasta)))
            caminho_pasta_hql,pasta_hql = pasta.rsplit("/",1)
            flg_print = 0
            for hql in arquivos_hql:
                if "create" in hql:
                    flg_print = 1
                    with open(hql,"r",encoding="utf-8-sig") as f1:
                        f0.write(filler + "-- ##### CREATE " + pasta_hql + " #####" + filler)
                        f0.writelines(f1.readlines())
                        f1.close()

            if flg_print:
                print("Adicionado create da pasta {0} com sucesso!".format(pasta_hql))
        f0.close()
"""


def create_lines(file):
    linhas_create = []
    with open(file,"r+",encoding="utf-8") as f0:
        arq_create = f0.readlines()
        f0.close()

    for linha in arq_create:
        linha = linha.lower()
        if not linha.startswith("--") and ("create" in linha):
            if "external" in linha \
            or "table"    in linha \
            or "view"     in linha:
                linha = " ".join(linha \
                    .replace("\n","") \
                    .replace("(","") \
                    .replace("select","") \
                    .replace("\t"," "*4) \
                    .rstrip() \
                    .split()) \
                    .rsplit(" as")[0]
                linhas_create.append(linha)
    return(linhas_create)


def drop_lines(create_lines):
    drop = []
    for line in create_lines:
        words = line.split()
        ind_object_type = words.index("view") if "view" in line else words.index("table")
        drop.append("DROP {0} IF EXISTS {1};\n".format(words[ind_object_type].upper(), words[-1]))
    return drop


def create_databases(drop_lines):
    databases = ["CREATE DATABASE IF NOT EXISTS {0};\n".format(line.rstrip(";").split()[-1].split(".")[0]) for line in drop_lines]
    return sorted(list(set(databases)),reverse=False)


def chmod_chown_commands(drop_lines):
    path_stg = "/gpa/rawzone/stg/{0}/{1}"
    path_arc = "/gpa/rawzone/arc/{0}/{1}"
    path_landing_zone = "/bid/landing_zone/{0}/{1}"

    tables = [line.strip().rstrip(";").split()[-1].split(".") for line in drop_lines if ("VIEW" not in line and "ext_" not in line)]
    chmod_stg = "chmod 775 \\\n" + " \\\n".join([path_stg.format("_".join(line[0].split("_")[1:]),line[1]) for line in tables]) + "\n"
    chmod_arc = "chmod 775 \\\n" + " \\\n".join([path_arc.format("_".join(line[0].split("_")[1:]),line[1]) for line in tables]) + "\n"
    chmod_landingzone = "chmod 775 \\\n" + " \\\n".join([path_landing_zone.format("_".join(line[0].split("_")[1:]),line[1]) for line in tables]) + "\n"
    chown_stg = "hdfs dfs -chown sv3main:prod \\\n" + " \\\n".join([path_stg.format("_".join(line[0].split("_")[1:]),line[1]) for line in tables]) + "\n"
    chown_arc = "hdfs dfs -chown sv3main:prod \\\n" + " \\\n".join([path_arc.format("_".join(line[0].split("_")[1:]),line[1]) for line in tables]) + "\n"

    return chmod_stg, chmod_arc, chmod_landingzone, chown_stg, chown_arc


def mkdir_commands(drop_lines):
    path_stg = "/gpa/rawzone/stg/{0}/{1}"
    path_arc = "/gpa/rawzone/arc/{0}/{1}"
    path_landing_zone = "/bid/landing_zone/{0}/{1}"

    tables = [line.strip().rstrip(";").split()[-1].split(".") for line in drop_lines if ("VIEW" not in line and "ext_" not in line)]
    mkdir_stg = "sudo -u hdfs hdfs dfs -mkdir -p \\\n" + " \\\n".join([path_stg.format("_".join(line[0].split("_")[1:]),line[1]) for line in tables]) + "\n"
    mkdir_arc = "sudo -u hdfs hdfs dfs -mkdir -p \\\n" + " \\\n".join([path_arc.format("_".join(line[0].split("_")[1:]),line[1]) for line in tables]) + "\n"
    mkdir_landingzone = "mkdir -p \\\n" + " \\\n".join([path_landing_zone.format("_".join(line[0].split("_")[1:]),line[1]) for line in tables]) + "\n"

    return mkdir_stg, mkdir_arc, mkdir_landingzone


def lista_duplicatas(L):
    seen = set()
    seen2 = set()
    seen_add = seen.add
    seen2_add = seen2.add
    for item in L:
        if item in seen:
            seen2_add(item)
        else:
            seen_add(item)
    return list(seen2)


def ordena(lista,r=False,unico=False):
    return sorted(list(set(lista)),reverse=r) if unico else sorted(lista,reverse=r)


if __name__ == "__main__":
    from os import getcwd
    import sys

    base_path = "C:/git-repositories/Projeto-BID/hql/PROJETO_RH/" if len(sys.argv) > 1 else getcwd().replace("\\","/") + "/"
    drop_ddl_refined =[]
    drop_ddl_trusted =[]
    drop_ddl_raw = []
    drop_ddl_all = []

    #indenta_creates(base_path+"DDL/RH/rh_create_gerados.hql")
    concatena_creates_v2(base_path+"rawzone")
    concatena_creates_v2(base_path+"refinedzone")
    concatena_creates_v2(base_path+"trustedzone")

    drop_ddl_refined += drop_lines(create_lines(base_path+"rh_create_refinedzone.hql"))
    drop_ddl_trusted += drop_lines(create_lines(base_path+"rh_create_trustedzone.hql"))
    drop_ddl_raw += drop_lines(create_lines(base_path+"rh_create_rawzone.hql"))

    drop_ddl_all = drop_ddl_raw + drop_ddl_trusted + drop_ddl_refined
    drop_ddl_all = ordena(drop_ddl_all,True)
    #imprime_lista(lista_duplicatas(drop_ddl_all))

    drop_ddl_raw = ordena(drop_ddl_raw,True)

    mkdir_stg, mkdir_arc, mkdir_landingzone = mkdir_commands(drop_ddl_raw)
    chmod_stg, chmod_arc, chmod_landingzone, chown_stg, chown_arc = chmod_chown_commands(drop_ddl_raw)

    with open(base_path + "rh_create_databases.hql","w",encoding="utf-8",newline="\n") as f:
        f.writelines(create_databases(drop_ddl_all))
        f.close()

    with open(base_path + "rh_drop_commands.hql","w",encoding="utf-8",newline="\n") as f:
        f.writelines(drop_ddl_all)
        f.close()

    with open(base_path + "rh_mkdir_commands.txt","w",encoding="utf-8",newline="\n") as f:
        f.write(mkdir_stg+"\n")
        f.write(mkdir_arc+"\n")
        f.write(mkdir_landingzone+"\n")
        f.close()

    with open(base_path + "rh_chmod_chown_commands.txt","w",encoding="utf-8",newline="\n") as f:
        f.write(chmod_stg+"\n")
        f.write(chmod_arc+"\n")
        f.write(chmod_landingzone+"\n")
        f.write(chown_stg+"\n")
        f.write(chown_arc)
        f.close()
