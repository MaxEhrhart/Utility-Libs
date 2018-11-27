#!/usr/bin/python
import csv
import sys
import os.path

#Versao 2 do gerador das RawZones

#global
DIR_PYGEN = "/bid/_temporario_/pythonGeneration/"
DIR_INTGR_PAR = "/bid/integration_layer/par/"
DIR_INTGR_JOB = "/bid/integration_layer/job/"

def Sqoop(filename,source_database,source_table, split_field, sourcesystem, tablename):
    nomearquivo_INI = DIR_INTGR_PAR + filename + ".ini"
    nomearquivo_SH = DIR_INTGR_JOB + filename + ".sh"

    with open(DIR_PYGEN + "Par_Examples/HD008.ini") as f, open(nomearquivo_INI, "w") as f1:
        for line in f:
            if "# HD008.ini" in line:
                Modifiedline = "# " + filename + ".ini\n"
                f1.write(Modifiedline)
            elif "export SOURCE_DATABASE=" in line:
                Modifiedline2 = "export SOURCE_DATABASE=\"" + source_database +  "\"\n"
                f1.write(Modifiedline2)
            elif "export SOURCE_TABLE=" in line:
                Modifiedline = "export SOURCE_TABLE=\"" + source_table + "\"\n"
                f1.write(Modifiedline)
            elif "export SPLIT_FIELD=" in line:
                Modifiedline = "export SPLIT_FIELD=\"" + split_field + "\"\n"
                f1.write(Modifiedline)
            elif "export SOURCESYSTEM=" in line:
                Modifiedline = "export SOURCESYSTEM=\"" + sourcesystem + "\"\n"
                f1.write(Modifiedline)
            elif "export TABLENAME=" in line:
                Modifiedline = "export TABLENAME=\"" + tablename + "\"\n"
                f1.write(Modifiedline)
            else:
                f1.write(line)
    print "01 :   Created the ini file " + nomearquivo_INI + " sucessfully."
    GenerateSH("01",nomearquivo_SH,DIR_PYGEN + "Job_Examples/HD008.sh")

def TransferirArquivos(filename,database,tablename):
    nomearquivo_INI = DIR_INTGR_PAR + filename + ".ini"
    nomearquivo_SH = DIR_INTGR_JOB + filename + ".sh"
    with open(DIR_PYGEN + "Par_Examples/HD001.ini") as f, open(nomearquivo_INI, "w") as f1:
        for line in f:
            if "# SCRIPTNAMEGOESHERE" in line:
                Modifiedline = "# " + filename + ".ini\n"
                f1.write(Modifiedline)
            elif "export DATABASE=" in line:
                Modifiedline = "export DATABASE=\"" + database + "\"\n"
                f1.write(Modifiedline)
            elif "export TABLENAME=" in line:
                Modifiedline2 = "export TABLENAME=\"" + tablename +  "\"\n"
                f1.write(Modifiedline2)
            else:
                f1.write(line)
    print "01 :   Created the " + nomearquivo_INI + " sucessfully."
    GenerateSH("01",nomearquivo_SH,DIR_PYGEN + "Job_Examples/HD001.sh")

def CargaRawZone(filename,database,tablename):
    nomearquivo_INI = DIR_INTGR_PAR + filename + ".ini"
    nomearquivo_SH = DIR_INTGR_JOB + filename + ".sh"
    with open(DIR_PYGEN + "Par_Examples/HD002.ini") as f, open(nomearquivo_INI, "w") as f1:
        for line in f:
            if "# SCRIPTNAMEGOESHERE" in line:
                Modifiedline = "# " + filename + ".ini\n"
                f1.write(Modifiedline)
            elif "export DATABASE=" in line:
                Modifiedline = "export DATABASE=\"" + database + "\"\n"
                f1.write(Modifiedline)
            elif "export TABLENAME=" in line:
                Modifiedline2 = "export TABLENAME=\"" + tablename +  "\"\n"
                f1.write(Modifiedline2)
            else:
                f1.write(line)

    print "02 :   Created the " + nomearquivo_INI + " sucessfully."
    GenerateSH("02",nomearquivo_SH,DIR_PYGEN + "Job_Examples/HD002.sh")

def Profile(filename,database,tablename,columnsnames):
    nomearquivo_INI = DIR_INTGR_PAR + filename + ".ini"
    nomearquivo_SH = DIR_INTGR_JOB + filename + ".sh"
    with open(DIR_PYGEN + "Par_Examples/HD003.ini") as f, open(nomearquivo_INI, "w") as f1:
        for line in f:
            if "# SCRIPTNAMEGOESHERE" in line:
                Modifiedline = "# " + filename + ".ini\n"
                f1.write(Modifiedline)
            elif "export DATABASE=" in line:
                Modifiedline = "export DATABASE=\"" + database + "\"\n"
                f1.write(Modifiedline)
            elif "export TABLENAME=" in line:
                Modifiedline2 = "export TABLENAME=\"" + tablename +  "\"\n"
                f1.write(Modifiedline2)
            elif "export COLUNA=" in line:
                Modifiedline2 = "export COLUNA=\"" + columnsnames +  "\"\n"
                f1.write(Modifiedline2)
            else:
                f1.write(line)

    print "03 :   Created the " + nomearquivo_INI + " sucessfully."
    GenerateSH("03",nomearquivo_SH,DIR_PYGEN + "Job_Examples/HD003.sh")

def Stats(filename,database,tablename,opcao):
    nomearquivo_INI = DIR_INTGR_PAR + filename + ".ini"
    nomearquivo_SH = DIR_INTGR_JOB + filename + ".sh"
    with open(DIR_PYGEN + "Par_Examples/HD004.ini") as f, open(nomearquivo_INI, "w") as f1:
        for line in f:
            if "# SCRIPTNAMEGOESHERE" in line:
                Modifiedline = "# " + filename + ".ini\n"
                f1.write(Modifiedline)
            elif "export DATABASE=" in line:
                Modifiedline = "export DATABASE=\"" + database + "\"\n"
                f1.write(Modifiedline)
            elif "export TABLENAME=" in line:
                Modifiedline2 = "export TABLENAME=\"" + tablename +  "\"\n"
                f1.write(Modifiedline2)
            elif "export TP_CHAMADA=" in line:
                Modifiedline2 = "export TP_CHAMADA=\"" + opcao +  "\"\n"
                f1.write(Modifiedline2)
            else:
                f1.write(line)

    print "   Created the " + nomearquivo_INI + " sucessfully."
    GenerateSH("04",nomearquivo_SH,DIR_PYGEN + "Job_Examples/HD004.sh")
    FourStepIntegrated(filename)

def GenerateSH(stepNumber,nomearquivo_SH,moduleExample_sh):
    with open(moduleExample_sh) as f2, open(nomearquivo_SH,"w") as sh:
        for line in f2:
            sh.write(line)
    print "Created the " + nomearquivo_SH + " sucessfully."

def Refined_Trusted(filename,database,tablename,qtd_steps):
    qtd = 0
    if 1 == qtd_steps:
        OneStepIntegrated(filename)
    if 2 == qtd_steps:
        TwoStepIntegrated(filename)
    if 3 == qtd_steps:
        ThreeStepIntegrated(filename)
    if 4 == qtd_steps:
        FiveStepIntegrated(filename)
    if 5 == qtd_steps:
        FiveStepIntegrated(filename)
    if 6 == qtd_steps:
        SixStepIntegrated(filename)
    if 7 == qtd_steps:
        SevenStepIntegrated(filename)
    if 8 == qtd_steps:
        EightStepIntegrated(filename)
    if 9 == qtd_steps:
        NineStepIntegrated(filename)
    if qtd_steps > 9:
        maisde10steps(filename,qtd_steps)

    while (qtd < int(qtd_steps)):
        qtd = 1 + qtd
        nomearquivo_INI = DIR_INTGR_PAR + filename + "{0:02d}".format(qtd) + ".ini"
        nomearquivo_SH = DIR_INTGR_JOB + filename + "{0:02d}".format(qtd) + ".sh"
        with open(DIR_PYGEN + "Par_Examples/HD007.ini") as f, open(nomearquivo_INI, "w") as f1:
            for line in f:
                if "# SCRIPTNAMEGOESHERE" in line:
                    Modifiedline = "# " + filename + ".ini\n"
                    f1.write(Modifiedline)
                elif "export DATABASE=" in line:
                    Modifiedline = "export DATABASE=\"" + database + "\"\n"
                    f1.write(Modifiedline)
                elif "export TABLENAME=" in line:
                    Modifiedline2 = "export TABLENAME=\"" + tablename +  "\"\n"
                    f1.write(Modifiedline2)
                else:
                    f1.write(line)
            GenerateSH("04",nomearquivo_SH,DIR_PYGEN + "Job_Examples/HD007.sh")
        if qtd == qtd_steps:
            break

def OneStepIntegrated(filename):
    fname = filename[:8]
    filenamecompled = DIR_INTGR_JOB + fname + ".sh"
    with open(DIR_PYGEN + "template/Template_1Job.sh") as f,  open(filenamecompled, "w") as ag:
        for line in f:
            if "# SCRIPTNAMEGOESHERE" in line:
                Modifiedline = "# " + fname + ".ini\n"
                ag.write(Modifiedline)
            elif "/step01" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "01" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            else:
                ag.write(line)
    print "Created the directory " + DIR_INTGR_JOB + fname + ".sh"

def TwoStepIntegrated(filename):
    fname = filename[:8]
    filenamecompled = DIR_INTGR_JOB + fname + ".sh"
    with open(DIR_PYGEN + "template/Template_2Job.sh") as f, open(filenamecompled, "w") as ag:
        for line in f:
            if "# SCRIPTNAMEGOESHERE" in line:
                Modifiedline = "# " + fname + ".ini\n"
                ag.write(Modifiedline)
            elif "/step01" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "01" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step02" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "02"  + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            else:
                ag.write(line)
    print "Created the directory " + DIR_INTGR_JOB + fname + ".sh"

def ThreeStepIntegrated(filename):
    fname = filename[:8]
    filenamecompled = DIR_INTGR_JOB + fname + ".sh"
    with open(DIR_PYGEN + "template/Template_3Job.sh") as f, open(filenamecompled, "w") as ag:
        for line in f:
            if "# SCRIPTNAMEGOESHERE" in line:
                Modifiedline = "# " + fname + ".ini\n"
                ag.write(Modifiedline)
            elif "/step01" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "01" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step02" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "02"  + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step03" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "03" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            else:
                ag.write(line)
    print "Created the directory " + DIR_INTGR_JOB + fname + ".sh"

def FourStepIntegrated(filename):
    fname = filename[:8]
    filenamecompled = DIR_INTGR_JOB + fname + ".sh"
    with open(DIR_PYGEN + "template/Template_4Job.sh") as f, open(filenamecompled, "w") as ag:
        for line in f:
            if "# SCRIPTNAMEGOESHERE" in line:
                Modifiedline = "# " + fname + ".ini\n"
                ag.write(Modifiedline)
            elif "/step01" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "01" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step02" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "02"  + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step03" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "03" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step04" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "04" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            else:
                ag.write(line)
    print "Created the directory " + DIR_INTGR_JOB + fname + ".sh"

def FiveStepIntegrated(filename):
    fname = filename[:8]
    filenamecompled = DIR_INTGR_JOB + fname + ".sh"
    with open(DIR_PYGEN + "template/Template_5Job.sh") as f, open(filenamecompled, "w") as ag:
        for line in f:
            if "# SCRIPTNAMEGOESHERE" in line:
                Modifiedline = "# " + fname + ".ini\n"
                ag.write(Modifiedline)
            elif "/step01" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "01" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step02" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "02"  + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step03" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "03" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step04" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "04" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step05" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "05" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            else:
                ag.write(line)
    print "Created the directory " + DIR_INTGR_JOB + fname + ".sh"

def SixStepIntegrated(filename):
    fname = filename[:8]
    filenamecompled = DIR_INTGR_JOB + fname + ".sh"
    with open(DIR_PYGEN + "template/Template_6Job.sh") as f, open(filenamecompled, "w") as ag:
        for line in f:
            if "# SCRIPTNAMEGOESHERE" in line:
                Modifiedline = "# " + fname + ".ini\n"
                ag.write(Modifiedline)
            elif "/step01" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "01" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step02" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "02"  + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step03" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "03" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step04" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "04" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step05" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "05" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step06" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "06" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            else:
                ag.write(line)
    print "Created the directory " + DIR_INTGR_JOB + fname + ".sh"

def SevenStepIntegrated(filename):
    fname = filename[:8]
    filenamecompled = DIR_INTGR_JOB + fname + ".sh"
    with open(DIR_PYGEN + "template/Template_7Job.sh") as f, open(filenamecompled, "w") as ag:
        for line in f:
            if "# SCRIPTNAMEGOESHERE" in line:
                Modifiedline = "# " + fname + ".ini\n"
                ag.write(Modifiedline)
            elif "/step01" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "01" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step02" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "02"  + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step03" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "03" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step04" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "04" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step05" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "05" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step06" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "06" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step07" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "07" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            else:
                ag.write(line)
    print "Created the directory " + DIR_INTGR_JOB + fname + ".sh"

def EightStepIntegrated(filename):
    fname = filename[:8]
    filenamecompled = DIR_INTGR_JOB + fname + ".sh"
    with open(DIR_PYGEN + "template/Template_8Job.sh") as f, open(filenamecompled, "w") as ag:
        for line in f:
            if "# SCRIPTNAMEGOESHERE" in line:
                Modifiedline = "# " + fname + ".ini\n"
                ag.write(Modifiedline)
            elif "/step01" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "01" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step02" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "02"  + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step03" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "03" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step04" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "04" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step05" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "05" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step06" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "06" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step07" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "07" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step08" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "08" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            else:
                ag.write(line)
    print "Created the directory " + DIR_INTGR_JOB + fname + ".sh"

def NineStepIntegrated(filename):
    fname = filename[:8]
    filenamecompled = DIR_INTGR_JOB + fname + ".sh"
    with open(DIR_PYGEN + "template/Template_9Job.sh") as f, open(filenamecompled, "w") as ag:
        for line in f:
            if "# SCRIPTNAMEGOESHERE" in line:
                Modifiedline = "# " + fname + ".ini\n"
                ag.write(Modifiedline)
            elif "/step01" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "01" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step02" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "02"  + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step03" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "03" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step04" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "04" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step05" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "05" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step06" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "06" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step07" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "07" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step08" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "08" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            elif "/step09" in line:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + fname + "09" + ".sh ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
            else:
                ag.write(line)
    print "Created the directory " + DIR_INTGR_JOB + fname + ".sh"

def maisde10steps(filename,qtd_steps):
    fname = filename[:8]
    filenamecompled = DIR_INTGR_JOB + fname + ".sh"
    steps, step_n, flg_replace_steps = range(1,qtd_steps+1),0,True
    
    if not os.path.exists(DIR_PYGEN + "template/Template_{0}Job.sh".format(qtd_steps)):
        with open(DIR_PYGEN + "template/Template_{0:02d}Job.sh".format(qtd_steps),"w+") as f:
            f.write("#\n")
            f.write("# SCRIPTNAMEGOESHERE\n")
            f.write("#\n")
            f.write("# AUTOR........: Fabricio Quiles - fabricio.quiles@keyrus.com.br\n")
            f.write("# DATA.........: 18/01/2018\n")
            f.write("# VERSAO.......: 1.0\n")
            f.write("# COMENTARIO...: Criacao de shell\n")
            f.write("#\n")
            f.write("# OBJETIVO: Job para execucao com STEP\n")
            f.write("#\n")
            f.write("# OBSERVACOES: Parametros de chamada:\n")
            f.write("#\n")
            f.write("# 1 - Movto do tws formato YYYYMMDD\n")
            f.write("# 2 - Sistema (verificar se pode setar no proprio TWS)\n")
            f.write("# 3 - Diretorio Job (verificar se pode setar no proprio TWS)\n")
            f.write("# 4 - Diretorio arquivo configuracao global (verificar se pode setar no proprio TWS)\n")
            f.write("# 5 - Step a ser executado (opcional)\n")
            f.write("###################################################################################################################################\n")
            f.write("\n")
            f.write("echo Inicio do processamento - `date`\n")
            f.write("echo --------------------------------------------------------------------------\n")
            f.write("\n")
            f.write("export MOVTO=$1\n")
            f.write("export HOME_BID_COMMON_PAR=$2\n")
            f.write("export JOBNAME=$(basename $0 .sh)\n")
            f.write("export step=$3\n")
            f.write("source ${HOME_BID_COMMON_PAR}/config_global.ini\n")
            f.write("source ${HOME_BID_COMMON_FUNCTION}/function.cfg\n")
            f.write("source ${HOME_BID_COMMON_PAR}/config_local.ini\n")
            f.write("\n")
            f.write("bn=$0\n")
            f.write("\n")
            f.write("if [ \"$step\" == \"\" ]; then\n")
            f.write("\n")
            f.write("  echo \"Iniciando script $bn - MOVTO $MOVTO a partir do inicio\"\n")
            f.write("\n")
            f.write("else\n")
            f.write("\n")
            f.write("  echo \"Iniciando script $bn - MOVTO $MOVTO a partir do step $step\"\n")
            f.write("\n")
            f.write("fi\n")
            f.write("\n")
            f.write("case $step in\n")
            f.write("\n")
            f.write("\"\")\n")
            f.write("\n")
            f.write("  echo Inicio do script $bn;&\n")
            f.write("\n")
            f.write("\"step00\")\n")
            f.write("\n")
            f.write("  echo Inicio do step step00  - `date`\n")
            f.write("  echo Testando se existe arquivo de controle ${JOBNAME}.ok ....\n")
            f.write("\n")
            f.write("  if [ -f \"${HOME_INTEGRATION_LAYER_JOB}/${JOBNAME}.ok\" ]; then\n")
            f.write("\n")
            f.write("    echo Arquivo ${JOBNAME}.ok encontrado em ${HOME_INTEGRATION_LAYER_JOB}.\n")
            f.write("    echo Removendo arquivo de controle...\n")
            f.write("\n")
            f.write("    rm ${HOME_INTEGRATION_LAYER_JOB}/${JOBNAME}.ok\n")
            f.write("    status=$?\n")
            f.write("\n")
            f.write("    if [ ${status} -ne 0 ]; then\n")
            f.write("\n")
            f.write("      echo Final do step step00 com erro - `date`\n")
            f.write("      echo Falha ao tentar remover o arquivo de controle ${JOBNAME}.ok de ${HOME_INTEGRATION_LAYER_JOB}\n")
            f.write("      echo --------------------------------------------------------------------------\n")
            f.write("      exit 10\n")
            f.write("\n")
            f.write("    else\n")
            f.write("\n")
            f.write("      echo Final do step step00 com sucesso - `date`\n")
            f.write("      echo --------------------------------------------------------------------------\n")
            f.write("\n")
            f.write("    fi\n")
            f.write("\n")
            f.write("  else\n")
            f.write("\n")
            f.write("    echo Final do step step00 com erro - `date`\n")
            f.write("    echo Nao foi encontrado o arquivo de controle  ${JOBNAME}.ok em ${HOME_INTEGRATION_LAYER_JOB}\n")
            f.write("    echo Isto pode indicar que a execucao anterior nao finalizou com sucesso\n")
            f.write("    echo --------------------------------------------------------------------------\n")
            f.write("    exit 10\n")
            f.write("\n")
            f.write("  fi\n")
            f.write("\n")
            f.write("  ;&\n")

            for i in steps:
                f.write("\n")
                f.write("\"step{0:02d}\")\n".format(i))
                f.write("\n")
                f.write("  echo Inicio do step step{0:02d} - `date`\n".format(i))
                f.write("\n")
                f.write("  # colocar o nome correto do job. Ex.: BI03A0101.txt\n")
                f.write("  ${HOME_INTEGRATION_LAYER_JOB}/step" + "{0:02d}\n".format(i))
                f.write("  status=$?\n")
                f.write("\n")
                f.write("  if [ ${status} -ne 0 ]; then\n")
                f.write("\n")
                f.write("    echo Final do step step{0:02d} com erro - `date`\n".format(i))
                f.write("    echo Falha ao tentar executar o job ${HOME_INTEGRATION_LAYER_JOB}/teste_shell_pcp.txt\n")
                f.write("    echo --------------------------------------------------------------------------\n")
                f.write("    exit 10\n")
                f.write("\n")
                f.write("  else\n")
                f.write("\n")
                f.write("    echo Final do step step{0:02d} com sucesso - `date`\n".format(i))
                f.write("    echo --------------------------------------------------------------------------\n")
                f.write("\n")
                f.write("  fi\n")
                f.write("\n")
                f.write("  ;&\n")

            f.write("\n")
            f.write("\"step99\")\n")
            f.write("\n")
            f.write("  echo Inicio do step step99 - `date`\n")
            f.write("  touch ${HOME_INTEGRATION_LAYER_JOB}/${JOBNAME}.ok\n")
            f.write("  status=$?\n")
            f.write("\n")
            f.write("  if [ ${status} -ne 0 ]; then\n")
            f.write("\n")
            f.write("    echo Final do step step99 com erro - `date`\n")
            f.write("    echo --------------------------------------------------------------------------\n")
            f.write("    exit 10\n")
            f.write("\n")
            f.write("  else\n")
            f.write("\n")
            f.write("    echo Final do step step99 com sucesso - `date`\n")
            f.write("    echo --------------------------------------------------------------------------\n")
            f.write("\n")
            f.write("  fi\n")
            f.write("  ;;\n")
            f.write("\n")
            f.write("*)\n")
            f.write("\n")
            f.write("  echo ------ step $step nao existe --------\n")
            f.write("  exit 10\n")
            f.write("  ;;\n")
            f.write("\n")
            f.write("esac\n")

    with open(DIR_PYGEN + "template/Template_{0}Job.sh".format(qtd_steps)) as f, open(filenamecompled, "w") as ag:
        for line in f:
            if "# SCRIPTNAMEGOESHERE" in line:
                Modifiedline = "# " + fname + ".ini\n"
                ag.write(Modifiedline)
            elif "/step{0:02d}".format(steps[step_n]) in line and flg_replace_steps:
                Modifiedline = "  ${HOME_INTEGRATION_LAYER_JOB}/" + "{0}{1:02d}.sh".format(fname,steps[step_n]) + " ${MOVTO} ${HOME_BID_COMMON_PAR}\n"
                ag.write(Modifiedline)
                if step_n+1 == qtd_steps:
                    flg_replace_steps = False
                else:
                    step_n += 1
            else:
                ag.write(line)

    print "Created file" + DIR_INTGR_JOB + fname + ".sh"

def main():
    print ' '
    print '---------------------------------- Inicio Do Programa ----------------------------------'
    print ' '

    with open(DIR_PYGEN + "Step01.csv") as myFile:
        reader = csv.reader(myFile)
        meuArquivo = list(reader)
        for x in meuArquivo:
            createScript_Yes_or_No = str(x[0])
            step = str(x[1])
            if createScript_Yes_or_No == 'yes':
                if '1' in step:
                    TransferirArquivos(str(x[2]),str(x[3]),str(x[4]))
                if '2' in step:
                    CargaRawZone(str(x[2]),str(x[3]),str(x[4]))
                if '3' in step:
                    Profile(str(x[2]),str(x[3]),str(x[4]),str(x[5]))
                if '4' in step:
                    Stats(str(x[2]),str(x[3]),str(x[4]),str(x[5]))
                if '5' in step:
                    Refined_Trusted(str(x[2]),str(x[3]),str(x[4]),int(x[5]))
                if '6' in step:
                    Sqoop(str(x[2]),str(x[3]),str(x[4]),str(x[5]),str(x[6]),str(x[7]))

        print ' '
        print '---------------------------------- Fim Do Programa ----------------------------------'
main()
