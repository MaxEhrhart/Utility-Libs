#!/bin/sh
#set +x
#################################################################################
#                                                                               #
#  AUTOR........: MAXIMILIAN ERHARD (KEYRUS)                                    #
#  DATA.........: 02/04/2018                                                    #
#  VERSAO.......: 1.0                                                           #
#  OBJETIVO.....: ENVIO DE RESULTADOS DE CONSULTA POR EMAIL.                    #
#  COMENTARIOS..:                                                               #
#  OBSERVACOES..:                                                               #
#  *SE O NUMERO DE ENDEREÇOS NO REMETENTE OU NO DESTINARIO FOR MAIOR QUE UM,    #
#   SEPARAR ENDEREÇOS POR VIRGULA, SEM ESPAÇO ENTRE ELES.                       #
#                                                                               #
#  *ARQUIVOS EM ANEXOS DEVEM SER SEPARADOS POR VIRGULA SEM ESPAÇO ENTE ELES     #
#   CASO HAJA MAIS DE UM.                                                       #
#                                                                               #
#  *DEIXAR O REMETENTE VAZIO CASO DESEJE UTILIZAR O USUARIO PADRÃO E VER O      #
#   HISTORICO DE ENVIO DE MENSAGENS COM O COMANDO MAIL.                         #
#                                                                               #
#################################################################################
#
export          JOBNAME=$(basename $0 .sh)
export            MOVTO=$1
export   BID_COMMON_PAR=$2
#
##################################### IMPALA ####################################
#
source ${BID_COMMON_PAR}/config_global.ini
source ${BID_COMMON_PAR}/config_local.ini
source ${HOME_BID_COMMON_FUNCTION}/function.cfg
source ${HOME_INTEGRATION_LAYER_PAR}/${JOBNAME}.ini
#
##################################################################################
######################
# Inicio do Programa #
######################
iniLogExec $0
echoLog "--------------------------------------------------------------------------------"
echoLog " Exportando resultados da query no caminho ${ARQUIVO_SAIDA} ..."
echoLog "--------------------------------------------------------------------------------"

####### EXPORTA ARQUIVO
${IMPALA_CONNECTION} \
     -f ${HOME_INTEGRATION_LAYER_HQL}/${ARQUIVO_QUERY} \
    --print_header \
    --output_file ${ARQUIVO_SAIDA} \
    --delimited \
    --output_delimiter=${DELIMITADOR} \
    --var=MOVTO=${MOVTO}

RETORNO=$?
if [ ${RETORNO} -ne 0 ]; then
echoLog "--------------------------------------------------------------------------------"
echoLog "Erro de exportação do arquivo, Código de erro: ${RETORNO}"
echoLog "--------------------------------------------------------------------------------"
else
echoLog "--------------------------------------------------------------------------------"
echoLog "Arquivo gerado com sucesso!"
echoLog "Arquivo gerado no caminho ${ARQUIVO_SAIDA}"
echoLog "--------------------------------------------------------------------------------"

####### EMAIL
MENSAGEM=""
COMANDO="mailx -v -s \"${ASSUNTO}\""
#ANEXOS
if [ "${ANEXOS}" != "" ]; then
    IFS=","
    for ANEXO in ${ANEXOS}; do
        if [ $( cat ${ANEXO} | wc -l ) -gt 1 ]; then
            COMANDO="${COMANDO} -a $ANEXO"
            MENSAGEM=$(printf "%s\nAnexo: ${ANEXO##*/}" "${MENSAGEM_OK}")
        else
            MENSAGEM=$(printf "%s\n" "${MENSAGEM_NOK}")
        fi
    done
    IFS=" "
fi

#COM COPIA
if [ "$COM_COPIA_PARA" != "" ]; then
    COMANDO="${COMANDO} -c ${COM_COPIA_PARA}"
fi

#REMETENTE
if [ "$REMETENTE" != "" ]; then
    COMANDO="${COMANDO} -r ${REMETENTE}"
fi

#DESTINATARIO E MENSAGEM
COMANDO="${COMANDO} ${DESTINATARIO} <<< \"${MENSAGEM}\""
#echo ${COMANDO}
eval ${COMANDO}
RETORNO=$?
if [ $RETORNO -ne 0 ]; then
    echoLog "--------------------------------------------------------------------------------"
    echoLog "Erro durante envio de email, Código de erro: ${RETORNO}"
    echoLog "--------------------------------------------------------------------------------"
else
    echoLog "--------------------------------------------------------------------------------"
    echoLog "Mensagem enviada para os seguintes destinatários: "
    echoLog "Destinatários: ${DESTINATARIO}"
    if [ "$COM_COPIA_PARA" != "" ]; then
        echoLog "Com cópia para: ${COM_COPIA_PARA}"
    fi
    if [ "$REMETENTE" != "" ]; then
        echoLog "Remetente: ${REMETENTE}"
    else
        echoLog "Remetente: PADRÃO"
    fi
    if [ "$ANEXOS" != "" ]; then
        echoLog "Anexos: ${ANEXOS}"
    else
        echoLog "Anexos: Nenhum"
    fi
    echoLog "--------------------------------------------------------------------------------"
fi
fi
######################
# Fim do Programa    #
######################
fimLogExec $0 ${RETORNO}
exit ${RETORNO}
