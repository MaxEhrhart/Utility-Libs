QTD_ARQUIVOS=$(ls ${HOME_BID_LANDZONE_FILES}/*${FILENAME}*${EXTENSION}  2>/dev/null | grep bid | wc -l)
if [ $QTD_ARQUIVOS -ne 0 ]; then
    for ARQUIVO in $(ls ${HOME_BID_LANDZONE_FILES}/*${FILENAME}*${EXTENSION}); do
        gawk -v RS='"' 'NR % 2 == 0 { gsub(/[\r\n]+/, " ") } { printf("%s%s", $0, RT) }' ${ARQUIVO} > ${ARQUIVO}.tmp
        rm ${ARQUIVO}
        mv ${ARQUIVO}.tmp ${ARQUIVO}
    done
fi
