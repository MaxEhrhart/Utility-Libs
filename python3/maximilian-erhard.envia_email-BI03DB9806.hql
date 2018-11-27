----------------------------------------------------------------------------------------------------------
--
-- BI03DB9806.hql
--
-- AUTOR........: KEYRUS
-- DATA.........: 2018/04/03
-- VERSAO.......: 1.0
-- COMENTARIO...: EXECUTAR NO IMPALA
-- OBJETIVO.....: INFORMAR AOS USUSARIOS AS ENTIDADES PRESTES A VENCER.
-- OBSERVACOES..: DATA MOVTO + 30
-----------------------------------------------------------------------------------------------------------
-- EXECUTAR NO IMPALA
-----------------------------------------------------------------------------------------------------------
SELECT

     e.cod_entid
    ,f.nom_fornec as Entidade
    ,e.cod_lj
    ,l.nom_local as Loja
    ,CONCAT(
        SUBSTR(CAST(e.dat_valid AS STRING),7,2),"/",
        SUBSTR(CAST(e.dat_valid AS STRING),5,2),"/",
        SUBSTR(CAST(e.dat_valid AS STRING),1,4)
    ) as Data_Validade

FROM tszv_igpa.v_entidade e

LEFT join tszv_planej_coml.v_fornec f
  ON e.cod_fornec = f.cod_fornec
 AND f.dat_fim = 21001231

LEFT join tszv_planej_coml.v_local l
  ON e.cod_lj = l.cod_local
 AND l.dat_fim = 21001231

WHERE e.dat_fim_vig = 21001231
  AND e.dat_valid >= ${VAR:MOVTO}
  AND e.dat_valid <= CAST(from_timestamp(ADDDATE(from_unixtime(unix_timestamp(CAST(${VAR:MOVTO} AS STRING), 'yyyyMMdd')), 30), 'yyyyMMdd') AS INT)
;
