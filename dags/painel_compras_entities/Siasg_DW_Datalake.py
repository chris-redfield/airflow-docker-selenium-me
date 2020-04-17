# -*- coding: utf-8 -*-
FORNECEDOR_CONTRATO = """SELECT ID_CNTR_CONTRATO,
                                ID_CMPR_COMPRA,
                                ID_FRND_FORNECEDOR_CONTRATO,       
                                ID_UNDD_UNID_PARTICIPANTE_CONTRATO,    
                                NO_FRND_FORNECEDOR_CONTRATO, 
                                NO_UNDD_UNID_PARTICIPANTE_CONTRATO, 
                                NO_MODALIDADE_COMPRA,      
                                VL_CNTR_INICIAL_CONTRATO,
                                VL_CNTR_FINAL_CONTRATO,
                                CH_CNTR_IDENT_CONTRATO_EDIT,
                                ID_CNTR_IN_EXEC_CONT,
                                DS_CNTR_OBJ_CONT,    
                                DT_CNTR_ASS_CONT,
                                DT_CNTR_INICIO_VIGENCIA,
                                DT_CNTR_FIM_VIGENCIA    
                            FROM (    
                            SELECT C.ID_CNTR_CONTRATO,
                                C.ID_CMPR_COMPRA,
                                C.ID_FRND_FORNECEDOR_COMPRA AS ID_FRND_FORNECEDOR_CONTRATO,       
                                C.ID_UNDD_UNID_PARTICIPANTE AS ID_UNDD_UNID_PARTICIPANTE_CONTRATO,    
                                F.NO_FRND_FORNECEDOR as NO_FRND_FORNECEDOR_CONTRATO, 
                                U.NO_UNDD_UNIDADE as NO_UNDD_UNID_PARTICIPANTE_CONTRATO,
                                CM.DS_CMPR_MODALIDADE_COMPRA AS NO_MODALIDADE_COMPRA,       
                                C.VL_CNTR_INICIAL_CONTRATO,
                                C.VL_CNTR_FINAL_CONTRATO,
                                D.CH_CNTR_IDENT_CONTRATO_EDIT,
                                D.ID_CNTR_IN_EXEC_CONT,
                                D.DS_CNTR_OBJ_CONT,    
                                D.DT_CNTR_ASS_CONT,
                                D.DT_CNTR_INI_VIG_CONT AS DT_CNTR_INICIO_VIGENCIA,
                                CASE 
                                    WHEN (T.DT_TMAD_FIM_VIGENCIA > D.DT_CNTR_FIM_VIG_CONT OR T.DT_TMAD_FIM_VIGENCIA > D.DT_CNTR_FIM_VIG_ULT_CONT) THEN T.DT_TMAD_FIM_VIGENCIA 
                                    WHEN (D.DT_CNTR_FIM_VIG_CONT > D.DT_CNTR_FIM_VIG_ULT_CONT) THEN D.DT_CNTR_FIM_VIG_CONT
                                    ELSE D.DT_CNTR_FIM_VIG_ULT_CONT END AS DT_CNTR_FIM_VIGENCIA
                            FROM F_CONTRATO C
                            INNER JOIN D_FRND_FORNECEDOR F ON F.ID_FRND_FORNECEDOR = C.ID_FRND_FORNECEDOR_COMPRA
                            INNER JOIN D_UNDD_UNIDADE U ON U.ID_UNDD_UNIDADE = C.ID_UNDD_UNID_PARTICIPANTE
                            INNER JOIN D_CNTR_CONTRATO D ON D.ID_CNTR_CONTRATO = C.ID_CNTR_CONTRATO
                            INNER JOIN D_CMPR_COMPRA CO ON CO.ID_CMPR_COMPRA = C.ID_CMPR_COMPRA
                            INNER JOIN D_CMPR_MODALIDADE_COMPRA CM ON CO.ID_CMPR_MODALIDADE_COMPRA = CM.ID_CMPR_MODALIDADE_COMPRA
                            LEFT  JOIN 
                            (
                            SELECT T.ID_CNTR_CONTRATO,
                                   MAX(TA.DT_TMAD_FIM_VIGENCIA) AS DT_TMAD_FIM_VIGENCIA
                            FROM F_TERMO_ADITIVO T
                            LEFT  JOIN D_TMAD_TERMO_ADITIVO TA ON TA.ID_TMAD_TERMO_ADITIVO = T.ID_TMAD_TERMO_ADITIVO
                            WHERE TA.ID_TMAD_TP_TERMO_ADITIVO IN (2,5,7,8)
                            GROUP BY T.ID_CNTR_CONTRATO
                            ) T ON T.ID_CNTR_CONTRATO = C.ID_CNTR_CONTRATO
                            ) T;
"""

FORNECEDOR_CONTRATO_ITEM = """
SELECT 
    I.ID_ITCT_ITEM_CONTRATO,    
    I.ID_CNTR_CONTRATO,
    I.ID_ITCP_ITEM_COMPRA,
    CH_ITCT_ITEM_CONTRATO_EDIT,
    DS_ITCP_ITEM_COMPRA as NO_ITCP_ITEM_CONTRATO,
    I.ID_ITCP_TP_COD_MAT_SERV,
    CASE WHEN TI.ID_ITCP_TP_MATERIAL_SERVICO = '1' THEN 'Material' WHEN TI.ID_ITCP_TP_MATERIAL_SERVICO = '2' THEN 'Serviço' else 'Não informado' end as NO_ITCP_TP_COD_MAT_SERV,
    I.QT_ITCT_ITENS_CONTRATO,
    I.QT_ITCT_CONTRATADA,
    I.VL_ITCT_CONTRATADO
FROM F_ITEM_CONTRATO I
INNER JOIN D_ITCT_ITEM_CONTRATO D ON D.ID_ITCT_ITEM_CONTRATO = I.ID_ITCT_ITEM_CONTRATO
INNER JOIN D_ITCP_ITEM_COMPRA C ON C.ID_ITCP_ITEM_COMPRA = I.ID_ITCP_ITEM_COMPRA
INNER JOIN D_ITCP_MATERIAL_SERVICO TI ON I.ID_ITCP_TP_COD_MAT_SERV = TI.ID_ITCP_TP_COD_MAT_SERV;
"""

FORNECEDOR_SUMARIO_COMPRA = """SELECT F.ID_FRND_FORNECEDOR_COMPRA as ID_FORNECEDOR,
            count(Distinct F.ID_CMPR_COMPRA) as QTD_COMPRA,
            count(Distinct F.ID_ITCP_ITEM_COMPRA) as QTD_ITEM_COMPRA,
            count(Distinct case when F.VL_PRECO_TOTAL_HOMOLOG > 0 then F.ID_CMPR_COMPRA end) as QTD_COMPRA_HOMOLOG,
            count(Distinct case when F.VL_PRECO_TOTAL_HOMOLOG > 0 then F.ID_ITCP_ITEM_COMPRA end) as QTD_ITEM_COMPRA_HOMOLOG,
            sum(F.VL_PRECO_TOTAL_HOMOLOG) as VALOR_HOMOLOGADO
            from F_ITEM_FORNECEDOR F
            group by F.ID_FRND_FORNECEDOR_COMPRA;
"""

CONTRATO_CONTINUADO = """
SELECT C.ID_FRND_FORNECEDOR_COMPRA as ID_FORNECEDOR,
	3 as ORDEM,
	'Contratos continuados: ' AS ATRIBUTO,
	CONCAT(SUM(QTD_TERMO),' / ',COUNT(C.ID_CNTR_CONTRATO)) AS VALOR,
	'Quantidade de Contratos continuados vigentes para este fornecedor pela quantidade total de contratos de qualquer espécie. Tais informações são obtidas do Sistema de Contratos do COMPRASNET, com dados inseridos diretamente pelos agentes de compras dos diversos órgãos, sejam SISG ou não-SISG.' AS OBSERVACAO,
	1 AS STATUS
FROM
(
   SELECT
   C.ID_FRND_FORNECEDOR_COMPRA,
   C.ID_CNTR_CONTRATO,
   CASE WHEN SUM(QTD_TERMO_ADITIVO) > 0 THEN 1 ELSE 0 END AS QTD_TERMO
   FROM
   (
      SELECT
      C.ID_FRND_FORNECEDOR_COMPRA,
      C.ID_CNTR_CONTRATO,
      sum
      (
         CASE WHEN T.ID_TMAD_TP_TERMO_ADITIVO IN (2,5,7,8) THEN 1 ELSE 0 END
      )
      AS QTD_TERMO_ADITIVO
      FROM F_CONTRATO C
      LEFT JOIN F_TERMO_ADITIVO F ON F.ID_CNTR_CONTRATO = C.ID_CNTR_CONTRATO
      LEFT JOIN D_TMAD_TERMO_ADITIVO T ON T.ID_TMAD_TERMO_ADITIVO = F.ID_TMAD_TERMO_ADITIVO
      AND T.ID_TMAD_TP_TERMO_ADITIVO IN (2,5,7,8)
      GROUP BY C.ID_FRND_FORNECEDOR_COMPRA, C.ID_CNTR_CONTRATO
   )
   C
   GROUP BY C.ID_FRND_FORNECEDOR_COMPRA, C.ID_CNTR_CONTRATO
)
C
GROUP BY C.ID_FRND_FORNECEDOR_COMPRA;
"""
