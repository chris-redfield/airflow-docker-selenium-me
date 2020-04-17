# -*- coding: utf-8 -*-
CONTRATO_ATIVOS = """INSERT INTO [FORNECEDOR_HISTORICO_CONTRATO]
(
      ID_FORNECEDOR,
      ORDEM,
      ATRIBUTO,
      VALOR,
      OBSERVACAO,
      STATUS
)
select C.ID_FRND_FORNECEDOR_CONTRATO AS ID_FORNECEDOR,
       5 AS ORDEM,
       'Contratos ativos com o governo: ' as ATRIBUTO,
       COUNT(DISTINCT C.ID_CNTR_CONTRATO) AS VALOR,
       'Contratos ativos no SIASG.' as OBSERVACAO,
       1 as STATUS
  from FORNECEDOR_CONTRATO C
WHERE C.DT_CNTR_FIM_VIGENCIA >= GETDATE()
GROUP BY C.ID_FRND_FORNECEDOR_CONTRATO;
"""

CONTRATO_VENCIDOS = """INSERT INTO [FORNECEDOR_HISTORICO_CONTRATO]
(
      ID_FORNECEDOR,
      ORDEM,
      ATRIBUTO,
      VALOR,
      OBSERVACAO,
      STATUS
)
select C.ID_FRND_FORNECEDOR_CONTRATO AS ID_FORNECEDOR,
       6 AS ORDEM,
       'Contratos vencidos com o governo: ' as ATRIBUTO,
         COUNT(DISTINCT C.ID_CNTR_CONTRATO) AS VALOR,
        'Contratos vencidos no SIASG.' as OBSERVACAO,
          1 as STATUS
  from FORNECEDOR_CONTRATO C
WHERE C.DT_CNTR_FIM_VIGENCIA < GETDATE()
GROUP BY C.ID_FRND_FORNECEDOR_CONTRATO;
"""


LICITACOES = """
INSERT INTO [FORNECEDOR_HISTORICO]
(
      ID_FORNECEDOR,
      ORDEM,
      ATRIBUTO,
      VALOR,
      OBSERVACAO,
      STATUS
)
SELECT ID_FORNECEDOR,
	   case 
		  when ATRIBUTO = 'Licitações disputadas: ' then 1
		  when ATRIBUTO = 'Licitações vencidas: ' then 2
		  when ATRIBUTO = 'Itens de licitações disputadas: ' then 3
		  when ATRIBUTO = 'Itens de licitações vencidas: ' then 4  
	   END as ORDEM,
	   ATRIBUTO,
	   VALOR,
	   'Dados referente ao sistema do Comprasnet.' as OBSERVACAO,
	   1 as STATUS
  FROM (SELECT ID_FORNECEDOR 
			  ,[QTD_COMPRA] as [Licitações disputadas: ]
			  ,[QTD_COMPRA_HOMOLOG] as [Licitações vencidas: ]
              ,[QTD_ITEM_COMPRA] as [Itens de licitações disputadas: ]
			  ,[QTD_ITEM_COMPRA_HOMOLOG] as [Itens de licitações vencidas: ]	
         FROM [seges_cgial_fornecedor].[dbo].[FORNECEDOR_SUMARIO_COMPRA]) T
UNPIVOT
(
	  VALOR
	  FOR ATRIBUTO IN (
			   [Licitações disputadas: ]
			  ,[Licitações vencidas: ]
              ,[Itens de licitações disputadas: ]
			  ,[Itens de licitações vencidas: ])
) AS T;
"""
