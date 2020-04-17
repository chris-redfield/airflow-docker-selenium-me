CREDENCIAMENTO_INDICADORES = """INSERT INTO seges_cgial_fornecedor.[dbo].[_FORNECEDOR_INDICADORES] 
(
    ID_FORNECEDOR,
    ORDEM,
    ATRIBUTO,
    VALOR,
    OBSERVACAO,
    ICONE,
    [STATUS]
) 
select DISTINCT F.ID_FORNECEDOR,
    1 AS ORDEM,
    'SITUAÇÃO DO CREDENCIAMENTO NO SICAF: ' AS ATRIBUTO,
       CASE WHEN CAST([DT_VENCIMENTO_CREDENCIAMENTO] AS DATE) > GETDATE() THEN 'Credenciado' ELSE 'Não credenciado' END AS VALOR,
    'Informa se o credenciamento do fornecedor junto ao SICAF está em situação regular (verde) ou não (vermelho).' as OBSERVACAO,
    CASE WHEN CAST([DT_VENCIMENTO_CREDENCIAMENTO] AS DATE) > GETDATE() THEN 1 ELSE 0 END AS ICONE,
    1 as STATUS
from seges_cgial_fornecedor.[dbo]._FORNECEDOR_COMPLEMENTO FC 
INNER JOIN seges_cgial_fornecedor.[dbo]._FORNECEDOR F ON F.ID_PESSOA = FC.ID_PESSOA;
"""




PENALIDADES_VIGENTES = """INSERT INTO seges_cgial_fornecedor.[dbo].[_FORNECEDOR_INDICADORES] 
(
    ID_FORNECEDOR,
    ORDEM,
    ATRIBUTO,
    VALOR,
    OBSERVACAO,
    ICONE,
    [STATUS]
) 
SELECT 
    P.ID_FORNECEDOR,
    3 AS ORDEM,
    'Penalidades vigentes: ' as ATRIBUTO,
    COUNT(P.ID_FORNECEDOR) AS VALOR,
    'Penalidades vigentes do fornecedor.' as OBSERVACAO,
     case when COUNT(P.ID_FORNECEDOR) > 0 then 0 else 1 end as ICONE,
     1 as STATUS
FROM [dbo].[_FORNECEDOR_PENALIDADE] P
WHERE P.DT_FINAL >= GETDATE()
GROUP BY P.ID_FORNECEDOR;
"""


VINCULO_SERVIDOR_SOCIO = """INSERT INTO [dbo].[_FORNECEDOR_INDICADORES] 
(
ID_FORNECEDOR,
ORDEM,
ATRIBUTO,
VALOR,
OBSERVACAO,
ICONE,
[STATUS]
) 
Select DISTINCT t.[ID_FORNECEDOR],
		'4' AS ORDEM,
       'SÓCIO(S) SEM VÍNCULO COM SERVIDOR(ES) PÚBLICO(S): ' as ATRIBUTO,
	   CASE WHEN t.DESCRICAO is null THEN 'Não' ELSE 'Sim' END as VALOR,
	   'Informa se algum sócio da empresa possui vínculo com servidores públicos. Utiliza como fonte a base do SIGEPE (SIstema de Gestão de Pessoas do Governo Federal).' as DESCRICAO,
	   CASE WHEN t.DESCRICAO is null THEN 1 ELSE 0 END as ICONE,
	   1 as STATUS
from 
(
Select DISTINCT SO.[ID_FORNECEDOR],		
	  (STUFF((SELECT 	
				CAST(', ' + 
				(
				CASE WHEN S.ID_SERVIDOR <> '' THEN CONCAT('Servidor com CPF: ',
						S.ID_SERVIDOR,
						' - Nome: ', 
						S.NO_SERVIDOR, 
						' - Órgão: ',
						S.[LOTACAO]) END
				)  AS VARCHAR(MAX))
			FROM _SERVIDOR S 
			inner join _FORNECEDOR_SOCIO FS ON S.ID_SERVIDOR = FS.SOCIO
			WHERE FS.ID_FORNECEDOR = SO.ID_FORNECEDOR
			FOR XML PATH('')), 1, 2, '')) as DESCRICAO
FROM  FORNECEDOR_SOCIO SO 
) t;
"""

FORNECEDOR_PENALIDADE_FORN = """INSERT INTO  [dbo].[_FORNECEDOR_PENALIDADE_FORN]
(
    ID_FORNECEDOR,
    QTD_VIGENTE,
    QTD_VENCIDA
)
SELECT F.ID_FORNECEDOR,
       sum(isnull(QTD_VIGENTE,0)) AS QTD_VIGENTE,
       sum(isnull(QTD_VENCIDAS,0)) AS QTD_VENCIDA
FROM _FORNECEDOR F
    LEFT JOIN 
        (SELECT 
        P.ID_FORNECEDOR,
        COUNT(P.ID_FORNECEDOR) AS QTD_VIGENTE
        FROM [dbo].[_FORNECEDOR_PENALIDADE] P
        WHERE P.DT_FINAL >= GETDATE()
        GROUP BY P.ID_FORNECEDOR) V ON F.ID_FORNECEDOR = V.ID_FORNECEDOR
    LEFT JOIN 
        (SELECT 
        P.ID_FORNECEDOR,
        COUNT(P.ID_FORNECEDOR) AS QTD_VENCIDAS
        FROM [dbo].[_FORNECEDOR_PENALIDADE] P
        WHERE P.DT_FINAL < GETDATE()
        GROUP BY P.ID_FORNECEDOR) A ON F.ID_FORNECEDOR = A.ID_FORNECEDOR
Group by F.ID_FORNECEDOR;
"""

FORNECEDOR_PENALIDADE_SOCIO = """INSERT INTO  [dbo].[_FORNECEDOR_PENALIDADE_SOCIO]
(
    ID_FORNECEDOR,
    QTD_VIGENTE,
    QTD_VENCIDA
)
SELECT F.ID_FORNECEDOR,
    SUM(isnull(QTD_VIGENTE,0)) AS QTD_VIGENTE,
    SUM(isnull(QTD_VENCIDAS,0)) AS QTD_VENCIDA
FROM FORNECEDOR_SOCIO F
LEFT JOIN 
    (SELECT 
    P.ID_FORNECEDOR,
    COUNT(P.ID_FORNECEDOR) AS QTD_VIGENTE
    FROM [dbo].[_FORNECEDOR_PENALIDADE] P
    WHERE P.DT_FINAL >= GETDATE()
    GROUP BY P.ID_FORNECEDOR) V ON F.SOCIO = V.ID_FORNECEDOR
LEFT JOIN 
    (SELECT 
    P.ID_FORNECEDOR,
    COUNT(P.ID_FORNECEDOR) AS QTD_VENCIDAS
    FROM [dbo].[_FORNECEDOR_PENALIDADE] P
    WHERE P.DT_FINAL < GETDATE()
GROUP BY P.ID_FORNECEDOR) A ON F.SOCIO = A.ID_FORNECEDOR
GROUP BY F.ID_FORNECEDOR;
"""

CONTRATO_ATIVOS = """INSERT INTO [_FORNECEDOR_HISTORICO_CONTRATO]
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
       'Quantidade de contratos vigentes registrados no Sistema de Contratos do Comprasnet. Tal informação é inserida diretamente pelos agentes de compras dos diversos órgãos, sejam SISG ou não-SISG. São contabilizados também os prazos de vigência atuais atualizados pelos aditivos e apostilamentos.' as OBSERVACAO,
       1 as STATUS
  from _FORNECEDOR_CONTRATO C
WHERE C.DT_CNTR_FIM_VIGENCIA >= GETDATE()
GROUP BY C.ID_FRND_FORNECEDOR_CONTRATO;
"""

CONTRATO_VENCIDOS = """INSERT INTO [_FORNECEDOR_HISTORICO_CONTRATO]
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
        'Quantidade de contratos que perderam sua vigência e que tiveram algum registro inicial de existência do contrato no Sistema de Contratos do Comprasnet.' as OBSERVACAO,
          1 as STATUS
  from _FORNECEDOR_CONTRATO C
WHERE C.DT_CNTR_FIM_VIGENCIA < GETDATE()
GROUP BY C.ID_FRND_FORNECEDOR_CONTRATO;
"""


LICITACOES = """
INSERT INTO [_FORNECEDOR_HISTORICO]
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
	   	   case 
		  when ATRIBUTO = 'Licitações disputadas: ' then 'Quantidade de licitações que o fornecedor teve alguma participação, sem considerar a quantidade de itens do certame.'
		  when ATRIBUTO = 'Licitações vencidas: ' then 'Quantidade de licitações que o fornecedor teve êxito e se sagrou como vencedor. <BR> Ex. Fornecedor participou muito e nunca ganhou. Pode representar um possível ator para influenciar as licitações.'
		  when ATRIBUTO = 'Itens de licitações disputadas: ' then 'Quantidade de itens de licitação que o fornecedor teve alguma participação. <BR> Caso haja 5 itens distintos em uma licitação, serão contabilizadas 5  participações para esse indicador e 1 para o indicador de "Licitações Disputadas".'
		  when ATRIBUTO = 'Itens de licitações vencidas: ' then 'Quantidade de itens de licitação que o fornecedor  teve êxito e se sagrou como vencedor. <BR>Caso haja 5 itens distintos em uma licitação em que o fornecedor foi vencedor, serão contabilizadas 5 participações para esse indicador e 1 para o indicador de "Licitações Disputadas". '  
	   END as OBSERVACAO,
	   1 as STATUS
  FROM (SELECT ID_FORNECEDOR 
			  ,[QTD_COMPRA] as [Licitações disputadas: ]
			  ,[QTD_COMPRA_HOMOLOG] as [Licitações vencidas: ]
              ,[QTD_ITEM_COMPRA] as [Itens de licitações disputadas: ]
			  ,[QTD_ITEM_COMPRA_HOMOLOG] as [Itens de licitações vencidas: ]	
         FROM [seges_cgial_fornecedor].[dbo].[_FORNECEDOR_SUMARIO_COMPRA]) T
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