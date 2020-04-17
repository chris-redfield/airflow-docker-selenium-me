# -*- coding: utf-8 -*-
CREDENCIAMENTO_INDICADORES = """INSERT INTO seges_cgial_fornecedor.[dbo].[FORNECEDOR_INDICADORES] 
(
    ID_FORNECEDOR,
    ORDEM,
    ATRIBUTO,
    VALOR,
    OBSERVACAO,
    ICONE,
    [STATUS]
) 
select F.ID_FORNECEDOR,
    1 AS ORDEM,
    'Situação no SICAF: ' AS ATRIBUTO,
       CASE WHEN CAST([DT_VENCIMENTO_CREDENCIAMENTO] AS DATE) > GETDATE() THEN 'Credenciado' ELSE 'Não credenciado' END AS VALOR,
    'Situação do credenciamento no SICAF' as OBSERVACAO,
    CASE WHEN CAST([DT_VENCIMENTO_CREDENCIAMENTO] AS DATE) > GETDATE() THEN 1 ELSE 0 END AS ICONE,
    1 as STATUS
from seges_cgial_fornecedor.[dbo].FORNECEDOR_COMPLEMENTO FC 
INNER JOIN seges_cgial_fornecedor.[dbo].FORNECEDOR F ON F.ID_PESSOA = FC.ID_PESSOA;
"""




PENALIDADES_VIGENTES = """INSERT INTO seges_cgial_fornecedor.[dbo].[FORNECEDOR_INDICADORES] 
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
FROM [dbo].[FORNECEDOR_PENALIDADE] P
WHERE P.DT_FINAL >= GETDATE()
GROUP BY P.ID_FORNECEDOR;
"""


VINCULO_SERVIDOR_SOCIO = """INSERT INTO [dbo].[FORNECEDOR_INDICADORES] 
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
       'Sócio com vínculo com servidor público: ' as ATRIBUTO,
	   CASE WHEN t.DESCRICAO is null THEN 'Não' ELSE 'Sim' END as VALOR,
	   CASE WHEN t.DESCRICAO is null THEN 'Sócio sem vínculo com servidor público.' ELSE t.DESCRICAO END as DESCRICAO,
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
			FROM SERVIDOR S 
			inner join FORNECEDOR_SOCIO FS ON S.ID_SERVIDOR = FS.SOCIO
			WHERE FS.ID_FORNECEDOR = SO.ID_FORNECEDOR
			FOR XML PATH('')), 1, 2, '')) as DESCRICAO
FROM  FORNECEDOR_SOCIO SO 
) t;
"""

FORNECEDOR_PENALIDADE_FORN = """INSERT INTO  [dbo].[FORNECEDOR_PENALIDADE_FORN]
(
    ID_FORNECEDOR,
    QTD_VIGENTE,
    QTD_VENCIDA
)
SELECT F.ID_FORNECEDOR,
       isnull(QTD_VIGENTE,0) AS QTD_VIGENTE,
       isnull(QTD_VENCIDAS,0) AS QTD_VENCIDA
FROM FORNECEDOR F
    LEFT JOIN 
        (SELECT 
        P.ID_FORNECEDOR,
        COUNT(P.ID_FORNECEDOR) AS QTD_VIGENTE
        FROM [dbo].[FORNECEDOR_PENALIDADE] P
        WHERE P.DT_FINAL >= GETDATE()
        GROUP BY P.ID_FORNECEDOR) V ON F.ID_FORNECEDOR = V.ID_FORNECEDOR
    LEFT JOIN 
        (SELECT 
        P.ID_FORNECEDOR,
        COUNT(P.ID_FORNECEDOR) AS QTD_VENCIDAS
        FROM [dbo].[FORNECEDOR_PENALIDADE] P
        WHERE P.DT_FINAL < GETDATE()
        GROUP BY P.ID_FORNECEDOR) A ON F.ID_FORNECEDOR = A.ID_FORNECEDOR;
"""

FORNECEDOR_PENALIDADE_SOCIO = """INSERT INTO  [dbo].[FORNECEDOR_PENALIDADE_SOCIO]
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
    FROM [dbo].[FORNECEDOR_PENALIDADE] P
    WHERE P.DT_FINAL >= GETDATE()
    GROUP BY P.ID_FORNECEDOR) V ON F.SOCIO = V.ID_FORNECEDOR
LEFT JOIN 
    (SELECT 
    P.ID_FORNECEDOR,
    COUNT(P.ID_FORNECEDOR) AS QTD_VENCIDAS
    FROM [dbo].[FORNECEDOR_PENALIDADE] P
    WHERE P.DT_FINAL < GETDATE()
GROUP BY P.ID_FORNECEDOR) A ON F.SOCIO = A.ID_FORNECEDOR
GROUP BY F.ID_FORNECEDOR;
"""