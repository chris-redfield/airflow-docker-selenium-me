ATA_VIGENTES = """
SELECT f.identificacao as ID_FORNECEDOR,
     7 AS ORDEM,     			
	'Atas de registro de preço vigente: ' as ATRIBUTO, 
	count(i.codigolicitacaosrp) as VALOR,
	'Quantidade de Atas de Registro de Preços vigentes que possuem esse fornecedor como provedor. Este dado possui como fonte o módulo de Gestão de Atas do COMPRASNET. Tal informação é automaticamente gerada e controlada a partir das informações de cadastro das compras no Sistema de Registro de Preços. ' as OBSERVACAO,
1 as STATUS
FROM Siasgnet.Siasgnet_atasrp_VBL.fornecedor f 
 left join Siasgnet.Siasgnet_atasrp_VBL.itematasrp i on i.codigoitematasrp = f.codigoitematasrp
Where i.datafimvigenciaata >= now()
group by f.identificacao;
"""


ATA_VENCIDAS = """
SELECT f.identificacao as ID_FORNECEDOR,
     8 AS ORDEM,     			
	'Atas de registro de preços vencidas: ' as ATRIBUTO, 
	count(i.codigolicitacaosrp) as VALOR,
	'Quantidade de Atas de Registro de Preços vencidas que possuíam esse fornecedor como provedor e que constam no módulo de Gestão de Atas do COMPRASNET. Tal informação é automaticamente gerada e controlada a partir das informações de cadastro das compras no Sistema de Registro de Preços.' as OBSERVACAO,
1 as STATUS
FROM Siasgnet.Siasgnet_atasrp_VBL.fornecedor f 
left join Siasgnet.Siasgnet_atasrp_VBL.itematasrp i 
on i.codigoitematasrp = f.codigoitematasrp
Where i.datafimvigenciaata < now()
group by f.identificacao;
"""