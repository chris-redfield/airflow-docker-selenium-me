DESCLASSIFICACAO_FORNECEDORES = """
SELECT p.prpCNPJ as ID_FORNECEDOR,
       2 AS ORDEM,
       'Desclassificações: ' as ATRIBUTO,
       count(DISTINCT i.sppCod)||' / '||count(DISTINCT i.ippCod) as VALOR,
       'Quantidade de desclassificações geradas para este fornecedor pela quantidade de participações nos itens de licitação. Tais desclassificações decorrem do momento em que o fornecedor obteve o menor preço na disputa mas foi desclassificado por algum motivo. Por exemplo: Documentos de habilitação ausentes ou inadequados ; Documentação técnica insuficiente ; Condição de participação como ME/EPP indevida ; entre outros. ' as OBSERVACAO,
       1 as STATUS
  FROM Comprasnet.Comprasnet_VBL.tbl_Proposta p
  inner join Comprasnet.Comprasnet_VBL.tbl_PropostaItem i on p.prpCod = i.prpCod
where p.prpCNPJ is not null and p.prpCNPJ <> ''
Group by p.prpCNPJ;
"""




# Esta consulta terá que ser ajustada, quando for implentado as tabelas faltante no datalake
# Hoje ela está no Quartzo (Postgres) e será migrada para o Datalake(SQL SERVER)
RECURSOS = """
Select e.CNPJ as ID_FORNECEDOR,       
       1 AS ORDEM,
       'Recursos Impetrados: ' as ATRIBUTO,
       coalesce(count(DISTINCT r.reCod),0) || ' / ' || sum(ITEM) as VALOR,
       'Quantidade de recursos impetrados pelo fornecedor pela quantidade de itens de licitação que foram disputador por ele. Busca-se identificar fornecedores que recorrentemente entram com recursos, pois podem estar prejudicando os trâmites normais do processo sem as motivações adequadas.' as OBSERVACAO,
       1 as STATUS
from 
(SELECT p.prpCNPJ as ID_FORNECEDOR,       
       count(DISTINCT i.ippCod) as ITEM
  FROM Comprasnet.Comprasnet_VBL.tbl_Proposta p
  inner join Comprasnet.Comprasnet_VBL.tbl_PropostaItem i on p.prpCod = i.prpCod
Group by p.prpCNPJ) p 
inner join Comprasnet.Comprasnet_VBL.tb_Empresa e on p.ID_FORNECEDOR = e.CNPJ
inner join Comprasnet.Comprasnet_VBL.tb_Usuario u  on e.Cod_Empresa = u.Cod_Empresa 
inner join Comprasnet.Comprasnet_VBL.tbl_Recurso r on r.cliente_id = u.Login
group by e.CNPJ;
"""