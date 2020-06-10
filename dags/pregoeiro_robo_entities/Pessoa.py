DROP_TABLE = """
DROP TABLE IF EXISTS _PESSOA;
"""

_PESSOA =  """
CREATE TABLE seges_cgial_fornecedor.dbo._PESSOA (
id_pessoa varchar(19) COLLATE Latin1_General_CI_AS NULL, 
nom_nome varchar(150) COLLATE Latin1_General_CI_AS NULL,
num_ddd varchar(2) NULL,
num_telefone varchar(12) NULL,
num_ddd_alternativo varchar(2) NULL,
dsc_email varchar(115)  COLLATE Latin1_General_CI_AS NULL,
tp_pessoa int NULL,
id_endereco varchar(19)  COLLATE Latin1_General_CI_AS NULL,
dt_sincronizacao_receita datetime NULL,
num_participacao_acionaria decimal NULL,
data_hora_desvinculacao datetime NULL, 
tp_vinculo_fornecedor varchar(19) NULL,
id_fornecedor_proprietario varchar(19) COLLATE Latin1_General_CI_AS NULL,
num_cpf_cnpj varchar(50) COLLATE Latin1_General_CI_AS NULL

);
"""

PESSOA = """select DISTINCT CAST((case when p.tp_pessoa = 1 then LPAD(p.num_cpf_cnpj, 11,'0') when p.tp_pessoa = 2 then LPAD(p.num_cpf_cnpj, 14,'0') end) As VARCHAR(50)) as num_cpf_cnpj
, CAST(id_pessoa As VARCHAR(50)) as id_pessoa
, nom_nome
, num_ddd
, num_telefone
, num_ddd_alternativo
, dsc_email, tp_pessoa
, CAST(id_endereco As VARCHAR(50)) as id_endereco
, dt_sincronizacao_receita
, num_participacao_acionaria
, data_hora_desvinculacao
, tp_vinculo_fornecedor
, CAST(id_fornecedor_proprietario As VARCHAR(50)) as id_fornecedor_proprietario
 from pessoa p;
"""