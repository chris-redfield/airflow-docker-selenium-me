CREATE TABLE seges_cgial_fornecedor.dbo.PESSOA (
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