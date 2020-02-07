CREATE TABLE dbo.Dados_CEIS(
	TIPO_PESSOA varchar(20) ,
	CPF_CNPJ_SANCIONADO varchar(14) ,
	NOME varchar(200) ,
	RAZAO_SOCIAL varchar(150) ,
	NOME_FANTASIA varchar(150) ,
	NUMERO_PROCESSO varchar(50) ,
	TIPO_SANCAO varchar(100) ,
	DATA_INICIO_SANCAO varchar(10) ,
	DATA_FINAL_SANCAO varchar(10) ,
	ORGAO_SANCIONADOR varchar(300) ,
	UF_ORGAO_SANCIONADOR varchar(5) ,
	ORIGEM_INFORMACOES varchar(500) ,
	DATA_ORIGEM_INFORMACOES varchar(10) ,
	DATA_PUBLICACAO varchar(10) ,
	PUBLICACAO varchar(100) ,
	DETALHAMENTO varchar(2500) ,
	ABRAGENCIA_JUDICIAL varchar(500) ,
	FUNDAMENTACAO_LEGAL varchar(100) ,
	DESCRICAO_FUND_LEGAL varchar(2500) ,
	DATA_TRANSITO_JULGADO varchar(10) ,
	COMPLEMENTO_ORGAO varchar(2500) ,
	OBSERVACOES varchar(2500) 
);

CREATE INDEX IDX_DADOS_CEIS ON dbo.Dados_CEIS (CPF_CNPJ_SANCIONADO);
