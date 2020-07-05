from datetime import datetime
id_code = str(datetime.now())
id_code = id_code.replace('-','').replace(':','').replace('.','').replace(' ','')

DROP_TABLE = """
DROP TABLE IF EXISTS _FORNECEDOR_CONTRATO_ITEM_TERMO_ADITIVO;
"""

CREATE_FORNECEDOR_CONTRATO_ITEM_TERMO_ADITIVO="""
CREATE TABLE 
seges_cgial_fornecedor.dbo._FORNECEDOR_CONTRATO_ITEM_TERMO_ADITIVO (
	CH_TMAD_TERMO_ADIT_EDIT varchar (100) COLLATE Latin1_General_CI_AS NOT NULL,
    ID_TMAD_TERMO_ADITIVO  float NOT NULL,
	NR_TMAD_IDENT_TERMO_ADIT  float NOT NULL,
    ID_CNTR_CONTRATO float NOT NULL,
	CH_CNTR_IDENT_CONTRATO_EDIT varchar (100) COLLATE Latin1_General_CI_AS NULL, 
	NR_CNTR_IDENT_CONTRATO float NOT NULL,
    ID_UNDD_ORGAO_SUP float NULL,
    DS_UNDD_ORGAO_SUP  varchar(300) COLLATE Latin1_General_CI_AS NULL,
    ID_UNDD_UNID_RESP_CONT float NULL,
    NOME_UNID_RESP_CONT varchar(300) COLLATE Latin1_General_CI_AS NULL,
    ESFERA varchar (100) COLLATE Latin1_General_CI_AS NULL,
    PODER varchar (100) COLLATE Latin1_General_CI_AS NULL,
    TIPO_ADMINISTRACAO varchar (100) COLLATE Latin1_General_CI_AS NULL,
	DS_LCAL_UF varchar (50) COLLATE Latin1_General_CI_AS NULL,
	ID_LCAL_MUNICIPIO_UNIDADE float NULL,
	DS_LCAL_MUNICIPIO varchar (100) COLLATE Latin1_General_CI_AS NULL,
	DS_CNTR_OBJ_CONT varchar(300) COLLATE Latin1_General_CI_AS NULL,
	DS_CNTR_SIT_ATUAL_CONTRATO varchar (100) COLLATE Latin1_General_CI_AS NULL,
	ID_FRND_FORNECEDOR_COMPRA_CONTRATO varchar(50) COLLATE Latin1_General_CI_AS NULL,
	NO_FRND_FORNECEDOR_CONTRATO varchar(300) COLLATE Latin1_General_CI_AS NULL,
	DS_CMPR_MODALIDADE_COMPRA varchar (100) COLLATE Latin1_General_CI_AS NULL,
	TIPO_CONTRATO varchar (100) COLLATE Latin1_General_CI_AS NULL,
    DS_TMAD_OBJ_TERMO_ADIT varchar(300) COLLATE Latin1_General_CI_AS NULL,
	DS_TMAD_TP_TERMO_ADITIVO varchar (100) COLLATE Latin1_General_CI_AS NULL,
	DS_TMAD_TP_VALOR_TERMO varchar (100) COLLATE Latin1_General_CI_AS NULL,
	DT_TMAD_PUBL_TERMO datetime NULL,
	DT_TMAD_INI_VIGENCIA datetime NULL,
	DT_TMAD_FIM_VIGENCIA datetime NULL,
	QTD_TERMOS_ADITIVOS float NULL,
	VALOR_ACRESCIMO float NULL,
	VALOR_SUPRESSAO float NULL
CONSTRAINT _FORNECEDOR_CONTRATO_ITEM_TERMO_ADITIVO_PK_{0} PRIMARY KEY (CH_TMAD_TERMO_ADIT_EDIT)

);
""".format(id_code)


LOAD_FORNECEDOR_CONTRATO_ITEM_TERMO_ADITIVO="""
select  a14.CH_TMAD_TERMO_ADIT_EDIT,
	a11.ID_TMAD_TERMO_ADITIVO,
	a14.NR_TMAD_IDENT_TERMO_ADIT,
	a11.ID_CNTR_CONTRATO,
	a13.CH_CNTR_IDENT_CONTRATO_EDIT, 
	a13.NR_CNTR_IDENT_CONTRATO,
	a15.ID_UNDD_ORGAO_SUP,
	a116.DS_UNDD_ORGAO_SUP,
	a13.ID_UNDD_UNID_RESP_CONT,
	a15.NO_UNDD_UNIDADE AS NOME_UNID_RESP_CONT,
	CASE WHEN a15.ID_UNDD_ESFERA = '1' THEN 'Municipal'
		WHEN a15.ID_UNDD_ESFERA = '2' THEN 'Estadual'
		WHEN a15.ID_UNDD_ESFERA = '3' THEN 'Federal'
		ELSE 'Não Informado'
		END AS ESFERA,
	CASE WHEN a15.ID_UNDD_PODER = '1' THEN 'Executivo'
		WHEN a15.ID_UNDD_PODER = '2' THEN 'Legislativo'
		WHEN a15.ID_UNDD_PODER = '3' THEN 'Judiciario'
		ELSE 'Não Informado'
		END AS PODER,
	a118.DS_UNDD_TP_ADM AS TIPO_ADMINISTRACAO,
	a117.DS_LCAL_UF,
	a15.ID_LCAL_MUNICIPIO_UNIDADE,
	a115.DS_LCAL_MUNICIPIO,
	a13.DS_CNTR_OBJ_CONT,
	a111.DS_CNTR_SIT_ATUAL_CONTRATO  DS_CNTR_SIT_ATUAL_CONTRATO,
	a11.ID_FRND_FORNECEDOR_COMPRA AS ID_FRND_FORNECEDOR_COMPRA_CONTRATO,
	a16.NO_FRND_FORNECEDOR AS NO_FRND_FORNECEDOR_CONTRATO,
	a17.DS_CMPR_MODALIDADE_COMPRA,
	CASE WHEN  a13.ID_CNTR_TIPO_CONTRATO = '50' THEN 'CONTRATO'
		WHEN  a13.ID_CNTR_TIPO_CONTRATO = '51' THEN 'CREDENCIAMENTO'
		WHEN  a13.ID_CNTR_TIPO_CONTRATO = '52' THEN 'COMODATO'
		WHEN  a13.ID_CNTR_TIPO_CONTRATO = '53' THEN 'ARRENDAMENTO'
		WHEN  a13.ID_CNTR_TIPO_CONTRATO = '54' THEN 'CONCESSÃO'
		ELSE 'Não Informado'
		END AS TIPO_CONTRATO,
	a14.DS_TMAD_OBJ_TERMO_ADIT,
	a113.DS_TMAD_TP_TERMO_ADITIVO,
	a114.DS_TMAD_TP_VALOR_TERMO,
	a14.DT_TMAD_PUBL_TERMO,
	a14.DT_TMAD_INI_VIGENCIA,
	a14.DT_TMAD_FIM_VIGENCIA,
	sum(a11.QT_TMAD_TERMOS_ADITIVOS) AS QTD_TERMOS_ADITIVOS,
	sum(a11.VL_TMAD_ACRESCIMO)  AS VALOR_ACRESCIMO,
	sum(a11.VL_TMAD_SUPRESSAO)  AS VALOR_SUPRESSAO
from	F_TERMO_ADITIVO	a11
	join	D_CMPR_COMPRA	a12
	  on 	(a11.ID_CMPR_COMPRA = a12.ID_CMPR_COMPRA)
	join	D_CNTR_CONTRATO	a13
	  on 	(a11.ID_CNTR_CONTRATO = a13.ID_CNTR_CONTRATO)
	join	D_TMAD_TERMO_ADITIVO	a14
	  on 	(a11.ID_TMAD_TERMO_ADITIVO = a14.ID_TMAD_TERMO_ADITIVO)
	join	D_UNDD_UNIDADE	a15
	  on 	(a13.ID_UNDD_UNID_RESP_CONT = a15.ID_UNDD_UNIDADE)
	join	D_FRND_FORNECEDOR	a16
	  on 	(a11.ID_FRND_FORNECEDOR_COMPRA = a16.ID_FRND_FORNECEDOR)
	join	D_CMPR_MODALIDADE_COMPRA	a17
	  on 	(a12.ID_CMPR_MODALIDADE_COMPRA = a17.ID_CMPR_MODALIDADE_COMPRA)
	join	D_DT_DATA	a18
	  on 	(a14.DT_TMAD_FIM_VIGENCIA = a18.ID_DT_DATA)
	join	D_DT_DATA	a19
	  on 	(a14.DT_TMAD_INI_VIGENCIA = a19.ID_DT_DATA)
	join	D_DT_DATA	a110
	  on 	(a14.DT_TMAD_PUBL_TERMO = a110.ID_DT_DATA)
	join	D_CNTR_SIT_ATUAL_CONTRATO	a111
	  on 	(a13.ID_CNTR_SIT_ATUAL_CONTRATO = a111.ID_CNTR_SIT_ATUAL_CONTRATO)
	join	D_CNTR_TIPO_CONTRATO	a112
	  on 	(a13.ID_CNTR_TIPO_CONTRATO = a112.ID_CNTR_TIPO_CONTRATO)
	join	D_TMAD_TP_TERMO_ADITIVO	a113
	  on 	(a14.ID_TMAD_TP_TERMO_ADITIVO = a113.ID_TMAD_TP_TERMO_ADITIVO)
	join	D_TMAD_TP_VALOR_TERMO	a114
	  on 	(a14.ID_TMAD_TP_VALOR_TERMO = a114.ID_TMAD_TP_VALOR_TERMO)
	join	D_LCAL_MUNICIPIO	a115
	  on 	(a15.ID_LCAL_MUNICIPIO_UNIDADE = a115.ID_LCAL_MUNICIPIO)
	join	D_UNDD_ORGAO_SUP	a116
	  on 	(a15.ID_UNDD_ORGAO_SUP = a116.ID_UNDD_ORGAO_SUP)
	join	D_LCAL_UF	a117
	  on 	(a15.ID_LCAL_UF_UNIDADE = a117.ID_LCAL_UF)
	Join    D_UNDD_TP_ADM a118
	  on    (a15.ID_UNDD_TP_ADM = a118.ID_UNDD_TP_ADM)  
group by	a14.CH_TMAD_TERMO_ADIT_EDIT,
	a11.ID_TMAD_TERMO_ADITIVO,
	a14.NR_TMAD_IDENT_TERMO_ADIT,
	a11.ID_CNTR_CONTRATO,
	a13.CH_CNTR_IDENT_CONTRATO_EDIT,
	a13.NR_CNTR_IDENT_CONTRATO,
	a15.ID_UNDD_ORGAO_SUP,
	a116.DS_UNDD_ORGAO_SUP,
	a13.ID_UNDD_UNID_RESP_CONT,
	a15.NO_UNDD_UNIDADE,
	a15.ID_UNDD_ESFERA,
	a15.ID_UNDD_PODER,
	a118.DS_UNDD_TP_ADM,
	a13.ID_UNDD_UNID_RESP_CONT,
	a117.DS_LCAL_UF,
	a15.ID_LCAL_MUNICIPIO_UNIDADE,
	a115.DS_LCAL_MUNICIPIO,
	a13.DS_CNTR_OBJ_CONT,
	a111.DS_CNTR_SIT_ATUAL_CONTRATO,FORNECEDOR_CONTRATO_ITEM_TERMO_ADITIVO
	a11.ID_FRND_FORNECEDOR_COMPRA,
	a16.NO_FRND_FORNECEDOR,
	a17.DS_CMPR_MODALIDADE_COMPRA,
	a13.ID_CNTR_TIPO_CONTRATO,
	a112.DS_CNTR_TIPO_CONTRATO,
	a14.DS_TMAD_OBJ_TERMO_ADIT,
	a113.DS_TMAD_TP_TERMO_ADITIVO,
	a114.DS_TMAD_TP_VALOR_TERMO,
    -- -- -- -- DATAS P AJUSTAR NO PYTON:
	a14.DT_TMAD_PUBL_TERMO,
	a14.DT_TMAD_INI_VIGENCIA,
	a14.DT_TMAD_FIM_VIGENCIA
"""