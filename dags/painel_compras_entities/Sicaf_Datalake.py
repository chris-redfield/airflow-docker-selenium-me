# -*- coding: utf-8 -*-
FORNECEDOR =  """SELECT distinct p.id_pessoa   as ID_PESSOA, 
                            CAST((case when p.tp_pessoa = 1 then REPLACE(LEFT(p.num_cpf_cnpj,11),' ','0') when p.tp_pessoa = 2 then REPLACE(LEFT(p.num_cpf_cnpj,14),' ','0') end) As VARCHAR(50)) as ID_FORNECEDOR, 
                            CAST((case when p.tp_pessoa = 1 then REPLACE(LEFT(p.num_cpf_cnpj,11),' ','0') when p.tp_pessoa = 2 then REPLACE(LEFT(p.num_cpf_cnpj,14),' ','0') end) As VARCHAR(50)) as NU_CNPJ_CPF, 
                            p.id_fornecedor_proprietario as ID_FORNECEDOR_PROPRIETARIO, 
                            p.id_endereco    as ID_ENDERECO, 
                            e.id_cidade 	 as ID_CIDADE, 
                            p.nom_nome 	 as NO_FORNECEDOR, 
                            pj.nom_fantasia  as NO_FANTASIA, 
                            pj.dt_abertura   as DT_ABERTURA, 
                            pj.codigosituacaocadastralrfb as CO_SITUACAO_RFB, 
                            p.num_ddd 		 as DDD, 
                            p.num_telefone   as TELEFONE, 
                            p.num_ddd_alternativo as DDD_ALTERNATIVO, 
                            p.num_telefone_alternativo as TELEFONE_ALTERNATIVO, 
                            p.dsc_email as EMAIL, 
                            e.dsc_bairro as BAIRRO, 
                            e.dsc_logradouro as LOGRADOURO, 
                            e.numero as NUMERO_ENDERECO, 
                            e.dsc_complemento as COMPLEMENTO_ENDERECO, 
                            e.num_cep as CEP, 
                            c.id_uf as UF, 
                            c.dsc_cidade as CIDADE, 
                            case when p.tp_pessoa = 1 then 'Física' when p.tp_pessoa = 2 then 'Jurídica' when p.tp_pessoa = 3 then 'Estrangeira' end as TIPO_FORNECEDOR, 
                            id_conjuge_atual as ID_CONJUGE 
                        FROM pessoa p 
                        inner join pessoa_juridica pj on pj.id_pessoa_juridica = p.id_pessoa 
                        left join pessoa_fisica pf on pf.id_pessoa_fisica = p.id_pessoa 
                        left join endereco e on p.id_endereco = e.id_endereco 
                        left join cidade c on c.id_cidade = e.id_cidade and c.ind_ativa = 'S'
                        Where p.tp_vinculo_fornecedor in (1,8);
                    """

SERVIDOR = """SELECT num_cpf as ID_SERVIDOR,
                        nom_nome as NO_SERVIDOR,
                        dsc_lotacao as LOTACAO,
                        dsc_cargo_funcao as CARGO,
                        dt_consulta as DT_CONSULTA_SERVIDOR
                    FROM vinculo_servico_publico
                    Where dsc_lotacao is not null;
                    """

FORNECEDOR_COMPLEMENTO= """SELECT f.id_fornecedor as ID_FORN_SICAF,
                        fj.id_pessoa_juridica as ID_PESSOA,
                        fj.id_responsavel_cadastro as ID_RESPONSAVEL_CADASTRO,
                        bl.DT_VALIDADE_BALANCO,
                        f.dt_cadastramento as DT_CADASTRO,
                        f.dt_ultima_atualizacao as DT_ULTIMA_ATUALIZACAO,
                        f.dt_vencimento_credenciamento AS DT_VENCIMENTO_CREDENCIAMENTO,
                        cast(f.dt_validacao_credenciamento as date) AS DT_VALIDADE_CREDENCIAMENTO,
                        f.tp_fornecedor AS ID_TIPO_FORNECEDOR,
                        case when f.tp_fornecedor = 1 then 'Física' when f.tp_fornecedor=2 then 'Jurídica' ELSE 'Não informado' end as TIPO_FORNECEDOR,
                        fj.cod_inscricao_estadual AS CO_INSCRICAO_ESTADUAL,
                        fj.cod_inscricao_municipal AS CO_INSCRICAO_MUNICIPAL,
                        fj.vl_capital_social AS VL_CAPITAL_SOCIAL,
                        fj.id_cnae AS ID_CNAE,
                        cn.dsc_cnae AS CNAE,
                        fj.id_tipo_empresa AS ID_PORTE_EMPRESA,
                        case when fj.id_tipo_empresa = 1 then 'Micro Empresa' when fj.id_tipo_empresa = 3 then 'Empresa de Pequeno Porte' when fj.id_tipo_empresa = 5 then 'Demais' else 'Não informado' end as PORTE_EMPRESA,    
                        f.ind_ativo AS STATUS,
                        f.ind_existe_pendencia_nivel1 AS EXISTE_PEND_NIVEL1,
                        f.situacao_nivel1 AS SITUACAO_NIVEL1,
                        f.situacao_nivel2 AS SITUACAO_NIVEL2,
                        f.situacao_nivel3 AS SITUACAO_NIVEl3,
                        fj.situacao_nivel4 AS SITUACAO_NIVEL4,
                        f.situacao_nivel5 AS SITUACAO_NIVEL5,        
                        fj.situacao_nivel6 AS SITUACAO_NIVEL6,
                        rc.num_cpf as CPF_RESPONSAVEL,
                        rc.nom_nome as NOME_RESPONSAVEL,
                        rc.dsc_email as EMAIL_RESPONSAVEL                        
                    FROM fornecedor f
                    left join fornecedor_juridico fj on fj.id_fornecedor_juridico = f.id_fornecedor
                    left join responsavel_cadastro rc on rc.id_responsavel_cadastro = fj.id_responsavel_cadastro
                    left join cnae cn on cn.id_cnae = fj.id_cnae
                    left join (
                    SELECT id_fornecedor_juridico,
                          max(dt_validade) AS DT_VALIDADE_BALANCO
                    FROM balanco_patrimonial
                    GROUP BY id_fornecedor_juridico
                    ) bl on bl.id_fornecedor_juridico = f.id_fornecedor;
                """


FORNECEDOR_SOCIO = """Select DISTINCT CAST((case when p.tp_pessoa = 1 then REPLACE(LEFT(p.num_cpf_cnpj,11),' ','0') when p.tp_pessoa = 2 then REPLACE(LEFT(p.num_cpf_cnpj,14),' ','0') end) As VARCHAR(50)) as ID_FORNECEDOR,
       p.nom_nome as NO_FORNECEDOR,
       p.id_fornecedor_proprietario as ID_FORNECEDOR_PROPRIETARIO,
       p.tp_vinculo_fornecedor as TP_VINCULO_FORNECEDOR,
       CAST((case when i.tp_pessoa = 1 then REPLACE(LEFT(i.num_cpf_cnpj,11),' ','0') when i.tp_pessoa = 2 then REPLACE(LEFT(i.num_cpf_cnpj,14),' ','0') end) As VARCHAR(50)) as SOCIO
  from pessoa p
inner join PESSOA i on p.id_fornecedor_proprietario = i.id_fornecedor_proprietario 
Where p.tp_vinculo_fornecedor in(1,8) and i.tp_vinculo_fornecedor not in(1,8) and p.data_hora_desvinculacao is null and i.data_hora_desvinculacao is null;
"""


SOCIO = """
Select DISTINCT
    CAST((case when p.tp_pessoa = 1 then REPLACE(LEFT(p.num_cpf_cnpj,11),' ','0') when p.tp_pessoa = 2 then REPLACE(LEFT(p.num_cpf_cnpj,14),' ','0') end) As VARCHAR(50)) as socio,
    CAST((case when p.tp_pessoa = 1 then REPLACE(LEFT(p.num_cpf_cnpj,11),' ','0') when p.tp_pessoa = 2 then REPLACE(LEFT(p.num_cpf_cnpj,14),' ','0') end) As VARCHAR(50)) as id_socio,
    p.nom_nome as no_socio,
    CAST(p.id_fornecedor_proprietario As VARCHAR(50)) as pai_socio,
    case when p.tp_vinculo_fornecedor=1 or p.tp_vinculo_fornecedor=8 then 'Fornecedor' when p.tp_vinculo_fornecedor=3 or p.tp_vinculo_fornecedor=8 then 'Sócio' when p.tp_vinculo_fornecedor=4 then 'Dirigente' when p.tp_vinculo_fornecedor=7 then 'Sócio e Dirigente' else 'Não informado' end as grupo,
    p.tp_pessoa as tp_socio,
    p.num_participacao_acionaria
from pessoa p
Where p.tp_vinculo_fornecedor not in (1, 8)  and p.data_hora_desvinculacao is null
union 
Select DISTINCT
	CAST((case when p.tp_pessoa = 1 then REPLACE(LEFT(p.num_cpf_cnpj,11),' ','0') when p.tp_pessoa = 2 then REPLACE(LEFT(p.num_cpf_cnpj,14),' ','0') end) As VARCHAR(50)) as socio,
	CAST(p.id_fornecedor_proprietario As VARCHAR(50)) as id_socio,      
     i.nom_nome as no_socio,
    '' as pai_socio,
    'Fornecedor' as grupo,
    2 as tp_socio,
    p.num_participacao_acionaria
from pessoa p
inner join pessoa i on p.id_fornecedor_proprietario = i.id_pessoa
Where p.data_hora_desvinculacao is null and i.data_hora_desvinculacao is null
union
Select DISTINCT
       CAST((case when p.tp_pessoa = 1 then REPLACE(LEFT(p.num_cpf_cnpj,11),' ','0') when p.tp_pessoa = 2 then REPLACE(LEFT(p.num_cpf_cnpj,14),' ','0') end) As VARCHAR(50)) as socio,
       REPLACE(LEFT(p.num_cpf_cnpj,11),' ','0') as id_socio,
       c.nom_nome as no_socio,
       CAST((case when p.tp_pessoa = 1 then REPLACE(LEFT(p.num_cpf_cnpj,11),' ','0') when p.tp_pessoa = 2 then REPLACE(LEFT(p.num_cpf_cnpj,14),' ','0') end) As VARCHAR(50)) as pai_socio,
	   'Conjuge' as grupo,
	   p.tp_pessoa as tp_socio,
	   p.num_participacao_acionaria
from conjuge c
inner join pessoa_fisica pf on pf.id_conjuge_atual = c.id_conjuge 
inner join pessoa p on p.id_pessoa = pf.id_pessoa_fisica and p.tp_vinculo_fornecedor not in (1, 8)
Where p.data_hora_desvinculacao is null
union
Select DISTINCT
       CAST((case when p.tp_pessoa = 1 then REPLACE(LEFT(p.num_cpf_cnpj,11),' ','0') when p.tp_pessoa = 2 then REPLACE(LEFT(p.num_cpf_cnpj,14),' ','0') end) As VARCHAR(50)) as socio,
       CAST(p.id_fornecedor_proprietario As VARCHAR(50))  as id_socio,
       i.nom_nome as no_socio,
	   REPLACE(LEFT(p.num_cpf_cnpj,11),' ','0') as pai_socio,
	   'Fornecedor do Conjuge' as grupo,
	   p.tp_pessoa as tp_socio,
	   p.num_participacao_acionaria
from conjuge c
inner join pessoa p on p.num_cpf_cnpj = c.num_cpf
inner join pessoa i on p.id_fornecedor_proprietario = i.id_pessoa
Where p.data_hora_desvinculacao is null and i.data_hora_desvinculacao is null;  
"""

FORNECEDOR_PENALIDADE = """SELECT
	 case when f.tp_pessoa = 1 then LEFT(f.num_cpf_cnpj,11) when f.tp_pessoa = 2 then LEFT(f.num_cpf_cnpj,14) else F.num_cpf_cnpj end as ID_FORNECEDOR,
      'SICAF' as ORIGEM,
      'Inidoneidade' as TIPO,
	 OS.dt_inicial AS DT_INICIO,
	 OS.dt_final AS DT_FINAL,
	 M.dsc_motivo_dinamica AS DESCRICAO,
	 O.id_orgao_sancionador AS UASG,
	 U.nom_unidade AS ORGAO_SANCIONADOR,
	 O.dsc_detalhe AS DETALHE
 FROM ocorrencia O
 INNER JOIN fornecedor_infrator F ON F.id_fornecedor_infrator = O.id_fornecedor_infrator
 INNER JOIN ocorrencia_inidoneidade OS ON OS.id_ocorrencia_inidoneidade = O.id_ocorrencia 
 INNER JOIN motivo_ocorrencia_dinamica M ON OS.id_motivo = M.id_motivo_ocorrencia_dinamica
  INNER JOIN uasg U ON U.id_uasg = O.id_uasg
union 
SELECT
      case when f.tp_pessoa = 1 then LEFT(f.num_cpf_cnpj,11) when f.tp_pessoa = 2 then LEFT(f.num_cpf_cnpj,14) else F.num_cpf_cnpj end as ID_FORNECEDOR,
      'SICAF' as ORIGEM,
      'Impeditiva' as TIPO,
	 OS.dt_inicial AS DT_INICIO,
	 OS.dt_final AS DT_FINAL,
	 M.dsc_motivo_dinamica AS DESCRICAO,
	 O.id_orgao_sancionador AS UASG,
	 U.nom_unidade AS ORGAO_SANCIONADOR,
	 O.dsc_detalhe AS DETALHE
 FROM ocorrencia O
 INNER JOIN fornecedor_infrator F ON F.id_fornecedor_infrator = O.id_fornecedor_infrator
 INNER JOIN ocorrencia_impedimento OS ON OS.id_ocorrencia_impedimento = O.id_ocorrencia 
 INNER JOIN motivo_ocorrencia_dinamica M ON OS.id_motivo = M.id_motivo_ocorrencia_dinamica 
 INNER JOIN uasg U ON U.id_uasg = O.id_uasg
union 
SELECT
      case when f.tp_pessoa = 1 then LEFT(f.num_cpf_cnpj,11) when f.tp_pessoa = 2 then LEFT(f.num_cpf_cnpj,14) else F.num_cpf_cnpj end as ID_FORNECEDOR,
      'SICAF' as ORIGEM,
      'Suspensão' as TIPO,
	 OS.dt_inicial AS DT_INICIO,
	 OS.dt_final AS DT_FINAL,
	 M.dsc_motivo_dinamica AS DESCRICAO,
	 O.id_orgao_sancionador AS UASG,
	 U.nom_unidade AS ORGAO_SANCIONADOR,
	 O.dsc_detalhe AS DETALHE
 FROM ocorrencia O
 INNER JOIN fornecedor_infrator F ON F.id_fornecedor_infrator = O.id_fornecedor_infrator
 INNER JOIN ocorrencia_suspensao OS ON OS.id_ocorrencia_suspensao = O.id_ocorrencia 
 INNER JOIN motivo_ocorrencia_dinamica M ON OS.id_motivo = M.id_motivo_ocorrencia_dinamica
 INNER JOIN uasg U ON U.id_uasg = O.id_uasg
"""

REGULARIDADE_TRABALHISTA_FEDERAL = """Select ID_FORNECEDOR,
       ORDEM,
       ATRIBUTO,
	   VALOR,
	   OBSERVACAO,
	   case when VALOR = 'Sim' then 1 else 0 end as ICONE,
	   1 as STATUS
from (
Select t.id_fornecedor AS ID_FORNECEDOR,
       '5' AS ORDEM,
	   'SITUAÇÃO DE DOCUMENTOS TRABALHISTAS FEDERAIS: ' AS ATRIBUTO,
		case when
		count(case when t.id_emissor_comprovante=1 and valido=1 then 1 end) = 1 and
		count(case when t.id_emissor_comprovante=2 and valido=1 then 1 end) = 1 and	
		count(case when t.id_emissor_comprovante=7 and valido=1 then 1 end) = 1 then
		'Sim' else 'Não' end  VALOR,
		'Informa se consta ou não informações e documentos junto ao SICAF para avaliação da situação dos documentos trabalhistas federais.' as OBSERVACAO
from (
Select CAST((case when p.tp_pessoa = 1 then REPLACE(LEFT(p.num_cpf_cnpj,11),' ','0') when p.tp_pessoa = 2 then REPLACE(LEFT(p.num_cpf_cnpj,14),' ','0') end) As VARCHAR(50)) as id_fornecedor, 
       c.id_emissor_comprovante, 
       case when max(c.dt_validade) >= now() and f.situacao_nivel3=6 then 1 else 0 end as valido
from comprovante_nivel_cadastramento c
inner join fornecedor f on f.id_fornecedor = c.id_fornecedor
inner join fornecedor_juridico fj on fj.id_fornecedor_juridico = f.id_fornecedor
inner join pessoa p on p.id_pessoa =  fj.id_pessoa_juridica
Where tp_vinculo_fornecedor in (1, 8)
group by p.num_cpf_cnpj, tp_pessoa, c.id_emissor_comprovante, f.situacao_nivel3
) t
group by t.id_fornecedor
) t;
"""


REGULARIDADE_TRABALHISTA_MUNICIPAL = """Select ID_FORNECEDOR,
    ORDEM,
	ATRIBUTO,
	VALOR,
	OBSERVACAO,
	case when VALOR = 'Sim' then 1 else 0 end as ICONE,
                   1 as STATUS
from (
Select t.id_fornecedor AS ID_FORNECEDOR,
       '6' AS ORDEM,
      'SITUAÇÃO DE DOCUMENTOS TRABALHISTAS MUNICIPAIS: ' AS ATRIBUTO,
case when	
	count(case when t.id_emissor_comprovante=4 and valido=1 then 1 end)  = 1 and
	count(case when t.id_emissor_comprovante=5 and valido=1 then 1 end)  = 1 then
	'Sim' else 'Não' end  VALOR,
	'Informa se consta ou não informações e documentos junto ao SICAF para avaliação da situação dos documentos trabalhistas municipais.' as OBSERVACAO
from (
Select CAST((case when p.tp_pessoa = 1 then REPLACE(LEFT(p.num_cpf_cnpj,11),' ','0') when p.tp_pessoa = 2 then REPLACE(LEFT(p.num_cpf_cnpj,14),' ','0') end) As VARCHAR(50)) as id_fornecedor, 
       c.id_emissor_comprovante, 
       case when max(c.dt_validade) >= now() and fj.situacao_nivel4=6 then 1 else 0 end as valido
from comprovante_nivel_cadastramento c
inner join fornecedor_juridico fj on fj.id_fornecedor_juridico = c.id_fornecedor
inner join pessoa p on p.id_pessoa =  fj.id_pessoa_juridica
Where tp_vinculo_fornecedor in (1, 8)
group by p.num_cpf_cnpj, p.tp_pessoa, c.id_emissor_comprovante, fj.situacao_nivel4
) t
group by t.id_fornecedor
) t;
"""


IMPEDITIVO_PENALIDADE = """SELECT CAST((case when p.tp_pessoa = 1 then REPLACE(LEFT(p.num_cpf_cnpj,11),' ','0') when p.tp_pessoa = 2 then REPLACE(LEFT(p.num_cpf_cnpj,14),' ','0') end) As VARCHAR(50)) as id_fornecedor,
       2 as ORDEM,
       CAST('CONDIÇÃO DE PARTICIPAR DE LICITAÇÕES (IMPEDIMENTO): ' as varchar(50)) as ATRIBUTO,
       CAST(CASE WHEN COUNT(id_ocorrencia) > 0 THEN 'Sim' ELSE 'Não' end as varchar(4)) AS VALOR,
       'Informa se consta algum impedimento na base do SICAF (Sistema de Cadastramento de Fornecedores) do Governo Federal.'  AS OBSERVACAO,
       CAST(CASE WHEN COUNT(id_ocorrencia) > 0 THEN 0 ELSE 1 end as varchar(1)) AS ICONE,
       1 as STATUS
FROM FORNECEDOR_JURIDICO F
INNER JOIN PESSOA P ON F.ID_PESSOA_JURIDICA = P.ID_PESSOA
LEFT JOIN
(       
SELECT
      O.id_fornecedor AS ID_FORNECEDOR,
      OS.id_ocorrencia_inidoneidade as id_ocorrencia
 FROM ocorrencia O
 INNER JOIN ocorrencia_inidoneidade OS ON OS.id_ocorrencia_inidoneidade = O.id_ocorrencia 
Where O.id_tp_ocorrencia = 4 and O.ind_excluida = 'N' and OS.dt_inicial <= now() and (OS.dt_final >= now() or OS.dt_final is null)
union 
SELECT
      O.id_fornecedor AS ID_FORNECEDOR,
	 OS.id_ocorrencia_inativacao as id_ocorrencia
 FROM ocorrencia O
 INNER JOIN ocorrencia_inativacao OS ON OS.id_ocorrencia_inativacao = O.id_ocorrencia 
WHERE O.id_tp_ocorrencia = 6 and O.ind_excluida = 'N'
union 
SELECT
      O.id_fornecedor AS ID_FORNECEDOR,
	 OS.id_ocorrencia_generica as id_ocorrencia
 FROM ocorrencia O
 INNER JOIN ocorrencia_generica OS ON OS.id_ocorrencia_generica = O.id_ocorrencia 
WHERE O.id_tp_ocorrencia = 7 and O.ind_excluida = 'N'  and OS.dt_inicial <= now() and (OS.dt_final >= now() or OS.dt_final is null)
union 
SELECT
      O.id_fornecedor AS ID_FORNECEDOR,
	 OS.id_ocorrencia_suspensao as id_ocorrencia
 FROM ocorrencia O
 INNER JOIN ocorrencia_suspensao OS ON OS.id_ocorrencia_suspensao = O.id_ocorrencia 
WHERE O.id_tp_ocorrencia = 3 and O.ind_excluida = 'N'  and OS.dt_inicial <= now() and (OS.dt_final >= now() or OS.dt_final is null)
union 
SELECT
      O.id_fornecedor AS ID_FORNECEDOR,
	 OS.id_ocorrencia_impedimento as id_ocorrencia
 FROM ocorrencia O
 INNER JOIN ocorrencia_impedimento OS ON OS.id_ocorrencia_impedimento = O.id_ocorrencia 
WHERE O.id_tp_ocorrencia = 5 and O.ind_excluida = 'N'  and OS.dt_inicial <= now() and (OS.dt_final >= now() or OS.dt_final is null)
union
SELECT
      O.id_fornecedor AS ID_FORNECEDOR,
	 OS.id_ocorrencia_dinamica as id_ocorrencia
 FROM ocorrencia O 
 INNER JOIN ocorrencia_dinamica OS ON OS.id_ocorrencia_dinamica = O.id_ocorrencia 
 INNER JOIN tipo_ocorrencia_dinamica tpd on tpd.id_tp_ocorrencia_dinamica=OS.id_tp_ocorrencia_dinamica
WHERE O.id_tp_ocorrencia = 10 and O.ind_excluida = 'N' and tpd.ind_impeditiva = 'S'  and OS.dt_inicial <= now() and (OS.dt_final >= now() or OS.dt_final is null)
) O ON O.ID_FORNECEDOR = F.ID_FORNECEDOR_JURIDICO
GROUP BY P.num_cpf_cnpj, p.tp_pessoa;
"""
