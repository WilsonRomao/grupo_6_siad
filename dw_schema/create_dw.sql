create database if not exists dw_siad;
use dw_siad;

create table dim_tempo (
    id_tempo int primary key,             
    data_completa date,         
    ano char(4),            
    mes char(2),          
    dia char(2),            
    ano_epidemiologico char(4),
    semana_epidemiologica char(2)
);

create table dim_local(
    id_local int primary key,
    cod_municipio char(7),
    nome_municipio varchar(100),
    uf char(2)
);

create table fato_casos_dengue (
    id_tempo int,
    id_local int,
    num_casos int,
    num_obitos int,
    num_autoctones int,
    num_masculinos int,
    num_femininos int,
    num_criancas int,
    num_adultos int,
    num_idosos int,
    num_hospitalizados int,

    constraint pk_fato_casos_dengue primary key (id_tempo, id_local),
    constraint fk_tempo foreign key (id_tempo) references dim_tempo(id_tempo),
    constraint fk_local foreign key (id_local) references dim_local(id_local)

);

create table fato_clima (
    id_tempo int,
    id_local int,
    temperatura_media float,
    precipitacao_total float,

    constraint pk_fato_clima primary key (id_tempo, id_local),
    constraint fk_tempo_clima foreign key (id_tempo) references dim_tempo(id_tempo),
    constraint fk_local_clima foreign key (id_local) references dim_local(id_local)
);

create table fato_saneamento (
    id_local int,
    id_tempo int,
    percentual_acesso_agua float,
    percentual_esgoto_tratado float,
    percentual_coleta_lixo float,

    constraint pk_fato_saneamento primary key (id_local, id_tempo),
    constraint fk_tempo_saneamento foreign key (id_tempo) references dim_tempo(id_tempo),
    constraint fk_local_saneamento foreign key (id_local) references dim_local(id_local)
);

