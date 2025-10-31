create database if not exists dw_siad;
use dw_siad;

create table if not exists dim_tempo (
    id_tempo int primary key,             
    data_completa date,         
    ano char(4),            
    mes char(2),          
    dia char(2),            
    ano_epidemiologico char(4),
    semana_epidemiologica char(2)
);

create table if not exists dim_local(
    id_local int primary key,
    cod_municipio char(7),
    nome_municipio varchar(100),
    uf varchar(100)
);

create table if not exists fato_casos_dengue (
    id_tempo int,
    id_local int,
    num_casos int,
    num_obitos int,
    num_autoctones int,
    num_masculino int,
    num_feminino int,
    num_criancas int,
    num_adolescentes int,
    num_adultos int,
    num_idosos int,
    num_hospitalizacao int,

    constraint pk_fato_casos_dengue primary key (id_tempo, id_local),
    constraint fk_tempo foreign key (id_tempo) references dim_tempo(id_tempo),
    constraint fk_local foreign key (id_local) references dim_local(id_local)

);

create table if not exists fato_clima (
    id_tempo int,
    id_local int,
    temperatura_media float,
    precipitacao_total float,

    constraint pk_fato_clima primary key (id_tempo, id_local),
    constraint fk_tempo_clima foreign key (id_tempo) references dim_tempo(id_tempo),
    constraint fk_local_clima foreign key (id_local) references dim_local(id_local)
);

create table if not exists fato_socioeconomico (
    id_local int,
    id_tempo int,
    num_agua_tratada int,
    num_populacao int,
    num_esgoto int,

    constraint pk_fato_socioeconomico primary key (id_local, id_tempo),
    constraint fk_tempo_socioeconomico foreign key (id_tempo) references dim_tempo(id_tempo),
    constraint fk_local_socioeconomico foreign key (id_local) references dim_local(id_local)
);

