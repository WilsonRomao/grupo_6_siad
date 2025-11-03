# Projeto de ETL - Análise de Dengue

Pipeline de ETL para consolidar dados de Dengue, Clima (INMET) e Fatores Socioeconômicos (SNIS) em um Data Warehouse MySQL.

## Requisitos

Para executar este projeto, irá precisar de:

* Python 3.10+
* Um servidor MySQL a correr (Ex: `localhost:3306`).
* Todas as dependências do Python listadas no `requirements.txt`.

## 1. Instalação

Comece por criar um ambiente virtual e instalar as dependências.

```bash
# 1. Crie um ambiente virtual (opcional, mas recomendado)
python3 -m venv .venv
source .venv/bin/activate

# 2. Instale todas as bibliotecas necessárias
pip install -r requirements.txt
````

## 2\. Configuração

Antes de executar, são necessárias duas configurações:

### A. Base de Dados

1.  Crie um arquivo de configuração em `configs/db_config.yml`.
2.  Preencha-o com as suas credenciais do MySQL.

**Exemplo (`configs/db_config.yml`):**

```yaml
mysql:
  host: localhost
  port: 3306
  user: root
  password: "sua_senha_secreta"
  database: "dw_dengue" 
```

### B. Estrutura de Dados

Os scripts de ETL esperam que os dados brutos estejam na seguinte estrutura de pastas:

```
.
├── dados/
│   ├── brutos/
│   │   ├── dengue/
│   │   │   └── DENGBR2017.csv
│   │   │   └── DENGBR2018.csv
│   │   │   └── (etc...)
│   │   ├── local/
│   │   │   └── RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.xls
│   │   ├── meteorologico/
│   │   │   └── apenas_capitais/
│   │   │       └── INMET_SE_SP_A701_SAO PAULO - MIRANTE_01-01-2017_A_31-12-2017.CSV
│   │   │       └── (etc...)
│   │   └── br_mdr_snis_municipio_agua_esgoto.csv
│   │
│   └── processados/
│       (Esta pasta será criada e preenchida pelos scripts)
│
├── configs/
│   └── db_config.yml
│
├── dw_schema/
│   └── create_dw.sql
│
├── scripts/
│   ├── create_tables.py
│   ├── cria_dimensoes.py
│   ├── etl_clima.py
│   ├── etl_dengue.py
│   ├── etl_socioeconomico.py
│   ├── load.py
│   └── run_pipeline.py
│
└── requirements.txt
```

## 3\. Como Executar

Este projeto foi desenhado para ser executado através do orquestrador `run_pipeline.py`.

Certifique-se que o seu servidor MySQL está funcionando, em seguida, execute o seguinte comando a partir da pasta **raiz** do projeto:

```bash
python3 scripts/run_pipeline.py
```

O orquestrador irá executar todos os scripts na ordem:

1.  **`create_tables.py`**: Cria a base de dados `dw_dengue` e todas as tabelas.
2.  **`cria_dimensoes.py`**: Gera os CSVs das dimensões (local, tempo).
3.  **`etl_dengue.py`**: Processa os dados da dengue.
4.  **`etl_clima.py`**: Processa os dados de clima.
5.  **`etl_socioeconomico.py`**: Processa os dados do SNIS.
6.  **`load.py`**: Carrega todos os CSVs processados para o MySQL.