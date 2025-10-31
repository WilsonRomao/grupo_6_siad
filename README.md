# Projeto de Data Warehouse: Análise de Casos de Dengue

Este projeto é um pipeline de ETL (Extract, Transform, Load) completo, projetado para coletar, processar e carregar dados de diversas fontes em um Data Warehouse (DW). O objetivo é centralizar informações sobre casos de dengue, clima e fatores socioeconômicos para permitir análises e a criação de dashboards.

## 🎯 Objetivo do Projeto

O Data Warehouse construído por este pipeline permite correlacionar dados de saúde pública (casos, óbitos, hospitalizações por dengue) com dados externos, visando responder perguntas como:

  * Qual a correlação entre a precipitação e a temperatura média com o aumento de casos de dengue nas semanas seguintes?
  * Existe uma relação entre indicadores de saneamento básico (água e esgoto) e a incidência de dengue em diferentes capitais?
  * Como os casos de dengue evoluíram ao longo dos anos, segmentados por faixa etária e sexo, nas principais capitais do Brasil?

## Data Model (Esquema Estrela)

O pipeline foi desenhado para popular um esquema estrela (Star Schema) simples, ideal para ferramentas de Business Intelligence (como Power BI ou Tableau).

  * **Dimensões (Tabelas de Atributos):**

      * `dim_local`: Contém as 27 capitais do Brasil (UF, Nome, Código IBGE).
      * `dim_tempo`: Dimensão de calendário com granularidade diária, enriquecida com `ano_epidemiologico` e `semana_epidemiologica`.

  * **Fatos (Tabelas de Métricas):**

      * `fato_casos_dengue`: Métricas de dengue (casos, óbitos, hospitalizações) com granularidade **diária**.
      * `fato_clima`: Métricas de clima (temperatura média, precipitação total) com granularidade **semanal (epidemiológica)**.
      * `fato_socioeconomico`: Métricas de saneamento (população, água, esgoto) com granularidade **anual**.

<!-- end list -->

```
                 +-------------------+
                 |    dim_local      |
                 +-------------------+
                 |*id_local (PK)     |
                 | uf                |
                 | nome_municipio    |
                 | cod_municipio     |
                 +-------------------+
                      ^      ^      ^
                      |      |      |
+---------------------+      |      +-----------------------+
|                            |                              |
|   +---------------------+  |  +-------------------------+ |
|   | fato_casos_dengue   |  |  |   fato_socioeconomico   | |
|   +---------------------+  |  +-------------------------+ |
|   |*id_local (FK)       |  |  |*id_local (FK)           | |
|   |*id_tempo (FK)       |  |  |*id_tempo (FK) (anual)   | |
|   | num_casos           |  |  | num_populacao           | |
|   | num_obitos          |  |  | num_agua_tratada        | |
|   | ... (outras métricas) |  |  | num_esgoto              | |
|   +---------------------+  |  +-------------------------+ |
|             ^              |                ^             |
|             |              |                |             |
(diário)      |      +-------+-------+        | (anual)
              +------+   dim_tempo   +--------+
                     +---------------+
(semanal)     |      |*id_tempo (PK) |        |
              |      | data_completa |        |
+-------------+      | ano           |        |
|                    | mes           |        |
| +----------------+ | dia           |        |
| |  fato_clima    | | ano_epid...   |        |
| +----------------+ | semana_epid...|        |
| |*id_local (FK)  | +---------------+        |
| |*id_tempo (FK)  |                          |
| | temp_media     |                          |
| | precip_total   |                          |
| +----------------+                          |
|                                             |
+---------------------------------------------+
```

## ⚙️ Arquitetura e Funcionamento do Pipeline

O pipeline é orquestrado pelo script `run_pipeline.py`, que executa todos os passos na ordem correta de dependência.

1.  **`create_tables.py`**

      * **O que faz:** Lê o arquivo `dw_schema/create_dw.sql` e as credenciais de `configs/db_config.yml`.
      * **Resultado:** Cria toda a estrutura de tabelas (DIMs e FATOs) no banco de dados MySQL.

2.  **`cria_dimensoes.py`**

      * **O que faz:** Gera as dimensões que são pré-requisito para os demais scripts.
      * **`dim_local`:** Lê o arquivo `.../RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.xls`, filtra apenas as 27 capitais e resolve ambiguidades (ex: "Palmas", "Campo Grande").
      * **`dim_tempo`:** Cria um calendário completo, dia a dia, para o intervalo de anos definido (ex: 2017-2022) e calcula a semana epidemiológica.
      * **Resultado:** Salva `dim_local.csv` e `dim_tempo.csv` na pasta `dados/processados/`.

3.  **`etl_dengue.py`**

      * **O que faz:** Processa os dados brutos de dengue (arquivos `DENGBR*.csv`).
      * **Otimização:** Carrega `dim_local` primeiro e filtra os dados brutos *durante a leitura*, economizando memória.
      * **Transformação:** Limpa dados, calcula idade, trata nulos, e mapeia as chaves `id_local` e `id_tempo` (diária).
      * **Resultado:** Agrega os dados por dia/local e salva `fato_casos_dengue.csv` em `dados/processados/`.

4.  **`etl_clima.py`**

      * **O que faz:** Processa múltiplos arquivos de clima do INMET (arquivos `INMET_*.CSV`).
      * **Transformação:** Lê os metadados do cabeçalho de cada arquivo para identificar a capital. Trata dados horários, interpola valores faltantes (NaNs) e agrega para o nível **diário**.
      * **Agregação:** Mapeia `id_local` e `id_tempo` e, em seguida, agrupa os dados diários no nível **semanal (epidemiológico)**, calculando médias (temperatura) e somas (precipitação).
      * **Resultado:** Salva `fato_clima.csv` em `dados/processados/`.

5.  **`etl_socioeconomico.py`**

      * **O que faz:** Processa os dados do SNIS (Sistema Nacional de Informações sobre Saneamento).
      * **Transformação:** Filtra os dados anuais para as capitais presentes na `dim_local`.
      * **Agregação:** Mapeia `id_local` e `id_tempo` aplicando a regra de negócio de granularidade **anual** (buscando o `id_tempo` referente ao dia 1º de Janeiro de cada ano).
      * **Resultado:** Salva `fato_socioeconomico.csv` em `dados/processados/`.

6.  **`load.py`**

      * **O que faz:** Etapa final de carga no Data Warehouse.
      * **Idempotência:** Primeiro, executa `DELETE FROM` em todas as tabelas (em ordem reversa, fatos primeiro) para garantir que o pipeline possa ser re-executado sem duplicar dados.
      * **Carga:** Lê todos os arquivos CSV da pasta `dados/processados/` e os carrega para as tabelas correspondentes no MySQL usando `pandas.to_sql`.
      * **Transação:** Gerencia a conexão e garante que todas as cargas sejam "commitadas" juntas no final, ou revertidas (rollback) em caso de erro.

## 📂 Estrutura de Diretórios Esperada

Para que o pipeline funcione, os arquivos devem estar organizados da seguinte forma:

```
/seu_projeto/
├── configs/
│   └── db_config.yml           # <-- VOCÊ PRECISA CRIAR
├── dados/
│   ├── brutos/
│   │   ├── dengue/
│   │   │   └── DENGBR20.csv      # (Exemplo de dado bruto)
│   │   │   └── ...
│   │   ├── meteorologico/
│   │   │   └── apenas_capitais/
│   │   │       └── INMET_...csv  # (Exemplo de dado bruto)
│   │   │       └── ...
│   │   ├── local/
│   │   │   └── RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.xls
│   │   └── br_mdr_snis_municipio_agua_esgoto.csv
│   │
│   └── processados/
│       ├── (arquivos .csv serão gerados aqui)
│
├── dw_schema/
│   └── create_dw.sql           # <-- VOCÊ PRECISA CRIAR
│
├── create_tables.py
├── cria_dimensoes.py
├── etl_clima.py
├── etl_dengue.py
├── etl_socioeconomico.py
├── load.py
├── run_pipeline.py
├── README.md
└── requirements.txt            # <-- Crie com o conteúdo abaixo
```

## 🚀 Como Executar

### 1\. Pré-requisitos

  * Python 3.8+
  * Um servidor MySQL (local ou em nuvem, ex: Docker).
  * Os arquivos de dados brutos listados acima, nas pastas corretas.

### 2\. Arquivos de Configuração

**`requirements.txt`**
Crie este arquivo na raiz do projeto:

```
pandas
numpy
SQLAlchemy
PyMySQL
PyYAML
openpyxl  # Necessário para ler o .xls do IBGE
```

**`configs/db_config.yml`**
Crie a pasta `configs` e o arquivo `db_config.yml` com suas credenciais:

```yaml
mysql:
  host: "localhost"     # ou o IP do seu container/servidor
  port: 3306
  user: "root"
  password: "sua_senha_segura"
  database: "dw_dengue"   # O banco de dados que será criado
```

**`dw_schema/create_dw.sql`**
Crie a pasta `dw_schema` e o arquivo `create_dw.sql` com o DDL (Data Definition Language) para criar seu banco e suas tabelas.

*Exemplo mínimo:*

```sql
-- Garante que o banco de dados e o schema sejam criados
CREATE DATABASE IF NOT EXISTS dw_dengue CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE dw_dengue;

-- (Cole aqui os comandos CREATE TABLE para dim_local, dim_tempo, e todas as fatos)
-- Ex:
CREATE TABLE IF NOT EXISTS dim_local (
    id_local INT PRIMARY KEY,
    uf VARCHAR(2),
    cod_municipio VARCHAR(7),
    nome_municipio VARCHAR(100)
);

-- ... (etc. para todas as outras tabelas) ...
```

### 3\. Instalação e Execução

1.  **Instale as dependências:**

    ```bash
    pip install -r requirements.txt
    ```

2.  **Execute o pipeline mestre:**
    O orquestrador `run_pipeline.py` cuidará de executar todos os scripts na ordem correta.

    ```bash
    python run_pipeline.py
    ```

Se tudo estiver configurado corretamente, o script executará todos os 6 passos, imprimindo o progresso no console, e ao final seus dados estarão carregados no Data Warehouse MySQL.