"""
Pipeline de Criação de Dimensões (Tempo e Local)

Este script é responsável por gerar e persistir as tabelas de dimensão
usadas nos pipelines de ETL.

1.  Cria a 'dim_tempo' baseada num intervalo de anos.
2.  Cria a 'dim_local' a partir de um arquivo do IBGE, filtrando e
    tratando ambiguidades para manter apenas as 27 capitais.
3.  Salva ambas as dimensões como arquivos CSV na pasta 'processados'.
"""

import os
import pandas as pd
import numpy as np

# =============================================================================
# 1. CONFIGURAÇÃO E CONSTANTES
# =============================================================================

# --- Caminhos (Paths) ---
CAMINHO_BASE = 'dados'
PATH_PROCESSADOS = os.path.join(CAMINHO_BASE, 'processados')
PATH_BRUTOS = os.path.join(CAMINHO_BASE, 'brutos')

# Caminho de entrada do arquivo XLS do IBGE
#Usamos o ficheiro de 2022 como fonte para os nomes e códigos das capitais

PATH_BRUTOS_LOCAL = os.path.join(PATH_BRUTOS, 'local') 
PATH_FONTE_LOCAL = os.path.join(PATH_BRUTOS_LOCAL, 'AR_BR_RG_UF_RGINT_MES_MIC_MUN_2022.xls')

# Caminhos de saída para as dimensões
PATH_DIM_TEMPO_SAIDA = os.path.join(PATH_PROCESSADOS, 'dim_tempo.csv')
PATH_DIM_LOCAL_SAIDA = os.path.join(PATH_PROCESSADOS, 'dim_local.csv')

# --- Configurações da Dim_Tempo ---
ANO_INICIO = 2017
ANO_FIM = 2022 

# --- Configurações da Dim_Local ---
# Lista das 27 capitais que queremos manter
LISTA_CAPITAIS = [
    'Aracaju', 'Belém', 'Belo Horizonte', 'Boa Vista', 'Brasília',
    'Campo Grande', 'Cuiabá', 'Curitiba', 'Florianópolis', 'Fortaleza',
    'Goiânia', 'João Pessoa', 'Macapá', 'Maceió', 'Manaus', 'Natal',
    'Palmas', 'Porto Alegre', 'Porto Velho', 'Recife', 'Rio Branco',
    'Rio de Janeiro', 'Salvador', 'São Luís', 'São Paulo', 'Teresina',
    'Vitória'
]

# Colunas que queremos extrair do arquivo do IBGE
COLUNAS_IBGE_RAW = [
    'NM_UF',
    'CD_MUN',
    'NM_MUN'
]

# Renomeia colunas para o padrão final do DW
MAPA_RENOMEAR_LOCAL = {
    'NM_UF': 'uf',
    'NM_MUN': 'nome_municipio',
    'CD_MUN': 'cod_municipio' 
}

# REGRA DE NEGÓCIO: Resolve nomes de capitais duplicados
# (ex: "Belém" existe no Pará e em Alagoas)
MAPA_CAPITAIS_AMBIGUAS = {
    'Belém': 'Pará',
    'Boa Vista': 'Roraima',
    'Campo Grande': 'Mato Grosso do Sul',
    'Palmas': 'Tocantins',
    'Rio Branco': 'Acre'
}

# =============================================================================
# ETAPA 1: CRIAÇÃO DA DIMENSÃO TEMPO
# =============================================================================

def criar_dimensao_tempo(ano_inicio, ano_fim):
    """
    Cria um DataFrame de dimensão de tempo com granularidade diária,
    incluindo ano/semana civil e epidemiológica.
    """
    print(f"\n--- Iniciando criação da dim_tempo ({ano_inicio}-{ano_fim}) ---")
    
    # 1. Criar o range de datas completo (ex: 2017-01-01 até 2022-12-31)
    datas = pd.date_range(start=f'{ano_inicio}-01-01', end=f'{ano_fim}-12-31')
    dim_tempo = pd.DataFrame({'data_completa': datas})

    # 2. Extrair atributos de data civil (Ano, Mês, Dia)
    dim_tempo['ano'] = dim_tempo['data_completa'].dt.year
    dim_tempo['mes'] = dim_tempo['data_completa'].dt.month
    dim_tempo['dia'] = dim_tempo['data_completa'].dt.day

    # 3. Calcular atributos epidemiológicos (Ano e Semana)
    # A semana epidemiológica começa no Domingo. '%U' faz esse cálculo.
    semana_ano_atual = dim_tempo['data_completa'].dt.strftime('%U').astype(int)
    
    # Identifica dias no início do ano que pertencem à semana 0
    eh_semana_zero = (semana_ano_atual == 0)

    # Se for semana 0, o ano epidemiológico é o ano anterior
    dim_tempo['ano_epidemiologico'] = np.where(
        eh_semana_zero,
        dim_tempo['ano'] - 1,  # Caso Verdadeiro
        dim_tempo['ano']       # Caso Falso
    )

    # Se for semana 0, descobre qual era a última semana (52 ou 53) do ano anterior.
    ultima_semana_ano_anterior = pd.to_datetime(
        dim_tempo['ano_epidemiologico'].astype(str) + '-12-31'
    ).dt.strftime('%U').astype(int)
    
    dim_tempo['semana_epidemiologica'] = np.where(
        eh_semana_zero,
        ultima_semana_ano_anterior, # Caso Verdadeiro
        semana_ano_atual            # Caso Falso
    )

    # 4. Criar a Chave Primária (PK)
    dim_tempo['id_tempo'] = dim_tempo.index + 1

    # 5. Reordenar colunas
    colunas_ordenadas = [
        'id_tempo', 'data_completa', 'ano', 'mes', 'dia',
        'ano_epidemiologico', 'semana_epidemiologica'
    ]
    dim_tempo = dim_tempo[colunas_ordenadas]
    
    print(f"dim_tempo criada com {len(dim_tempo)} linhas.")
    return dim_tempo


# =============================================================================
# ETAPA 2: CRIAÇÃO DA DIMENSÃO LOCAL
# =============================================================================

def _resolver_ambiguidade(row, mapa_ambiguas):
    """
    Função auxiliar para aplicar a regra de negócio de desambiguação.
    Retorna True se a linha for a capital correta, False caso contrário.
    """
    nome_municipio = row['NM_MUN']
    nome_uf = row['NM_UF']
    
    # Se o município não está no mapa, ele não é ambíguo. É uma capital.
    if nome_municipio not in mapa_ambiguas:
        return True
    
    # Se ESTÁ no mapa, verificamos se a UF é a correta
    # (ex: É 'Belém' E 'Pará'?)
    return nome_uf == mapa_ambiguas[nome_municipio]


def criar_dimensao_local(raw_file_path, lista_capitais, colunas_raw, mapa_ambiguas, mapa_rename):
    """
    Cria um DataFrame de dimensão de local, focado apenas nas 27 capitais.
    (Versão SIMPLIFICADA, lendo de um CSV de Área)
    """
    print(f"Iniciando criação da dim_local a partir de: {raw_file_path}")
    
    try:
        # 1. Extrair dados brutos (lendo o CSV)
        df_ibge_raw = pd.read_excel(raw_file_path)
    except FileNotFoundError:
        print(f"ERRO: Arquivo CSV do IBGE não encontrado em: {raw_file_path}")
        raise
    except Exception as e:
        print(f"ERRO ao ler o arquivo CSV: {e}")
        raise

    # 2. Selecionar apenas as colunas de interesse
    df_local = df_ibge_raw[colunas_raw].copy()

    # 3. Primeiro filtro: Manter apenas linhas cujo nome está na lista de capitais
    df_filtrado = df_local[df_local['NM_MUN'].isin(lista_capitais)].copy()
    print(f"Arquivo IBGE filtrado, {len(df_filtrado)} linhas de capitais (incluindo homónimas).")

    # 4. Segundo filtro: Resolver ambiguidades
    df_filtrado['is_capital_real'] = df_filtrado.apply(
        _resolver_ambiguidade, 
        axis=1, 
        mapa_ambiguas=mapa_ambiguas
    )
    
    df_final_capitais = df_filtrado[df_filtrado['is_capital_real'] == True].copy()
    
    # Verificação de segurança
    if len(df_final_capitais) != 27:
        print(f"ATENÇÃO! Esperava-se 27 capitais, mas {len(df_final_capitais) } foram encontradas.")
    else:
        print("Desambiguação concluída. 27 capitais únicas isoladas.")

    # 5. Renomear colunas para o padrão do DW
    df_final_capitais.rename(columns=mapa_rename, inplace=True)

    # Converte a coluna 'cod_municipio' (que é float) para INT (para remover o ".0")
    # e DEPOIS para STRING, para que seja salva como texto "1100015".
    df_final_capitais['cod_municipio'] = pd.to_numeric(
        df_final_capitais['cod_municipio'], 
        errors='coerce' # Ignora se houver algum erro de texto
    ).fillna(0).astype(int).astype(str)

    # 6. Criar a Chave Primária (PK)
    df_final_capitais.sort_values('nome_municipio', inplace=True)
    df_final_capitais.reset_index(drop=True, inplace=True)
    df_final_capitais['id_local'] = df_final_capitais.index + 1

    # 7. Reordenar colunas para o schema final
    colunas_finais_local = ['id_local', 'uf', 'cod_municipio', 'nome_municipio']
    dim_local = df_final_capitais[colunas_finais_local]
    
    return dim_local


# =============================================================================
# ETAPA 3: CARGA (LOAD) / PERSISTÊNCIA
# =============================================================================

def salvar_csv(df, output_path):
    """Salva o DataFrame final em um arquivo CSV."""
    print(f"\nA salvar dados em: {output_path}")
    try:
        # index=False evita salvar o índice do pandas no arquivo
        df.to_csv(output_path, index=False, sep=';')
        print(f"--- SUCESSO! ---")
        print(f"'{os.path.basename(output_path)}' salvo com {len(df)} linhas.")
    except Exception as e:
        print(f"\n--- ERRO AO SALVAR O CSV: {e} ---")
        raise


# =============================================================================
# ORQUESTRADOR PRINCIPAL (MAIN)
# =============================================================================

def main():

    print("========= INICIANDO PIPELINE DE CRIAÇÃO DE DIMENSÕES =========")
    
   
    # 1. Processar e Salvar Dimensão Tempo
    try:
        dim_tempo = criar_dimensao_tempo(ANO_INICIO, ANO_FIM)
        salvar_csv(dim_tempo, PATH_DIM_TEMPO_SAIDA)
    except Exception as e:
        print(f"Falha ao processar Dimensão Tempo: {e}")
        return # Interrompe

    # 2. Processar e Salvar Dimensão Local
    try:
        dim_local = criar_dimensao_local(
            PATH_FONTE_LOCAL, # O novo caminho do CSV
            LISTA_CAPITAIS,
            COLUNAS_IBGE_RAW,
            MAPA_CAPITAIS_AMBIGUAS,
            MAPA_RENOMEAR_LOCAL
        )
        salvar_csv(dim_local, PATH_DIM_LOCAL_SAIDA)
    except Exception as e:
        print(f"Falha ao processar Dimensão Local: {e}")
        return # Interrompe
    
    print("\n========= PIPELINE DE DIMENSÕES CONCLUÍDO =========")


if __name__ == "__main__":
    main()