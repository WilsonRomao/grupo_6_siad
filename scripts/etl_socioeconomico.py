# -*- coding: utf-8 -*-
"""
Pipeline de E-T (Extract-Transform) Simplificado para Fato Socioeconômico.

Este script executa o processo de E-T:
1. Monta o Google Drive.
2. Extrai dados brutos do SNIS.
3. Carrega as dimensões 'local' e 'tempo'.
4. Executa uma transformação completa numa única função.
5. Salva o DataFrame agregado final (fato) como um CSV.
"""

import os
import pandas as pd

# Tenta importar o 'drive' do Colab
try:
    from google.colab import drive
except ImportError:
    print("Aviso: Não estamos no Google Colab. A montagem do Drive será ignorada.")
    drive = None

# =============================================================================
# 1. CONFIGURAÇÃO E CONSTANTES
# =============================================================================

# --- Caminhos (Paths) ---
BASE_PATH = 'dados'
PATH_PROCESSADOS = os.path.join(BASE_PATH, 'processados')
PATH_BRUTOS = os.path.join(BASE_PATH, 'brutos')

CAMINHOS_ETL = {
    'bruto_snis': os.path.join(PATH_BRUTOS, 'br_mdr_snis_municipio_agua_esgoto.csv'),
    'dim_local': os.path.join(PATH_PROCESSADOS, 'dim_local.csv'),
    'dim_tempo': os.path.join(PATH_PROCESSADOS, 'dim_tempo.csv'),
    'saida_fato_csv': os.path.join(PATH_PROCESSADOS, 'fato_socioeconomico.csv')
}

# --- Definições de Schema para Extração (Otimização) ---
SCHEMA_SNIS_COLS = [
    'ano', 'id_municipio', 'sigla_uf', 
    'populacao_atendida_agua', 'populacao_atentida_esgoto', 'populacao_urbana'
]
SCHEMA_DIM_LOCAL_COLS = ['id_local', 'cod_municipio']
SCHEMA_DIM_TEMPO_COLS = ['id_tempo', 'ano', 'mes', 'dia']

# --- Constantes de Transformação ---
ANO_FILTRO_INICIAL = 2017

COLUNAS_METRICAS_PARA_LIMPAR = [
    'populacao_atendida_agua', 
    'populacao_atentida_esgoto', 
    'populacao_urbana'
]

# Regra de negócio: Fato Anual (usar o dia 1º de Janeiro)
REGRA_TEMPO_ANUAL = {'mes': 1, 'dia': 1}

# Mapeamento final dos nomes das colunas
COLUNAS_FATO_RENAME_MAP = {
    'id_local': 'id_local',
    'id_tempo': 'id_tempo',
    'populacao_atendida_agua': 'num_agua_tratada',
    'populacao_urbana': 'num_populacao',
    'populacao_atentida_esgoto': 'num_esgoto'
}
# Colunas finais que devem estar no CSV
COLUNAS_FATO_FINAL = [
    'id_local', 'id_tempo', 'num_agua_tratada', 'num_populacao', 
    'num_esgoto'
]

# =============================================================================
# FUNÇÃO AUXILIAR - AMBIENTE COLAB
# =============================================================================

def mount_google_drive():
    """Monta o Google Drive se o script estiver rodando no Google Colab."""
    if drive:
        try:
            drive.mount('/content/drive')
            print("Google Drive montado com sucesso.")
        except Exception as e:
            print(f"Erro ao montar o Google Drive: {e}")
    else:
        print("Skipping Google Drive mount (não estamos no Colab).")

# =============================================================================
# ETAPA DE EXTRAÇÃO (EXTRACT)
# =============================================================================

def extrair_dados_brutos(file_path, usecols):
    """Lê o arquivo CSV principal de dados brutos (SNIS)."""
    print(f"\n--- INICIANDO [E]XTRAÇÃO ---")
    print(f"A ler dados brutos: {file_path}")
    try:
        df = pd.read_csv(file_path, sep=',', usecols=usecols)
        print(f"Dados brutos lidos com sucesso ({len(df)} linhas).")
        return df
    except FileNotFoundError:
        print(f"ERRO: Arquivo de dados brutos não encontrado em: {file_path}")
        raise
    except Exception as e:
        print(f"ERRO ao ler dados brutos {file_path}: {e}")
        raise

def carregar_dimensao(file_path, usecols):
    """Função genérica para carregar um arquivo CSV de dimensão."""
    try:
        df = pd.read_csv(file_path, sep=';', usecols=usecols)
        print(f"Dimensão '{os.path.basename(file_path)}' carregada com {len(df)} linhas.")
        return df
    except FileNotFoundError:
        print(f"ERRO: Arquivo de dimensão não encontrado em: {file_path}")
        raise
    except Exception as e:
        print(f"ERRO ao carregar dimensão {file_path}: {e}")
        raise

# =============================================================================
# ETAPA DE TRANSFORMAÇÃO (TRANSFORM)
# =============================================================================

def transformar_dados_socioeconomicos(df_bruto, dim_local, dim_tempo):
    """
    Função consolidada que executa todas as etapas de transformação:
    Limpeza, preparação de chaves, filtros e merges.
    """
    print("\n--- INICIANDO ETAPA DE [T]RANSFORMAÇÃO ---")
    
    # --- 1. Preparar Dimensões ---
    print("Preparando dimensões (local e tempo)...")
    
    # 1.1. Preparar dim_local: Criar chave de 6 dígitos
    # O SNIS usa 6 dígitos (sem DV), a dim_local tem 7 (com DV)
    dim_local_clean = dim_local.copy()
    dim_local_clean['cod_municipio_6dig'] = dim_local['cod_municipio'].astype(str).str[:-1].astype(int)

    # 1.2. Preparar dim_tempo: Aplicar regra de negócio anual
    # Esta Fato é anual, então só queremos os IDs de 1º de Janeiro de cada ano.
    regra = REGRA_TEMPO_ANUAL
    dim_tempo_anual = dim_tempo[
        (dim_tempo['mes'] == regra['mes']) & (dim_tempo['dia'] == regra['dia'])
    ].copy()
    dim_tempo_anual = dim_tempo_anual[['id_tempo', 'ano']]
    dim_tempo_anual['ano'] = dim_tempo_anual['ano'].astype(int)
    print(f"Regra de tempo (anual) aplicada. {len(dim_tempo_anual)} chaves de ano encontradas.")

    # --- 2. Ajustar e Filtrar Dados Brutos ---
    print("Ajustando e filtrando dados brutos...")
    df = df_bruto.copy()
    
    # 2.1. Criar chave de 6 dígitos nos dados brutos (para o merge)
    df['id_municipio_6dig'] = df['id_municipio'].astype(str).str[:-1].astype(int)
    
    # 2.2. Filtrar por Ano (>= 2017)
    df_filtrado_ano = df[df['ano'] >= ANO_FILTRO_INICIAL].copy()
    
    # 2.3. Filtrar por Município (só manter os que estão na nossa dim_local)
    municipios_validos = dim_local_clean['cod_municipio_6dig'].unique()
    df_filtrado = df_filtrado_ano[
        df_filtrado_ano['id_municipio_6dig'].isin(municipios_validos)
    ].copy()
    print(f"Registos após filtros iniciais (ano e local): {len(df_filtrado)}")

    # --- 3. Limpar Nulos ---
    print("A tratar valores nulos (NaN -> 0) nas métricas...")
    for col in COLUNAS_METRICAS_PARA_LIMPAR:
        if col in df_filtrado.columns:
            df_filtrado[col] = df_filtrado[col].fillna(0).astype(int)
    
    df_limpo = df_filtrado # Renomeia para clareza

    # --- 4. Mapear Dimensões (Merge) ---
    print("Mapeando dimensões (buscando Foreign Keys)...")
    
    # 4.1. Merge com Dim_Local (usando a chave de 6 dígitos)
    df_com_local = pd.merge(
        df_limpo,
        dim_local_clean[['id_local', 'cod_municipio_6dig']],
        left_on='id_municipio_6dig',
        right_on='cod_municipio_6dig',
        how='left' 
    )
    
    # 4.2. Merge com Dim_Tempo (usando a chave 'ano')
    df_com_local['ano'] = df_com_local['ano'].astype(int)
    df_com_chaves = pd.merge(
        df_com_local,
        dim_tempo_anual,
        on='ano', # Chave 'ano' existe em ambos
        how='left'
    )
    
    # 4.3. Limpar dados que não encontraram chaves
    df_final = df_com_chaves.dropna(subset=['id_local', 'id_tempo'])
    print(f"Mapeamento de dimensões concluído. {len(df_final)} registos válidos.")

    # --- 5. Finalizar Schema da Fato ---
    # (Não é preciso agregar/groupby, os dados já estão no nível ano/município)
    print("A renomear e selecionar colunas finais para a Fato...")
    
    df_renomeado = df_final.rename(columns=COLUNAS_FATO_RENAME_MAP)
    fato_socioeconomico = df_renomeado[COLUNAS_FATO_FINAL]
    
    print("--- [T]RANSFORMAÇÃO CONCLUÍDA ---")
    return fato_socioeconomico

# =============================================================================
# ETAPA DE CARGA (LOAD)
# =============================================================================

def salvar_csv(df, output_path):
    """Salva o DataFrame final (tabela Fato) em um arquivo CSV."""
    print("\n--- INICIANDO ETAPA DE [L]OAD (Salvando CSV) ---")
    
    if df is None or df.empty:
        print("AVISO: O DataFrame final está vazio. Nenhum arquivo CSV será salvo.")
        return

    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False, sep=';')
        
        print(f"\n--- SUCESSO! ---")
        print(f"Arquivo 'fato_socioeconomico.csv' ({len(df)} linhas) salvo em:")
        print(output_path)
    except Exception as e:
        print(f"\n--- ERRO AO SALVAR O CSV: {e} ---")
        raise

# =============================================================================
# ORQUESTRADOR PRINCIPAL (MAIN)
# =============================================================================

def main():
    """Função principal que orquestra todo o pipeline de E-T."""
    print("========= INICIANDO PIPELINE ETL Socioeconomico =========")
    
    mount_google_drive()
    
    # 1. ETAPA DE EXTRAÇÃO
    try:
        df_bruto = extrair_dados_brutos(
            CAMINHOS_ETL['bruto_snis'],
            SCHEMA_SNIS_COLS
        )
        dim_local = carregar_dimensao(
            CAMINHOS_ETL['dim_local'],
            SCHEMA_DIM_LOCAL_COLS
        )
        dim_tempo = carregar_dimensao(
            CAMINHOS_ETL['dim_tempo'],
            SCHEMA_DIM_TEMPO_COLS
        )
    except Exception as e:
        print(f"ERRO na Extração. Pipeline interrompido: {e}")
        return

    if df_bruto.empty:
        print("Pipeline interrompido: Nenhum dado bruto foi carregado.")
        return
    
    # 2. ETAPA DE TRANSFORMAÇÃO
    fato_final = transformar_dados_socioeconomicos(
        df_bruto,
        dim_local,
        dim_tempo
    )
    
    # 3. ETAPA DE CARGA (Salvar CSV)
    salvar_csv(fato_final, CAMINHOS_ETL['saida_fato_csv'])
    
    print("\n========= PIPELINE ETL Socioeconomico CONCLUÍDO =========")


if __name__ == "__main__":
    main()