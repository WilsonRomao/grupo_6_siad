# -*- coding: utf-8 -*-
"""
Pipeline de E-T (Extract-Transform) Simplificado para Fato Socioeconômico.

VERSÃO ATUALIZADA (v4 - GitHub):
- Carrega 3 fontes de dados brutos:
  1. SNIS (Água/Esgoto) - CSV
  2. IBGE População (Histórico) - CSV
  3. IBGE Áreas Territoriais (Múltiplos ficheiros .XLS anuais)
- Combina os ficheiros de área anuais num único DataFrame.
- Usa o código de município de 7 dígitos (IBGE) como chave.
- Calcula a densidade demográfica.
"""

import os
import re
import pandas as pd
import numpy as np

# Bloco do Google Colab REMOVIDO

# =============================================================================
# 1. CONFIGURAÇÃO E CONSTANTES
# =============================================================================

# --- Caminhos (Paths) ---
BASE_PATH = 'dados'
PATH_PROCESSADOS = os.path.join(BASE_PATH, 'processados')
PATH_BRUTOS = os.path.join(BASE_PATH, 'brutos')
PATH_BRUTOS_LOCAL = os.path.join(PATH_BRUTOS, 'local')

CAMINHOS_ETL = {
    # 3 Fontes de Fatos Brutos
    'bruto_snis': os.path.join(PATH_BRUTOS_LOCAL, 'br_mdr_snis_municipio_agua_esgoto.csv'),
    'bruto_populacao': os.path.join(PATH_BRUTOS_LOCAL, 'br_ibge_populacao_municipio.csv'),
    
    # 2 Dimensões
    'dim_local': os.path.join(PATH_PROCESSADOS, 'dim_local.csv'),
    'dim_tempo': os.path.join(PATH_PROCESSADOS, 'dim_tempo.csv'),
    
    # 1 Saída
    'saida_fato_csv': os.path.join(PATH_PROCESSADOS, 'fato_socioeconomico.csv')
}

# --- Lista dos Ficheiros de Área (.XLS) ---
MAPA_FICHEIROS_AREA = {
    2017: {
        'path': os.path.join(PATH_BRUTOS_LOCAL, 'AR_BR_RG_UF_MES_MIC_MUN_2017.xls'),
        'sheet_name': 'AR_BR_MUN_2017',
        'col_id': 'CD_GCMUN',
        'col_area': 'AR_MUN_2017'
    },
    2018: {
        'path': os.path.join(PATH_BRUTOS_LOCAL, 'AR_BR_RG_UF_MES_MIC_MUN_2018.xls'),
        'sheet_name': 'AR_BR_MUN_2018',
        'col_id': 'CD_GCMUN',
        'col_area': 'AR_MUN_2018'
    },
    2019: {
        'path': os.path.join(PATH_BRUTOS_LOCAL, 'AR_BR_RG_UF_RGINT_RGIM_MES_MIC_MUN_2019.xls'),
        'sheet_name': 'AR_BR_MUN_2019',
        'col_id': 'CD_GCMUN',
        'col_area': 'AR_MUN_2019'
    },
    2020: {
        'path': os.path.join(PATH_BRUTOS_LOCAL, 'AR_BR_RG_UF_RGINT_RGIM_MES_MIC_MUN_2020.xls'),
        'sheet_name': 'AR_BR_MUN_2020',
        'col_id': 'CD_GCMUN',
        'col_area': 'AR_MUN_2020'
    },
    2021: {
        'path': os.path.join(PATH_BRUTOS_LOCAL, 'AR_BR_RG_UF_RGINT_RGIM_MES_MIC_MUN_2021.xls'),
        'sheet_name': 'AR_BR_MUN_2021',
        'col_id': 'CD_MUN',
        'col_area': 'AR_MUN_2021'
    },
    2022: {
        'path': os.path.join(PATH_BRUTOS_LOCAL, 'AR_BR_RG_UF_RGINT_MES_MIC_MUN_2022.xls'),
        'sheet_name': 'AR_BR_MUN_2022',
        'col_id': 'CD_MUN',
        'col_area': 'AR_MUN_2022'
    }
}


# --- Definições de Schema ---
SCHEMA_SNIS_COLS = [
    'ano', 'id_municipio', 'populacao_atendida_agua', 
    'populacao_atentida_esgoto', 'populacao_urbana'
]
SCHEMA_POPULACAO_COLS = ['ano', 'id_municipio', 'populacao']

SCHEMA_DIM_LOCAL_COLS = ['id_local', 'cod_municipio']
SCHEMA_DIM_TEMPO_COLS = ['id_tempo', 'ano', 'mes', 'dia']

# --- Constantes de Transformação ---
ANO_FILTRO_INICIAL = 2017
REGRA_TEMPO_ANUAL = {'mes': 1, 'dia': 1}

# Mapeamento final
COLUNAS_FATO_RENAME_MAP = {
    'id_local': 'id_local',
    'id_tempo': 'id_tempo',
    'populacao': 'num_populacao',
    'area_km2': 'area_territorio',
    'densidade_demografica': 'densidade_demografica',
    'populacao_atendida_agua': 'num_agua_tratada',
    'populacao_atentida_esgoto': 'num_esgoto'
}

COLUNAS_FATO_FINAL = [
    'id_local', 'id_tempo', 'num_populacao', 'area_territorio', 
    'densidade_demografica', 'num_esgoto', 'num_agua_tratada'
]

# Função 'mount_google_drive' REMOVIDA

# =============================================================================
# ETAPA DE EXTRAÇÃO (EXTRACT)
# =============================================================================

def extrair_csv(file_path, usecols, sep=','):
    """Lê um arquivo CSV."""
    print(f"A ler dados de: {os.path.basename(file_path)}")
    try:
        df = pd.read_csv(file_path, sep=sep, usecols=usecols)
        print(f"Lidas {len(df)} linhas.")
        return df
    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado em: {file_path}")
        raise
    except Exception as e:
        print(f"ERRO ao ler {file_path}: {e}")
        raise

def _carregar_e_combinar_areas_historicas(mapa_ficheiros):
    """
    Lê múltiplos ficheiros de área .XLS anuais, normaliza-os e 
    combina-os num único DataFrame histórico (ano, id_municipio, area_km2).
    """
    print("\nIniciando combinação dos ficheiros de área territorial (.xls)...")
    lista_dfs_area = []
    
    for ano, info in mapa_ficheiros.items():
        try:
            path = info['path']
            sheet = info['sheet_name']
            col_id = info['col_id']
            col_area = info['col_area']
            
            print(f"  A ler ficheiro de {ano} (Aba: {sheet})...")
            
            df_ano = pd.read_excel(
                path,
                sheet_name=sheet,
                usecols=[col_id, col_area]
            )
            
            df_ano.rename(columns={
                col_id: 'id_municipio',
                col_area: 'area_km2'
            }, inplace=True)
            
            df_ano['ano'] = ano
            
            lista_dfs_area.append(df_ano)
            
        except FileNotFoundError:
            print(f"  AVISO: Ficheiro de área de {ano} não encontrado em: {path}. A saltar este ano.")
        except Exception as e:
            print(f"  AVISO: Erro ao ler o ficheiro de {ano} (Aba: {sheet}). Erro: {e}.")
            print("         Verifique se 'xlrd' está instalado (pip install xlrd) e se o nome da aba está correto.")
            
    if not lista_dfs_area:
        print("ERRO: Nenhum ficheiro de área foi lido com sucesso.")
        raise FileNotFoundError("Nenhum dado de área territorial foi carregado.")

    df_area_historica = pd.concat(lista_dfs_area, ignore_index=True)
    
    print(f"Combinação de áreas concluída. {len(df_area_historica)} registos históricos criados.")
    return df_area_historica

# =============================================================================
# ETAPA DE TRANSFORMAÇÃO (TRANSFORM)
# =============================================================================

def _preparar_chave(df, col_int):
    """Garante que a coluna (ano ou id_municipio) é do tipo INT."""
    df[col_int] = pd.to_numeric(df[col_int], errors='coerce')
    df.dropna(subset=[col_int], inplace=True) 
    df[col_int] = df[col_int].astype(int)
    return df

def transformar_dados_socioeconomicos(df_snis, df_populacao, df_area, dim_local, dim_tempo):
    """
    Função consolidada que executa todas as etapas de transformação.
    """
    print("\n--- INICIANDO ETAPA DE TRANSFORMAÇÃO ---")
    
    # --- 1. Preparar Chaves de Junção (ANO e ID_MUNICIPIO) ---
    print("Preparando chaves de junção (ano, id_municipio de 7 dígitos)...")
    df_snis = _preparar_chave(df_snis, 'ano')
    df_snis = _preparar_chave(df_snis, 'id_municipio')
    
    df_populacao = _preparar_chave(df_populacao, 'ano')
    df_populacao = _preparar_chave(df_populacao, 'id_municipio')
    
    df_area = _preparar_chave(df_area, 'ano')
    df_area = _preparar_chave(df_area, 'id_municipio')

    # --- 2. Juntar (Merge) os Fatos Brutos ---
    print("Juntando datasets brutos (SNIS, População, Área)...")
    
    df_fatos = pd.merge(
        df_snis,
        df_populacao,
        on=['ano', 'id_municipio'],
        how='left'
    )
    
    df_fatos = pd.merge(
        df_fatos,
        df_area,
        on=['ano', 'id_municipio'],
        how='left'
    )
    
    print(f"Merge de fatos brutos concluído. {len(df_fatos)} linhas.")

    # --- 3. Preparar Dimensões ---
    print("Preparando dimensões (local e tempo)...")
    
    dim_local['cod_municipio'] = pd.to_numeric(dim_local['cod_municipio'], errors='coerce').fillna(0).astype(int)
    ids_capitais = dim_local['cod_municipio'].unique()

    regra = REGRA_TEMPO_ANUAL
    dim_tempo_anual = dim_tempo[
        (dim_tempo['mes'] == regra['mes']) & (dim_tempo['dia'] == regra['dia'])
    ].copy()
    dim_tempo_anual = dim_tempo_anual[['id_tempo', 'ano']]
    dim_tempo_anual['ano'] = dim_tempo_anual['ano'].astype(int)

    # --- 4. Filtrar Dados Juntos ---
    print("Filtrando dados (Ano e Capitais)...")
    
    df_filtrado_ano = df_fatos[df_fatos['ano'] >= ANO_FILTRO_INICIAL].copy()
    
    df_filtrado = df_filtrado_ano[
        df_filtrado_ano['id_municipio'].isin(ids_capitais)
    ].copy()
    print(f"Registos após filtros: {len(df_filtrado)}")

    # --- 5. Calcular Novas Métricas ---
    print("Calculando Densidade Demográfica...")
    
    df_filtrado['populacao'] = df_filtrado['populacao'].fillna(0)
    df_filtrado['area_km2'] = df_filtrado['area_km2'].fillna(0)

    df_filtrado['densidade_demografica'] = df_filtrado.apply(
        lambda row: row['populacao'] / row['area_km2'] if row['area_km2'] > 0 else 0,
        axis=1
    ).round(2)

    # --- 6. Limpar Nulos (das métricas restantes) ---
    print("Tratando valores nulos (NaN -> 0) nas métricas...")
    df_filtrado['populacao_atendida_agua'] = df_filtrado['populacao_atendida_agua'].fillna(0).astype(int)
    df_filtrado['populacao_atentida_esgoto'] = df_filtrado['populacao_atentida_esgoto'].fillna(0).astype(int)
    df_filtrado['populacao'] = df_filtrado['populacao'].astype(int)
    
    # --- 7. Mapear Dimensões (Merge Final) ---
    print("Mapeando Foreign Keys (id_local, id_tempo)...")
    
    df_com_local = pd.merge(
        df_filtrado,
        dim_local[['id_local', 'cod_municipio']],
        left_on='id_municipio', 
        right_on='cod_municipio',
        how='left' 
    )
    
    df_com_chaves = pd.merge(
        df_com_local,
        dim_tempo_anual,
        on='ano',
        how='left'
    )
    
    df_final = df_com_chaves.dropna(subset=['id_local', 'id_tempo'])
    print(f"Mapeamento concluído. {len(df_final)} registos válidos.")

    # --- 8. Finalizar Schema da Fato ---
    print("A renomear e selecionar colunas finais...")
    df_renomeado = df_final.rename(columns=COLUNAS_FATO_RENAME_MAP)
    fato_socioeconomico = df_renomeado[COLUNAS_FATO_FINAL]
    
    print("--- TRANSFORMAÇÃO CONCLUÍDA ---")
    return fato_socioeconomico

# =============================================================================
# ETAPA DE CARGA (LOAD)
# =============================================================================

def salvar_csv(df, output_path):
    """Salva o DataFrame final (tabela Fato) em um arquivo CSV."""
    print("\n--- INICIANDO ETAPA DE LOAD (Salvando CSV) ---")
    
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
    
    # Chamada ao 'mount_google_drive' REMOVIDA
    
    # 1. ETAPA DE EXTRAÇÃO
    try:
        # Fatos Brutos (CSV)
        df_snis = extrair_csv(
            CAMINHOS_ETL['bruto_snis'],
            SCHEMA_SNIS_COLS
        )
        df_populacao = extrair_csv(
            CAMINHOS_ETL['bruto_populacao'],
            SCHEMA_POPULACAO_COLS
        )
        
        # Fato Bruto (XLS Combinados)
        df_area = _carregar_e_combinar_areas_historicas(MAPA_FICHEIROS_AREA)
        
        # Dimensões (CSV)
        dim_local = extrair_csv(
            CAMINHOS_ETL['dim_local'],
            SCHEMA_DIM_LOCAL_COLS,
            sep=';'
        )
        dim_tempo = extrair_csv(
            CAMINHOS_ETL['dim_tempo'],
            SCHEMA_DIM_TEMPO_COLS,
            sep=';'
        )
    except Exception as e:
        print(f"ERRO na Extração. Pipeline interrompido: {e}")
        return
    
    # 2. ETAPA DE TRANSFORMAÇÃO
    fato_final = transformar_dados_socioeconomicos(
        df_snis,
        df_populacao,
        df_area,
        dim_local,
        dim_tempo
    )
    
    # 3. ETAPA DE CARGA (Salvar CSV)
    salvar_csv(fato_final, CAMINHOS_ETL['saida_fato_csv'])
    
    print("\n========= PIPELINE ETL Socioeconomico CONCLUÍDO =========")


if __name__ == "__main__":
    main()