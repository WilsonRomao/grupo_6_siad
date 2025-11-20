"""
Pipeline ETL para dados de Dengue.

Este script executa o processo completo de ETL para transformar dados brutos
de notificação de dengue em uma Tabela Fato agregada por Município e Semana.
"""

import os
import glob
import numpy as np
import pandas as pd

# =============================================================================
# 1. CONFIGURAÇÃO E CONSTANTES
# =============================================================================

BASE_PATH = 'dados'
PATH_BRUTOS = os.path.join(BASE_PATH, 'brutos', 'dengue', 'DENGBR*.csv')
PATH_PROCESSADOS = os.path.join(BASE_PATH, 'processados')

PATH_DIM_LOCAL = os.path.join(PATH_PROCESSADOS, 'dim_local.csv')
PATH_DIM_TEMPO = os.path.join(PATH_PROCESSADOS, 'dim_tempo.csv')
PATH_SAIDA_FATO = os.path.join(PATH_PROCESSADOS, 'fato_casos_dengue.csv')

COLUNAS_MASTER = [
    "ID_AGRAVO", "CLASSI_FIN", "ID_MN_RESI", "SG_UF", 
    "DT_NASC", "ANO_NASC", "CS_SEXO", "HOSPITALIZ", "EVOLUCAO",
    "DT_NOTIFIC", "SEM_NOT", "NU_IDADE_N"
]
DTYPES_MASTER = {col: 'str' for col in COLUNAS_MASTER}

COLUNAS_POS_EXTRACAO = [
    "DT_NOTIFIC", "ID_MN_RESI", "CS_SEXO", "HOSPITALIZ",
    "CLASSI_FIN", "EVOLUCAO", "DT_NASC", "ANO_NASC"
]

FILLNA_MAP = {
    'HOSPITALIZ': '9', 
    'EVOLUCAO': '9',    
    'CLASSI_FIN': '9',  
    'CS_SEXO': 'I'      
}

AGG_CRITERIA = {
    'CLASSI_FIN_EXCLUIR': ['2', '9'], 
    'EVOLUCAO_OBITO': '2', 
    'HOSPITALIZ_SIM': '1', 
    'SEXO_MASCULINO': 'M',
    'SEXO_FEMININO': 'F'
}

AGG_RENAMING_MAP = {
    'flag_casos': 'num_casos',
    'flag_obitos': 'num_obitos',
    'flag_masculino': 'num_masculino',
    'flag_feminino': 'num_feminino',
    'flag_criancas': 'num_criancas',
    'flag_adolescentes': 'num_adolescentes',
    'flag_adultos': 'num_adultos',
    'flag_idosos': 'num_idosos'
}


# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def carregar_csv(file_path, separador=';'):
    """Carrega um arquivo CSV, tipicamente usado para as tabelas de Dimensão."""
    try:
        df = pd.read_csv(file_path, sep=separador)
        print(f"Arquivo '{os.path.basename(file_path)}' carregado ({len(df)} linhas).")
        return df
    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado em: {file_path}")
        raise


# =============================================================================
# ETAPA DE EXTRAÇÃO (EXTRACT)
# =============================================================================

def extrair_dados_brutos_otimizado(file_pattern, codigos_filtro):
    """
    Lê múltiplos arquivos brutos de forma eficiente, aplicando o filtro de
    código de município (capitais) durante a leitura para economizar memória.
    Retorna um DataFrame único com os dados brutos filtrados.
    """
    print(f"\n--- INICIANDO EXTRAÇÃO: {file_pattern} ---")
    all_files = glob.glob(file_pattern)
    num_files = len(all_files)
    
    if num_files == 0:
        print("Aviso: Nenhum arquivo encontrado.")
        return pd.DataFrame()

    all_data = [] 

    for i, file in enumerate(all_files):
        try:
            print(f"  [{i+1}/{num_files}] Lendo e filtrando: {os.path.basename(file)}")
            
            df_header = pd.read_csv(file, nrows=0, dtype=DTYPES_MASTER, sep=',')
            cols_to_use = [col for col in COLUNAS_MASTER if col in df_header.columns]
            
            df = pd.read_csv(
                file, 
                usecols=cols_to_use, 
                dtype=DTYPES_MASTER, 
                sep=','
            )
            
            if 'DT_NASC' not in df.columns: df['DT_NASC'] = np.nan
            if 'ANO_NASC' not in df.columns: df['ANO_NASC'] = np.nan
            
            df_filtrado = df[df['ID_MN_RESI'].isin(codigos_filtro)]
            
            if not df_filtrado.empty:
                cols_final = [col for col in COLUNAS_POS_EXTRACAO if col in df_filtrado.columns]
                all_data.append(df_filtrado[cols_final])

        except Exception as e:
            print(f"  ERRO ao processar o arquivo {os.path.basename(file)}: {e}")

    if not all_data:
        print("Aviso: Nenhum dado foi extraído (ou filtro não encontrou dados).")
        return pd.DataFrame()
        
    print("Concatenando dados filtrados...")
    dengueDF = pd.concat(all_data, ignore_index=True)
    
    print(f"--- EXTRAÇÃO CONCLUÍDA ({len(dengueDF)} linhas) ---")
    return dengueDF


# =============================================================================
# ETAPA DE TRANSFORMAÇÃO (TRANSFORM)
# =============================================================================

def transformar_dados(df_bruto, dim_local, dim_tempo):
    """
    Prepara, limpa, enriquece e agrega os dados brutos em uma Tabela Fato semanal.
    """
    print("\n--- INICIANDO ETAPA DE TRANSFORMAÇÃO ---")
    df = df_bruto.copy()
    
    # 1. Preenchimento de Nulos e Conversão de Tipos
    print("Preenchendo nulos com códigos 'Ignorado'...")
    for coluna, valor in FILLNA_MAP.items():
        if coluna in df.columns:
             # Solução para FutureWarning: Atribuição direta.
             df[coluna] = df[coluna].fillna(valor)
    
    # 2. Harmonização da Idade
    print("Harmonizando datas e calculando idade...")
    df['DT_NOTIFIC_DT'] = pd.to_datetime(df['DT_NOTIFIC'], errors='coerce')
    df['DT_NASC_DT'] = pd.to_datetime(df['DT_NASC'], errors='coerce')
    df['ANO_NASC_INT'] = pd.to_numeric(df['ANO_NASC'], errors='coerce')

    idade_exata = (df['DT_NOTIFIC_DT'] - df['DT_NASC_DT']).dt.days / 365.25
    idade_aprox = df['DT_NOTIFIC_DT'].dt.year - df['ANO_NASC_INT']
    df['IDADE'] = idade_exata.fillna(idade_aprox).round().astype('Int64')
    
    df.dropna(subset=['IDADE', 'CS_SEXO'], inplace=True)
    
    # 3. Mapear Dimensões (Merge/Join)
    print("Mapeando dimensões (merge e obtenção de FKs)...")
    
    dim_local['cod_municipio_6dig'] = dim_local['cod_municipio'].astype(str).str[:-1]
    dim_tempo['data_completa'] = pd.to_datetime(dim_tempo['data_completa'], errors='coerce')
    
    df = df.merge(
        dim_tempo[['data_completa', 'id_tempo', 'ano_epidemiologico', 'semana_epidemiologica']],
        left_on='DT_NOTIFIC_DT',
        right_on='data_completa',
        how='left'
    )
    
    df = df.merge(
        dim_local[['cod_municipio_6dig', 'id_local']],
        left_on='ID_MN_RESI',
        right_on='cod_municipio_6dig',
        how='left'
    )
    
    # 4. Criar Flags (Colunas 0 ou 1)
    print("Criando colunas-flag para agregação...")
    
    df['CLASSI_FIN'] = df['CLASSI_FIN'].astype(str)
    df['EVOLUCAO'] = df['EVOLUCAO'].astype(str)
    df['HOSPITALIZ'] = df['HOSPITALIZ'].astype(str)
    
    df['flag_casos'] = np.where(df['CLASSI_FIN'].isin(AGG_CRITERIA['CLASSI_FIN_EXCLUIR']), 0, 1)
    df['flag_obitos'] = np.where(df['EVOLUCAO'] == AGG_CRITERIA['EVOLUCAO_OBITO'], 1, 0)
    df['flag_hospitalizacao'] = np.where(df['HOSPITALIZ'] == AGG_CRITERIA['HOSPITALIZ_SIM'], 1, 0)
    df['flag_masculino'] = np.where(df['CS_SEXO'] == AGG_CRITERIA['SEXO_MASCULINO'], 1, 0)
    df['flag_feminino'] = np.where(df['CS_SEXO'] == AGG_CRITERIA['SEXO_FEMININO'], 1, 0)
    
    idade = df['IDADE']
    df['flag_criancas'] = np.where((idade >= 0) & (idade <= 12), 1, 0)
    df['flag_adolescentes'] = np.where((idade >= 13) & (idade <= 17), 1, 0)
    df['flag_adultos'] = np.where((idade >= 18) & (idade <= 59), 1, 0)
    df['flag_idosos'] = np.where((idade >= 60), 1, 0)
    
    # 5. Agregação Semanal
    print("Agregando dados por MUNICÍPIO e SEMANA EPIDEMIOLÓGICA...")
    
    chaves_agrupamento = ['id_local', 'ano_epidemiologico', 'semana_epidemiologica']
    
    df_final = df.dropna(subset=chaves_agrupamento)
    
    colunas_para_somar = [col for col in df_final.columns if col.startswith('flag_')]
    agregacoes = {col: 'sum' for col in colunas_para_somar}
    fato_df = df_final.groupby(chaves_agrupamento).agg(agregacoes).reset_index()
    
    id_tempo_lookup = df_final.groupby(chaves_agrupamento)['id_tempo'].last().reset_index()
    fato_df = fato_df.merge(id_tempo_lookup, on=chaves_agrupamento, how='left')

    fato_df.drop(columns=['ano_epidemiologico', 'semana_epidemiologica'], inplace=True)

    fato_df.rename(columns=AGG_RENAMING_MAP, inplace=True)
    
    colunas_finais = ['id_tempo', 'id_local'] + list(AGG_RENAMING_MAP.values())
    fato_df = fato_df[colunas_finais]

    print(f"--- TRANSFORMAÇÃO CONCLUÍDA ({len(fato_df)} linhas agregadas) ---")
    return fato_df


# =============================================================================
# ETAPA DE CARGA (LOAD)
# =============================================================================

def salvar_csv(df, output_path):
    """Salva o DataFrame final (Tabela Fato) em um arquivo CSV."""
    print("\n--- INICIANDO ETAPA DE CARGA (Salvando CSV) ---")
    try:
        df.to_csv(output_path, index=False, sep=';', decimal=',')
        print(f"\n--- SUCESSO! ---")
        print(f"Arquivo salvo em: {output_path}")
    except Exception as e:
        print(f"\n--- ERRO AO SALVAR O CSV: {e} ---")
        raise


# =============================================================================
# ORQUESTRADOR PRINCIPAL (MAIN)
# =============================================================================

def main():

    print("========= INICIANDO PIPELINE ETL DENGUE =========")

    try:
        dim_local = carregar_csv(PATH_DIM_LOCAL)
        dim_tempo = carregar_csv(PATH_DIM_TEMPO)
    except Exception:
        print("Pipeline interrompido devido a erro no carregamento das dimensões.")
        return

    codigos_capitais = dim_local['cod_municipio'].astype(str).str[:-1].unique()

    # 1. EXECUTA A EXTRAÇÃO
    df_bruto = extrair_dados_brutos_otimizado(PATH_BRUTOS, codigos_capitais)
    
    if df_bruto.empty:
        print("Pipeline interrompido: Nenhum dado bruto foi carregado.")
        return
    
    # 2. EXECUTA A TRANSFORMAÇÃO
    fato_dengue_final = transformar_dados(
        df_bruto,
        dim_local,
        dim_tempo
    )
    
    # 3. EXECUTA A CARGA
    salvar_csv(fato_dengue_final, PATH_SAIDA_FATO)
    
    print("\n========= PIPELINE ETL DENGUE CONCLUÍDO =========")


if __name__ == "__main__":
    main()