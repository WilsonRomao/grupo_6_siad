"""
Pipeline de ETL Simplificado para dados de Dengue.

Este script executa o processo de ETL:
1. Extrai dados brutos de múltiplos arquivos CSV.
2. Carrega as dimensões 'local' e 'tempo'.
3. Executa uma transformação completa numa única função.
4. Salva o DataFrame agregado final (fato) como um CSV.
"""

import os
import glob
import numpy as np
import pandas as pd

# =============================================================================
# 1. CONFIGURAÇÃO E CONSTANTES
# =============================================================================
# Caminhos para os arquivos
BASE_PATH = 'dados'
PATH_BRUTOS = os.path.join(BASE_PATH, 'brutos','dengue','DENGBR*.csv')
PATH_PROCESSADOS = os.path.join(BASE_PATH, 'processados')

PATH_DIM_LOCAL = os.path.join(PATH_PROCESSADOS, 'dim_local.csv')
PATH_DIM_TEMPO = os.path.join(PATH_PROCESSADOS, 'dim_tempo.csv')
PATH_SAIDA_FATO = os.path.join(PATH_PROCESSADOS, 'fato_casos_dengue.csv')

# --- Definições de Schema para Extração ---
# Esquema "Antigo" (com DT_NASC)
COLUNAS_ANTIGO = [
    "ID_AGRAVO", "CLASSI_FIN", "ID_MN_RESI", "SG_UF", "TPAUTOCTO", "DT_NASC", "CS_SEXO", "HOSPITALIZ", "EVOLUCAO", "DT_NOTIFIC", "SEM_NOT"
]
DTYPES_ANTIGO = {
    'ID_AGRAVO': 'str', 'CLASSI_FIN': 'float64', 'ID_MN_RESI': 'str',
    'SG_UF': 'str', 'TPAUTOCTO': 'float64',
    'DT_NASC': 'str', 'CS_SEXO': 'str', 'HOSPITALIZ': 'float64',
    'EVOLUCAO': 'float64', 'DT_NOTIFIC': 'str', 'SEM_NOT': 'str'
}

# Esquema "Novo" (com ANO_NASC)
COLUNAS_NOVO = [
    "ID_AGRAVO", "CLASSI_FIN", "ID_MN_RESI", "SG_UF", "TPAUTOCTO",
    "ANO_NASC", "CS_SEXO", "HOSPITALIZ", "EVOLUCAO", "DT_NOTIFIC", "SEM_NOT"
]
DTYPES_NOVO = {
    'ID_AGRAVO': 'str', 'CLASSI_FIN': 'float64', 'ID_MN_RESI': 'str',
    'SG_UF': 'str', 'TPAUTOCTO': 'float64', 'ANO_NASC': 'str',
    'CS_SEXO': 'str', 'HOSPITALIZ': 'float64', 'EVOLUCAO': 'float64',
    'DT_NOTIFIC': 'str', 'SEM_NOT': 'str'
}

# --- Constantes de Transformação ---
# Mapeamento para preenchimento de valores nulos (NaN)
FILLNA_MAP = {
    'HOSPITALIZ': 9.0,  # 9 = Ignorado
    'EVOLUCAO': 9.0,    # 9 = Ignorado
    'CLASSI_FIN': 9.0,  # 9 = Ignorado
    'TPAUTOCTO': 3.0,   # 3 = Indeterminado
    'CS_SEXO': 'I'      # I = Ignorado
}

# Critérios para criação das flags (colunas 0 ou 1)
AGG_CRITERIA = {
    'CLASSI_FIN_DESCARTADOS': [2.0, 8.0, 9.0], # Descartado, Inconclusivo, Ignorado
    'EVOLUCAO_OBITO': 2.0,
    'HOSPITALIZ_SIM': 1.0,
    'TPAUTOCTO_SIM': 1.0,
    'SEXO_MASCULINO': 'M',
    'SEXO_FEMININO': 'F'
}

# Mapeamento final dos nomes das colunas (para o CSV de saída)
AGG_RENAMING_MAP = {
    'flag_casos': 'num_casos',
    'flag_obitos': 'num_obitos',
    'flag_autoctones': 'num_autoctones',
    'flag_masculino': 'num_masculino',
    'flag_feminino': 'num_feminino',
    'flag_criancas': 'num_criancas',
    'flag_hospitalizacao': 'num_hospitalizacao',
    'flag_adolescentes': 'num_adolescentes',
    'flag_adultos': 'num_adultos',
    'flag_idosos': 'num_idosos'
}

# =============================================================================
# ETAPA DE EXTRAÇÃO (EXTRACT)
# =============================================================================

def extrair_dados_brutos(file_pattern, codigos_filtro):
    """
    Lê múltiplos arquivos CSV, filtrando pelos códigos de município.
    Tenta ler dois formatos de arquivo (antigo com DT_NASC, novo com ANO_NASC).
    """
    print(f"\n--- INICIANDO EXTRAÇÃO: {file_pattern} ---")
    all_files = glob.glob(file_pattern)
    num_files = len(all_files)
    
    if num_files == 0:
        print("Aviso: Nenhum arquivo encontrado.")
        return pd.DataFrame()

    all_data = [] # Lista para guardar os DataFrames de cada arquivo

    for i, file in enumerate(all_files):
        file_name = os.path.basename(file)
        print(f"  [{i+1}/{num_files}] Lendo: {file_name}")
        df = None

        try:
            # Tenta ler o padrão antigo
            df = pd.read_csv(file, usecols=COLUNAS_ANTIGO, dtype=DTYPES_ANTIGO, sep=',')
        except ValueError:
            try:
                # Se falhar, tenta ler o padrão novo
                df = pd.read_csv(file, usecols=COLUNAS_NOVO, dtype=DTYPES_NOVO, sep=',')
                df.rename(columns={'ANO_NASC': 'DT_NASC'}, inplace=True)
            except Exception as e_novo:
                print(f"  ERRO ao ler {file_name} (padrão novo): {e_novo}")
        except Exception as e:
            print(f"  ERRO genérico ao processar o arquivo {file_name}: {e}")

        # Filtra o DataFrame ANTES de adicionar à lista (poupa memória)
        if df is not None:
            df_filtrado = df[df['ID_MN_RESI'].isin(codigos_filtro)]
            if not df_filtrado.empty:
                all_data.append(df_filtrado)

    if not all_data:
        print("Aviso: Nenhum dado foi extraído (ou filtro não encontrou dados).")
        return pd.DataFrame()

    print("Concatenando dados filtrados...")
    dengueDF = pd.concat(all_data, ignore_index=True)
    
    print(f"--- EXTRAÇÃO CONCLUÍDA ({len(dengueDF)} linhas) ---")
    return dengueDF


def carregar_csv(file_path, separador=';'):
    """Função genérica para carregar um arquivo CSV (como as dimensões)."""
    try:
        df = pd.read_csv(file_path, sep=separador)
        print(f"Arquivo '{os.path.basename(file_path)}' carregado ({len(df)} linhas).")
        return df
    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado em: {file_path}")
        raise


# =============================================================================
# ETAPA DE TRANSFORMAÇÃO (TRANSFORM)
# =============================================================================

def transformar_dados(df_bruto, dim_local, dim_tempo):
    """
    Executa todas as etapas de transformação (limpeza, cálculo, merge, flags)
    numa única sequência.
    """
    print("\n--- INICIANDO ETAPA DE TRANSFORMAÇÃO ---")
    
    # Faz uma cópia para evitar modificar o original
    df = df_bruto.copy()

    # 1. Preparar Dimensões
    print("Preparando dimensões...")
    dim_local['cod_municipio_6dig'] = dim_local['cod_municipio'].astype(str).str[:-1]
    dim_tempo['data_completa'] = pd.to_datetime(dim_tempo['data_completa'], errors='coerce')

    # 2. Limpar Nulos
    print("Limpando valores Nulos (NaN)...")
    df.drop(columns=['ID_AGRAVO'], inplace=True, errors='ignore')
    for coluna, valor in FILLNA_MAP.items():
        df[coluna].fillna(valor, inplace=True)

    # 3. Calcular Idade
    print("Uniformizando datas e calculando idade...")
    # Converte 'DT_NASC' (que pode ser ano ou data) para Ano
    datas_nascimento = pd.to_datetime(df['DT_NASC'], errors='coerce', format='%Y')
    df['ANO_NASC'] = datas_nascimento.dt.year.fillna(0).astype(int)
    
    # Converte data de notificação
    df['DT_NOTIFIC_DT'] = pd.to_datetime(df['DT_NOTIFIC'], errors='coerce')
    ano_notificacao = df['DT_NOTIFIC_DT'].dt.year.fillna(0).astype(int)

    # Calcula a idade
    idade_calculada = ano_notificacao - df['ANO_NASC']
    # Se ano de nascimento ou notificação for 0, marca idade como -1 (Desconhecida)
    idade_calculada[(df['ANO_NASC'] == 0) | (ano_notificacao == 0)] = -1
    df['IDADE_CALCULADA'] = idade_calculada
    
    df.drop(columns=['DT_NASC'], inplace=True) # Limpa coluna antiga

    # 4. Mapear Dimensões (Merge/Join)
    print("Mapeando dimensões (merge)...")
    # Merge com Dim_Tempo
    df = df.merge(
        dim_tempo[['data_completa', 'id_tempo']],
        left_on='DT_NOTIFIC_DT',
        right_on='data_completa',
        how='left'
    )
    # Merge com Dim_Local
    df = df.merge(
        dim_local[['cod_municipio_6dig', 'id_local']],
        left_on='ID_MN_RESI',
        right_on='cod_municipio_6dig',
        how='left'
    )
    
    # 5. Criar Flags (colunas 0 ou 1 para facilitar a soma)
    print("Criando colunas-flag para agregação...")
    # Casos (Não pode ser Descartado, Inconclusivo ou Ignorado)
    df['flag_casos'] = np.where(
        df['CLASSI_FIN'].isin(AGG_CRITERIA['CLASSI_FIN_DESCARTADOS']), 0, 1
    )
    df['flag_obitos'] = np.where(df['EVOLUCAO'] == AGG_CRITERIA['EVOLUCAO_OBITO'], 1, 0)
    df['flag_hospitalizacao'] = np.where(df['HOSPITALIZ'] == AGG_CRITERIA['HOSPITALIZ_SIM'], 1, 0)
    df['flag_autoctones'] = np.where(df['TPAUTOCTO'] == AGG_CRITERIA['TPAUTOCTO_SIM'], 1, 0)
    df['flag_masculino'] = np.where(df['CS_SEXO'] == AGG_CRITERIA['SEXO_MASCULINO'], 1, 0)
    df['flag_feminino'] = np.where(df['CS_SEXO'] == AGG_CRITERIA['SEXO_FEMININO'], 1, 0)
    
    # Faixa Etária (idade -1 = Desconhecida)
    idade = df['IDADE_CALCULADA']
    df['flag_criancas'] = np.where((idade >= 0) & (idade <= 12), 1, 0)
    df['flag_adolescentes'] = np.where((idade >= 13) & (idade <= 17), 1, 0)
    df['flag_adultos'] = np.where((idade >= 18) & (idade <= 59), 1, 0)
    df['flag_idosos'] = np.where((idade >= 60), 1, 0)

    # 6. Agregar (Group By)
    print("Agregando dados (groupby)...")
    chaves_agrupamento = ['id_local', 'id_tempo']
    
    # Remove linhas que não encontraram id_local ou id_tempo
    df_final = df.dropna(subset=chaves_agrupamento)
    
    # Lista de todas as colunas que queremos somar
    colunas_para_somar = [col for col in df_final.columns if col.startswith('flag_')]
    
    # Dicionário de agregação (ex: {'flag_casos': 'sum', 'flag_obitos': 'sum', ...})
    agregacoes = {col: 'sum' for col in colunas_para_somar}
    
    # Executa o GroupBy
    fato_df = df_final.groupby(chaves_agrupamento).agg(agregacoes).reset_index()
    
    # Renomeia as colunas (ex: 'flag_casos' -> 'num_casos')
    fato_df.rename(columns=AGG_RENAMING_MAP, inplace=True)
    
    print(f"--- TRANSFORMAÇÃO CONCLUÍDA ({len(fato_df)} linhas) ---")
    return fato_df


# =============================================================================
# ETAPA DE CARGA (LOAD)
# =============================================================================

def salvar_csv(df, output_path):
    """Salva o DataFrame final (tabela Fato) em um arquivo CSV."""
    print("\n--- INICIANDO ETAPA DE CARGA (Salvando CSV) ---")
    try:
        # index=False evita salvar o índice do pandas no arquivo
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

    # --- Carrega Dimensões (Necessárias para o filtro da Extração) ---
    try:
        dim_local = carregar_csv(PATH_DIM_LOCAL)
        dim_tempo = carregar_csv(PATH_DIM_TEMPO)
    except Exception as e:
        print(f"ERRO: Não foi possível carregar dimensões. Pipeline interrompido.")
        return

    # Prepara os códigos de filtro (só carregar dados das capitais)
    codigos_capitais = dim_local['cod_municipio'].astype(str).str[:-1].unique()

    # 1. ETAPA DE EXTRAÇÃO
    df_bruto = extrair_dados_brutos(PATH_BRUTOS, codigos_capitais)
    
    if df_bruto.empty:
        print("Pipeline interrompido: Nenhum dado bruto foi carregado.")
        return
    
    # 2. ETAPA DE TRANSFORMAÇÃO
    fato_dengue_final = transformar_dados(
        df_bruto,
        dim_local,
        dim_tempo
    )
    
    # 3. ETAPA DE CARGA
    salvar_csv(fato_dengue_final, PATH_SAIDA_FATO)
    
    print("\n========= PIPELINE ETL DENGUE CONCLUÍDO =========")


if __name__ == "__main__":

    main()