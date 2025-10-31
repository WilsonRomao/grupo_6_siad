# -*- coding: utf-8 -*-
"""
Pipeline de ETL Otimizado para dados de Dengue.

Este script executa o processo completo de ETL:
1. Monta o Google Drive (se estiver no Colab).
2. Extrai dados brutos de múltiplos arquivos CSV.
3. Carrega as dimensões 'local' e 'tempo'.
4. Executa um pipeline de transformação completo.
5. Salva o DataFrame agregado final (fato) como um CSV.
"""

import os
import glob
import numpy as np
import pandas as pd
# A importação do sqlalchemy e create_engine não era usada no script original,
# então foi removida. Se for carregar para um banco, pode adicionar depois.

# Tenta importar o 'drive' do Colab, mas não quebra se não estiver no Colab
try:
    from google.colab import drive
except ImportError:
    print("Aviso: Módulo 'google.colab.drive' não encontrado. "
          "Skipping drive mount. Certifique-se que os caminhos são acessíveis.")
    drive = None

# =============================================================================
# 1. CONFIGURAÇÃO E CONSTANTES
# =============================================================================
# Centralizar todas as configurações aqui facilita a manutenção.

# --- Caminhos (Paths) ---
BASE_PATH = 'dados'
PATH_BRUTOS = os.path.join(BASE_PATH, 'brutos','dengue','DENGBR*.csv')
PATH_PROCESSADOS = os.path.join(BASE_PATH, 'processados')

PATH_DIM_LOCAL = os.path.join(PATH_PROCESSADOS, 'dim_local.csv')
PATH_DIM_TEMPO = os.path.join(PATH_PROCESSADOS, 'dim_tempo.csv')
PATH_SAIDA_FATO = os.path.join(PATH_PROCESSADOS, 'fato_casos_dengue.csv')

# --- Definições de Schema para Extração ---
# Esquema "Antigo" (com DT_NASC)
COLUNAS_ANTIGO = [
    "ID_AGRAVO", "CLASSI_FIN", "ID_MN_RESI", "SG_UF", "TPAUTOCTO",
    "NU_IDADE_N", "DT_NASC", "CS_SEXO", "HOSPITALIZ", "EVOLUCAO", "DT_NOTIFIC", "SEM_NOT"
]
DTYPES_ANTIGO = {
    'ID_AGRAVO': 'str', 'CLASSI_FIN': 'float64', 'ID_MN_RESI': 'str',
    'SG_UF': 'str', 'TPAUTOCTO': 'float64', 'NU_IDADE_N': 'str',
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

# --- Constantes de Transformação (Valores "Mágicos") ---
# Mapeamento para preenchimento de valores nulos (NaN)
FILLNA_MAP = {
    'HOSPITALIZ': 9.0,  # 9 = Ignorado
    'EVOLUCAO': 9.0,    # 9 = Ignorado
    'CLASSI_FIN': 9.0,  # 9 = Ignorado
    'TPAUTOCTO': 3.0,   # 3 = Indeterminado (mais próximo de "não sabemos")
    'CS_SEXO': 'I'      # I = Ignorado
}

# Critérios para criação das flags de agregação
AGG_CRITERIA = {
    # Códigos que NÃO contam como caso confirmado
    'CLASSI_FIN_DESCARTADOS': [2.0, 8.0, 9.0], # Descartado, Inconclusivo, Ignorado
    'EVOLUCAO_OBITO': 2.0,
    'HOSPITALIZ_SIM': 1.0,
    'TPAUTOCTO_SIM': 1.0,
    'SEXO_MASCULINO': 'M',
    'SEXO_FEMININO': 'F'
}

# Mapeamento final dos nomes das colunas da tabela Fato
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
# FUNÇÃO AUXILIAR - AMBIENTE COLAB
# =============================================================================

def mount_google_drive():
    """
    Monta o Google Drive se o script estiver rodando no Google Colab.
    """
    if drive:
        try:
            drive.mount('/content/drive')
            print("Google Drive montado com sucesso.")
        except Exception as e:
            print(f"Erro ao montar o Google Drive: {e}")
            raise
    else:
        print("Skipping Google Drive mount (não estamos no Colab).")


# =============================================================================
# ETAPA DE EXTRAÇÃO (EXTRACT)
# =============================================================================

def extrair_dados_brutos(file_pattern, cols_antigo, dtypes_antigo, cols_novo, dtypes_novo, codigos_filtro=None):
    """
    (VERSÃO MELHORADA COM FILTRO DE MEMÓRIA E LOG)
    """
    print(f"Iniciando extração de arquivos: {file_pattern}")
    all_files = glob.glob(file_pattern)
    num_files = len(all_files)
    
    if num_files == 0:
        print("Aviso: Nenhum arquivo encontrado.")
        return pd.DataFrame()

    if codigos_filtro is None:
        print("Aviso: Nenhum filtro de códigos de município fornecido.")
        codigos_filtro = [] # Define como lista vazia se não for passado

    all_data = [] # Esta lista agora guardará DFs pequenos e filtrados

    for i, file in enumerate(all_files):
        file_name = os.path.basename(file)
        print(f"  [{i+1}/{num_files}] Lendo: {file_name}")
        df = None # Reseta o df

        try:
            # Tenta ler o padrão antigo...
            df = pd.read_csv(file, usecols=cols_antigo, dtype=dtypes_antigo, sep=',')
        except ValueError as e:
            if "DT_NASC" in str(e):
                try:
                    # Tenta ler o padrão novo...
                    df = pd.read_csv(file, usecols=cols_novo, dtype=dtypes_novo, sep=',')
                    df.rename(columns={'ANO_NASC': 'DT_NASC'}, inplace=True)
                except Exception as e_novo:
                    print(f"  ERRO INESPERADO ao ler {file_name} (padrão novo): {e_novo}")
            else:
                print(f"  ERRO ao ler {file_name} (padrão antigo): {e}")
        except Exception as e:
            print(f"  ERRO genérico ao processar o arquivo {file_name}: {e}")

        # --- MUDANÇA PRINCIPAL: FILTRAR AQUI ---
        if df is not None:
            # Filtra o DF antes de adicioná-lo à lista
            df_filtrado = df[df['ID_MN_RESI'].isin(codigos_filtro)]
            
            if not df_filtrado.empty:
                all_data.append(df_filtrado)
            # Se o df_filtrado estiver vazio, ele é descartado e a memória é liberada.
        # --- FIM DA MUDANÇA ---

    if not all_data:
        print("Aviso: Nenhum dado foi extraído (ou nenhum dado correspondeu ao filtro das capitais).")
        return pd.DataFrame()

    print(f"\nLeitura de {len(all_data)}/{num_files} ficheiros concluída (após filtro).")
    print("A concatenar dados filtrados...")
    
    # Agora o concat é muito mais leve!
    dengueDF = pd.concat(all_data, ignore_index=True)
    
    print(f"--- Leitura e Concatenação Concluídas ---")
    print(f"Total de {len(dengueDF)} linhas concatenadas (apenas capitais).")
    return dengueDF


def carregar_dimensao(file_path, dtypes=None):
    """
    Função genérica para carregar um arquivo CSV de dimensão.

    Entrada:
    - file_path (str): Caminho completo para o arquivo CSV.
    - dtypes (dict, opcional): Dicionário de tipos de dados.

    Saída:
    - (pd.DataFrame): DataFrame da dimensão carregada.
    """
    try:
        df = pd.read_csv(file_path, dtype=dtypes,sep=';')
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

def _preparar_dim_local(dim_local):
    """Helper: Prepara a dim_local para o merge."""
    # Garante que a cópia está sendo modificada
    df = dim_local.copy()
    # Chave 'cod_municipio' deve ter 6 dígitos (removendo o dígito verificador)
    df['cod_municipio_6dig'] = df['cod_municipio'].astype(str).str[:-1]
    return df

def _preparar_dim_tempo(dim_tempo):
    """Helper: Prepara a dim_tempo para o merge."""
    df = dim_tempo.copy()
    # Garante que a chave de data está no formato datetime
    df['data_completa'] = pd.to_datetime(df['data_completa'], errors='coerce')
    return df

def _filtrar_dados_capitais(df, dim_local_clean):
    """Helper: Filtra o DataFrame principal para conter apenas capitais."""
    print("Filtrando dados brutos (mantendo apenas capitais)...")
    codigos_capitais = dim_local_clean['cod_municipio_6dig'].unique()
    df_capitais = df[df['ID_MN_RESI'].isin(codigos_capitais)].copy()
    print(f"Dados filtrados: {len(df_capitais)} linhas restantes.")
    return df_capitais

def _limpar_valores_nulos(df, fillna_map):
    """Helper: Preenche valores nulos com base no dicionário de dados."""
    print("Limpando valores Nulos (NaN)...")
    df.drop(columns=['ID_AGRAVO'], inplace=True, errors='ignore')
    
    for coluna, valor in fillna_map.items():
        df[coluna].fillna(valor, inplace=True)
        
    print("Nulos preenchidos.")
    return df

def _calcular_idade_e_ajustar_datas(df):
    """
    Helper: Uniformiza DT_NASC para ano, calcula a idade e
    converte DT_NOTIFIC para datetime.
    """
    print("Uniformizando datas e calculando idade...")
    
    # 1. Uniformizar DT_NASC (contém anos e datas completas)
    datas_nascimento = pd.to_datetime(df['DT_NASC'], errors='coerce', format='%Y')
    anos_nascimento = datas_nascimento.dt.year.fillna(0).astype(int)
    df['ANO_NASC'] = anos_nascimento # Nova coluna 'ANO_NASC'
    
    # 2. Converter DT_NOTIFIC
    df['DT_NOTIFIC_DT'] = pd.to_datetime(df['DT_NOTIFIC'], errors='coerce')
    ano_notificacao = df['DT_NOTIFIC_DT'].dt.year.fillna(0).astype(int)

    # 3. Calcular Idade
    idade_calculada = ano_notificacao - df['ANO_NASC']
    
    # 4. Corrigir Idades Desconhecidas (onde ANO_NASC ou ANO_NOTIF era 0)
    # Usamos -1 para "Idade Desconhecida"
    idade_calculada[(df['ANO_NASC'] == 0) | (ano_notificacao == 0)] = -1
    df['IDADE_CALCULADA'] = idade_calculada
    
    # 5. Limpar colunas antigas
    df.drop(columns=['DT_NASC', 'NU_IDADE_N'], inplace=True)
    
    print("Cálculo de idade concluído.")
    return df

def _mapear_dimensoes(df, dim_local_clean, dim_tempo_clean):
    """Helper: Faz o merge (join) com as dimensões local e tempo."""
    print("Mapeando dimensões (buscando Foreign Keys)...")
    
    # 1. Merge com Dim_Tempo
    df = df.merge(
        dim_tempo_clean[['data_completa', 'id_tempo']],
        left_on='DT_NOTIFIC_DT',
        right_on='data_completa',
        how='left'
    )
    
    # 2. Merge com Dim_Local
    df = df.merge(
        dim_local_clean[['cod_municipio_6dig', 'id_local']],
        left_on='ID_MN_RESI',
        right_on='cod_municipio_6dig',
        how='left'
    )
    
    # 3. Verificação
    nulos_tempo = df['id_tempo'].isnull().sum()
    nulos_local = df['id_local'].isnull().sum()
    if nulos_tempo > 0:
        print(f"Aviso: {nulos_tempo} registros não encontraram ID_tempo.")
    if nulos_local > 0:
        print(f"Aviso: {nulos_local} registros não encontraram ID_Local.")
        
    print("Mapeamento de dimensões concluído.")
    return df

def _criar_flags_agregacao(df, criteria):
    """Helper: Cria colunas 'flag' (0 ou 1) para facilitar a soma."""
    print("Criando colunas-flag para agregação...")
    
    # 1. Casos (Não pode ser Descartado, Inconclusivo ou Ignorado)
    df['flag_casos'] = np.where(
        df['CLASSI_FIN'].isin(criteria['CLASSI_FIN_DESCARTADOS']), 0, 1
    )
    # 2. Óbitos
    df['flag_obitos'] = np.where(
        df['EVOLUCAO'] == criteria['EVOLUCAO_OBITO'], 1, 0
    )
    # 3. Hospitalização
    df['flag_hospitalizacao'] = np.where(
        df['HOSPITALIZ'] == criteria['HOSPITALIZ_SIM'], 1, 0
    )
    # 4. Autóctones
    df['flag_autoctones'] = np.where(
        df['TPAUTOCTO'] == criteria['TPAUTOCTO_SIM'], 1, 0
    )
    # 5. Sexo
    df['flag_masculino'] = np.where(
        df['CS_SEXO'] == criteria['SEXO_MASCULINO'], 1, 0
    )
    df['flag_feminino'] = np.where(
        df['CS_SEXO'] == criteria['SEXO_FEMININO'], 1, 0
    )
    
    # 6. Faixa Etária (idade -1 = Desconhecida)
    idade = df['IDADE_CALCULADA']
    df['flag_criancas'] = np.where((idade >= 0) & (idade <= 12), 1, 0)
    df['flag_adolescentes'] = np.where((idade >= 13) & (idade <= 17), 1, 0)
    df['flag_adultos'] = np.where((idade >= 18) & (idade <= 59), 1, 0)
    df['flag_idosos'] = np.where((idade >= 60), 1, 0)
    
    return df

def _agregar_dados_fato(df, renaming_map):
    """
    Helper: Executa o 'group by' final para criar a tabela Fato,
    agrupando por local e tempo e somando as flags.
    """
    print("Agregando dados (groupby)...")
    
    chaves_agrupamento = ['id_local', 'id_tempo']
    
    # Filtra o DF para evitar agrupar linhas sem chave
    df_final = df.dropna(subset=chaves_agrupamento)
    
    # Define as colunas que queremos somar (todas que começam com 'flag_')
    colunas_para_somar = [col for col in df_final.columns if col.startswith('flag_')]
    
    # Cria o dicionário de agregação dinamicamente
    agregacoes = {col: 'sum' for col in colunas_para_somar}
    
    # Executa o GroupBy
    fato_df = df_final.groupby(chaves_agrupamento).agg(agregacoes).reset_index()
    
    # Renomeia as colunas para o padrão final (ex: 'flag_casos' -> 'num_casos')
    fato_df.rename(columns=renaming_map, inplace=True)
    
    print(f"Agregação concluída. Tabela Fato criada com {len(fato_df)} linhas.")
    return fato_df


def pipeline_transformacao(raw_df, dim_local, dim_tempo, constants):
    """
    Orquestra todas as etapas de transformação dos dados.

    Entrada:
    - raw_df (pd.DataFrame): DataFrame bruto da extração.
    - dim_local (pd.DataFrame): Dimensão de local.
    - dim_tempo (pd.DataFrame): Dimensão de tempo.
    - constants (dict): Dicionário com mapas de preenchimento e critérios.

    Saída:
    - (pd.DataFrame): Tabela Fato final, agregada e pronta para carga.
    """
    print("\n--- INICIANDO ETAPA DE TRANSFORMAÇÃO ---")
    
    # 1. Preparar Dimensões
    dim_local_clean = _preparar_dim_local(dim_local)
    dim_tempo_clean = _preparar_dim_tempo(dim_tempo)


    # 2. Limpar Nulos
    df_cleaned = _limpar_valores_nulos(raw_df, constants['FILLNA_MAP'])
    
    # 3. Tratar Datas e Calcular Idade
    df_with_age = _calcular_idade_e_ajustar_datas(df_cleaned)
    
    # 4. Mapear Dimensões (Merge)
    df_merged = _mapear_dimensoes(df_with_age, dim_local_clean, dim_tempo_clean)
    
    # 5. Criar Flags (para contagem)
    df_with_flags = _criar_flags_agregacao(df_merged, constants['AGG_CRITERIA'])
    
    # 6. Agregar (Group By)
    fato_casos_dengue = _agregar_dados_fato(df_with_flags, constants['AGG_RENAMING_MAP'])
    
    print("--- TRANSFORMAÇÃO CONCLUÍDA ---")
    return fato_casos_dengue


# =============================================================================
# ETAPA DE CARGA (LOAD)
# =============================================================================

def carregar_dados_processados(df, output_path):
    """
    Salva o DataFrame final (tabela Fato) em um arquivo CSV.
    Esta é a etapa 'Load' do nosso pipeline, persistindo o dado processado.

    Entrada:
    - df (pd.DataFrame): O DataFrame final e agregado.
    - output_path (str): Caminho completo onde o CSV será salvo.
    """
    print("\n--- INICIANDO ETAPA DE CARGA (Salvando CSV) ---")
    try:
        # index=False é crucial para não salvar o índice do pandas
        df.to_csv(output_path, index=False, sep=';', decimal=',')
        print(f"\n--- SUCESSO! ---")
        print(f"O DataFrame 'fato_casos_dengue' ({len(df)} linhas) foi salvo em:")
        print(output_path)
    except Exception as e:
        print(f"\n--- ERRO AO SALVAR O CSV ---")
        print(f"Não foi possível salvar o arquivo em: {output_path}")
        print(f"Erro: {e}")
        raise


# =============================================================================
# ORQUESTRADOR PRINCIPAL (MAIN)
# =============================================================================

def main():
    """
    Função principal que orquestra todo o pipeline de ETL.
    (VERSÃO MELHORADA COM FILTRO NA EXTRAÇÃO)
    """
    print("========= INICIANDO PIPELINE ETL DENGUE =========")
    mount_google_drive()
    
    # --- MUDANÇA 1: Carregar Dimensões PRIMEIRO ---
    print("A carregar dimensões para filtro prévio...")
    try:
        dim_local = carregar_dimensao(PATH_DIM_LOCAL)
        dim_tempo = carregar_dimensao(PATH_DIM_TEMPO)
    except Exception as e:
        print(f"ERRO: Não foi possível carregar dimensões. Pipeline interrompido. {e}")
        return

    # --- MUDANÇA 2: Preparar os códigos de filtro ---
    # Garante que a cópia está sendo modificada
    df_local_clean = dim_local.copy()
    # Chave 'cod_municipio' deve ter 6 dígitos (removendo o dígito verificador)
    df_local_clean['cod_municipio_6dig'] = df_local_clean['cod_municipio'].astype(str).str[:-1]
    codigos_capitais = df_local_clean['cod_municipio_6dig'].unique()

    # 1. ETAPA DE EXTRAÇÃO (Agora com filtro)
    df_bruto = extrair_dados_brutos(
        PATH_BRUTOS,
        COLUNAS_ANTIGO, DTYPES_ANTIGO,
        COLUNAS_NOVO, DTYPES_NOVO,
        codigos_capitais  # <-- Passamos os códigos para a função
    )
    
    if df_bruto.empty:
        print("Pipeline interrompido: Nenhum dado bruto foi carregado.")
        return
    
    # 2. ETAPA DE TRANSFORMAÇÃO
    # Junta todas as constantes de transformação em um dicionário
    transform_constants = {
        'FILLNA_MAP': FILLNA_MAP,
        'AGG_CRITERIA': AGG_CRITERIA,
        'AGG_RENAMING_MAP': AGG_RENAMING_MAP
    }
    
    # Executa o pipeline de transformação completo
    fato_dengue_final = pipeline_transformacao(
        df_bruto,
        dim_local,
        dim_tempo,
        transform_constants
    )
    
    # 3. ETAPA DE CARGA
    # Salva o resultado final
    carregar_dados_processados(fato_dengue_final, PATH_SAIDA_FATO)
    
    print("\n========= PIPELINE ETL DENGUE CONCLUÍDO =========")


if __name__ == "__main__":
    main()