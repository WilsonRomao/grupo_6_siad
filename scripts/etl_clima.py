"""
Pipeline de ETL Simplificado para dados Meteorológicos (INMET)

Este script executa o processo de ETL:
1.  Carrega as dimensões 'local' e 'tempo'.
2.  Varre a pasta de dados brutos e processa um arquivo de cada vez.
3.  Para cada arquivo:
    a. Extrai metadados (UF, Cidade) e os dados horários.
    b. Executa a função de transformação e agregação.
4.  Junta os resultados de todos os arquivos.
5.  Salva o arquivo 'fato_clima.csv' final.
"""

import os
import glob
import pandas as pd
import numpy as np
import unicodedata 

# =============================================================================
# 1. CONFIGURAÇÃO E CONSTANTES
# =============================================================================

# --- Caminhos (Paths) ---
CAMINHO_BASE = 'dados'
PATH_PROCESSADOS = os.path.join(CAMINHO_BASE, 'processados')
PATH_BRUTOS = os.path.join(CAMINHO_BASE, 'brutos')

# Caminho de SAÍDA
PATH_SAIDA_FATO = os.path.join(PATH_PROCESSADOS, 'fato_clima.csv')

# Caminhos de ENTRADA (Dimensões)
PATH_DIM_TEMPO = os.path.join(PATH_PROCESSADOS, 'dim_tempo.csv')
PATH_DIM_LOCAL = os.path.join(PATH_PROCESSADOS, 'dim_local.csv')

# PADRÃO DE ENTRADA (Dados Brutos)
PADRAO_ARQUIVOS_CLIMA = os.path.join(
    PATH_BRUTOS, 'meteorologico','apenas_capitais','INMET_*.CSV'
)

# --- Definições de Schema para Extração ---

# Colunas que queremos manter e seus novos nomes
MAPA_COLUNAS_CLIMA = {
    'Data': 'Data',
    'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)': 'precipitacao_total',
    'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)': 'temperatura'
}
# Versão 2 do schema (nomes de colunas ligeiramente diferentes)
MAPA_COLUNAS_CLIMA_V2 = {
    'DATA (YYYY-MM-DD)': 'Data',
    'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)': 'precipitacao_total',
    'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)': 'temperatura'
}
# Colunas que precisam de tratamento numérico
COLUNAS_NUMERICAS = ['precipitacao_total', 'temperatura']

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

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
# ETAPA DE EXTRAÇÃO (EXTRACT)
# =============================================================================

def extrair_metadados_clima(file_path):
    """Lê o cabeçalho de um arquivo INMET (8 linhas) para extrair UF e Cidade."""
    try:
        # Lê apenas as 8 primeiras linhas
        df_header = pd.read_csv(
            file_path,
            sep=':;',          # Separador do cabeçalho
            encoding='latin-1',
            nrows=8,
            header=None,
            engine='python'
        )
        
        local = {
            'uf': df_header.iloc[1, 1].strip(), # Linha 1, coluna 1 (UF)
            'cidade': df_header.iloc[2, 1].split(' - ')[0].strip() # Linha 2, coluna 1 (Estação)
        }
        return local
    
    except Exception as e:
        print(f"  ERRO: Não foi possível ler os metadados do arquivo: {e}")
        return None

def extrair_dados_clima(file_path):
    """
    Lê os dados climáticos horários de um arquivo INMET,
    pulando o cabeçalho e tentando ler dois formatos (v1 e v2).
    """
    df = None
    rename_map = None

    try:
        # 1. Tenta ler com o padrão v1
        df = pd.read_csv(
            file_path,
            sep=';',
            encoding='latin-1',
            skiprows=8,
            usecols=list(MAPA_COLUNAS_CLIMA.keys())
        )
        rename_map = MAPA_COLUNAS_CLIMA

    except ValueError:
        # 2. Se falhar (ValueError), tenta ler com o padrão v2
        try:
            df = pd.read_csv(
                file_path,
                sep=';',
                encoding='latin-1',
                skiprows=8,
                usecols=list(MAPA_COLUNAS_CLIMA_V2.keys())
            )
            rename_map = MAPA_COLUNAS_CLIMA_V2
        except Exception as e_novo:
            print(f"  ERRO: Falha ao tentar ler padrão v2 em {file_path}: {e_novo}")
            return pd.DataFrame() # Retorna DF vazio
            
    except Exception as e_gen:
        print(f"  ERRO genérico ao processar o arquivo {file_path}: {e_gen}")
        return pd.DataFrame() # Retorna DF vazio

    # 3. Se o df foi carregado com SUCESSO:
    if df is not None:
        try:
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            df = df.rename(columns=rename_map)
            return df
        except Exception as e_proc:
            print(f"  ERRO ao processar (renomear/limpar) {file_path}: {e_proc}")
            return pd.DataFrame()

    print(f"  ERRO: A leitura de {file_path} falhou (formato irreconhecível).")
    return pd.DataFrame()

# =============================================================================
# ETAPA DE TRANSFORMAÇÃO E AGREGAÇÃO (TRANSFORM)
# =============================================================================

def transformar_e_agregar_clima(df_raw, metadata_local, dim_tempo, dim_local):
    """
    Função consolidada que limpa, transforma, enriquece e agrega os dados.
    Recebe os dados horários de UM arquivo e retorna os dados semanais.
    """
    if df_raw.empty:
        return pd.DataFrame()

    # --- ETAPA 1: LIMPEZA E TRANSFORMAÇÃO (de transformar_dados_clima) ---
    df = df_raw.copy()
    
    # 1.1. Converter 'Data' para datetime
    df['Data'] = pd.to_datetime(df['Data'])
    
    # 1.2. Corrigir problema do ',8' (precipitação)
    df['precipitacao_total'] = df['precipitacao_total'].astype(str).str.replace(
        '^,', '0,', regex=True
    )

    # 1.3. Converter colunas numéricas (e tratar decimais com vírgula)
    for col in COLUNAS_NUMERICAS:
        df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 1.4. Interpolar (preencher) valores NaN (Nulos)
    df = df.set_index('Data')
    df[COLUNAS_NUMERICAS] = df[COLUNAS_NUMERICAS].interpolate(method='time')
    df[COLUNAS_NUMERICAS] = df[COLUNAS_NUMERICAS].ffill().bfill() # Preenche pontas
    
    df_limpo = df.reset_index()

    # --- ETAPA 2: AGREGAÇÃO (de agregar_dados_clima) ---

    # 2.1. AGREGAR (HORÁRIO -> DIÁRIO)
    df_agregado_diario = df_limpo.groupby('Data').agg(
        temperatura_media=('temperatura', 'mean'),
        precipitacao_total=('precipitacao_total', 'sum')
    ).reset_index()

    # 2.2. ENRIQUECER (BUSCAR FKS)
    
    # 1. Normaliza a cidade do arquivo (diretamente)
    cidade_atual_norm = unicodedata.normalize('NFD', str(metadata_local['cidade'])) \
        .encode('ascii', 'ignore') \
        .decode('utf-8') \
        .upper()
    
    # 2. Normaliza a coluna de municípios (usando .apply com lambda)
    nomes_municipio_norm = dim_local['nome_municipio'].apply(
        lambda texto: unicodedata.normalize('NFD', str(texto))
                                .encode('ascii', 'ignore')
                                .decode('utf-8')
                                .upper()
    )

    # 3. Compara as duas strings normalizadas
    resultado_local = dim_local[
        nomes_municipio_norm == cidade_atual_norm
    ]
        
    if resultado_local.empty:
        # A mensagem de aviso agora mostra o nome original E o normalizado
        print(f"  AVISO: ID_local não encontrado para '{metadata_local['cidade']}' (Normalizado: '{cidade_atual_norm}'). A saltar este arquivo.")
        return pd.DataFrame() # Retorna DF vazio
    
    id_local = resultado_local.iloc[0]['id_local']

    # Buscar ID_Tempo (e dados da semana)
    dim_tempo['data_completa'] = pd.to_datetime(dim_tempo['data_completa'])
    df_agregado_diario['Data'] = pd.to_datetime(df_agregado_diario['Data'])

    df_diario_com_chaves = pd.merge(
        df_agregado_diario,
        dim_tempo,
        left_on='Data',
        right_on='data_completa',
        how='inner' # Só mantém dias que existem na dim_tempo
    )

    # 2.3. AGREGAR (DIÁRIO -> SEMANAL)
    df_agregado_semanal = df_diario_com_chaves.groupby(
        ['ano_epidemiologico', 'semana_epidemiologica']
    ).agg(
        id_tempo=('id_tempo', 'last'), # FK: Pega o ID do último dia da semana
        temperatura_media=('temperatura_media', 'mean'),
        precipitacao_total=('precipitacao_total', 'sum')
    ).reset_index()

    # Adiciona o ID_Local (FK) e arredonda os valores
    df_agregado_semanal['id_local'] = id_local
    df_agregado_semanal['temperatura_media'] = df_agregado_semanal['temperatura_media'].round(2)

    # Seleciona e reordena as colunas finais
    colunas_fato_clima = ['id_tempo', 'id_local', 'temperatura_media', 'precipitacao_total']
    
    return df_agregado_semanal[colunas_fato_clima]


# =============================================================================
# ETAPA DE CARGA (LOAD)
# =============================================================================

def salvar_csv(df_final, output_path):
    """Salva (SOBRESCRVENDO) o DataFrame agregado final em um CSV."""
    print(f"\nA salvar dados finais em: {output_path}")
    try:
        # Salva o arquivo final
        df_final.to_csv(output_path, index=False, mode='w', header=True, sep=';')
        
        print(f"--- SUCESSO! ---")
        print(f"'fato_clima.csv' salvo com {len(df_final)} linhas.")
    
    except Exception as e:
        print(f"\n--- ERRO AO SALVAR O CSV: {e} ---")
        raise


# =============================================================================
# ORQUESTRADOR PRINCIPAL (MAIN)
# =============================================================================

def main():
    
    print("========= INICIANDO PIPELINE ETL CLIMA =========")
    
    # 1. Carregar Dimensões (apenas uma vez)
    try:
        dim_tempo = carregar_csv(PATH_DIM_TEMPO)
        dim_local = carregar_csv(PATH_DIM_LOCAL)
    except Exception as e:
        print(f"Pipeline interrompido: Falha ao carregar dimensões. Erro: {e}")
        return

    # 2. Encontrar todos os arquivos de dados brutos
    all_files = glob.glob(PADRAO_ARQUIVOS_CLIMA)
    if not all_files:
        print(f"Aviso: Nenhum arquivo de clima encontrado em: {PADRAO_ARQUIVOS_CLIMA}")
        return

    print(f"Encontrados {len(all_files)} arquivos de clima para processar...")
    
    # Lista para guardar os resultados processados de cada arquivo
    lista_dfs_semanais = []
    
    # 3. Loop de Processamento (Extract, Transform, Aggregate)
    for i, file_path in enumerate(all_files):
        print(f"\n--- Processando arquivo {i+1}/{len(all_files)} ---")
        print(f"  Arquivo: {os.path.basename(file_path)}")
        
        # 3.1. Extract
        metadata = extrair_metadados_clima(file_path)
        if metadata is None:
            continue # Salta para o próximo arquivo

        print(f"  Metadados: {metadata['cidade']} - {metadata['uf']}")
        
        df_raw = extrair_dados_clima(file_path)
        if df_raw.empty:
            continue # Salta para o próximo arquivo

        # 3.2. Transform & Aggregate (Função Consolidada)
        df_semanal = transformar_e_agregar_clima(
            df_raw, metadata, dim_tempo, dim_local
        )
        
        if not df_semanal.empty:
            lista_dfs_semanais.append(df_semanal)
            print(f"  Processado com sucesso: {len(df_semanal)} semanas agregadas.")

    # 4. Concatenar (Juntar) todos os resultados
    if not lista_dfs_semanais:
        print("\nNenhum dado foi processado com sucesso.")
        print("========= PIPELINE ETL CLIMA CONCLUÍDO (SEM DADOS) =========")
        return

    print(f"\nConcatenando resultados de {len(lista_dfs_semanais)} arquivos...")
    fato_clima_final = pd.concat(lista_dfs_semanais, ignore_index=True)

    # 5. Carregar (Load) - Salva o arquivo final
    salvar_csv(fato_clima_final, PATH_SAIDA_FATO)
    
    print("\n========= PIPELINE ETL CLIMA CONCLUÍDO =========")


if __name__ == "__main__":
    main()