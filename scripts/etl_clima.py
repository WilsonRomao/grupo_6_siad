# -*- coding: utf-8 -*-
"""
Pipeline de ETL para dados Meteorológicos (INMET)

Este script executa o processo completo de ETL para múltiplos arquivos
de dados climáticos das capitais.

1.  Monta o Google Drive.
2.  Carrega as dimensões 'local' e 'tempo'.
3.  Varre a pasta de dados brutos em busca de todos os arquivos de clima.
4.  Para cada arquivo:
    a. Extrai metadados (UF, Cidade) do cabeçalho.
    b. Extrai os dados horários.
    c. Limpa e transforma os dados (corrige vírgulas, trata NaNs).
    d. Agrega de horário para diário.
    e. Mapeia as FKs (id_local, id_tempo) das dimensões.
    f. Agrega de diário para semanal (nível da Fato).
5.  Concatena os resultados de todos os arquivos.
6.  Salva (sobrescreve) o arquivo 'fato_clima.csv' final.
"""

import os
import glob
import pandas as pd
import numpy as np

# Tenta importar o 'drive' do Colab
try:
    from google.colab import drive
except ImportError:
    print("Aviso: Módulo 'google.colab.drive' não encontrado. "
          "A saltar a montagem do drive. Certifique-se que os caminhos são acessíveis.")
    drive = None

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
# Usa o wildcard (*) para encontrar todos os arquivos CSV na pasta
PADRAO_ARQUIVOS_CLIMA = os.path.join(
    PATH_BRUTOS, 'meteorologico','apenas_capitais','INMET_*.CSV'
)

# --- Configurações de Schema e Transformação ---

# Colunas que queremos manter e seus novos nomes
MAPA_COLUNAS_CLIMA = {
    'Data': 'Data',
    'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)': 'precipitacao_total',
    'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)': 'temperatura'
}
COLUNAS_NUMERICAS = ['precipitacao_total', 'temperatura']

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def mount_google_drive():
    """
    Monta o Google Drive se o script estiver a ser executado no Google Colab.
    """
    if drive:
        try:
            drive.mount('/content/drive')
            print("Google Drive montado com sucesso.")
        except Exception as e:
            print(f"Erro ao montar o Google Drive: {e}")
            raise
    else:
        print("A saltar a montagem do Google Drive (não estamos no Colab).")

def carregar_dimensao(file_path, dtypes=None):
    """
    Função genérica para carregar um arquivo CSV de dimensão.
    !! Inclui a correção sep=';' !!

    Entrada:
    - file_path (str): Caminho completo para o arquivo CSV.
    - dtypes (dict, opcional): Dicionário de tipos de dados.

    Saída:
    - (pd.DataFrame): DataFrame da dimensão carregada.
    """
    try:
        # CORREÇÃO: Adicionado sep=';' para ler os arquivos salvos
        df = pd.read_csv(file_path, dtype=dtypes, sep=';')
        print(f"Dimensão '{os.path.basename(file_path)}' carregada com {len(df)} linhas.")
        return df
    except FileNotFoundError:
        print(f"ERRO: Arquivo de dimensão não encontrado em: {file_path}")
        raise
    except Exception as e:
        print(f"ERRO ao carregar dimensão {file_path}: {e}")
        raise


# =============================================================================
# ETAPA DE EXTRAÇÃO (EXTRACT)
# =============================================================================

def extrair_metadata_clima(file_path):
    """
    Lê o cabeçalho de um arquivo INMET (primeiras 8 linhas)
    para extrair a UF e a Cidade.

    Entrada:
    - file_path (str): Caminho para o arquivo INMET.

    Saída:
    - (dict): Dicionário com 'uf' e 'cidade' (ex: {'uf': 'MG', 'cidade': 'BELO HORIZONTE'}).
    """
    try:
        # Lê apenas as 8 primeiras linhas do cabeçalho
        df_header = pd.read_csv(
            file_path,
            sep=':;',          # O cabeçalho usa ':;' como separador
            encoding='latin-1',
            nrows=8,           # Apenas as linhas de metadados
            header=None,       # Não há cabeçalho de colunas
            engine='python'    # Necessário para separadores regex como ':;'
        )
        
        local = {
            'uf': df_header.iloc[1, 1].strip(), # Linha 1, coluna 1 (UF)
            'cidade': df_header.iloc[2, 1].split(' - ')[0].strip() # Linha 2, coluna 1 (Estação)
        }
        return local
    
    except Exception as e:
        print(f"  ERRO: Não foi possível ler os metadados do arquivo: {e}")
        return None

def extrair_dados_clima(file_path, colunas_map):
    """
    Lê os dados climáticos horários de um arquivo INMET,
    pulando o cabeçalho e selecionando colunas.

    Entrada:
    - file_path (str): Caminho para o arquivo INMET.
    - colunas_map (dict): Dicionário de colunas para manter.

    Saída:
    - (pd.DataFrame): DataFrame com os dados brutos horários.
    """
    try:
        df = pd.read_csv(
            file_path,
            sep=';',           # Os dados usam ';' como separador
            encoding='latin-1',
            skiprows=8,        # Pula as 8 linhas de metadados
            usecols=list(colunas_map.keys()) # Lê apenas as colunas de interesse
        )
        
        # Remove a última coluna 'Unnamed' se ela existir (comum em CSVs do INMET)
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        
        # Renomeia as colunas
        df = df.rename(columns=colunas_map)
        return df
        
    except Exception as e:
        print(f"  ERRO: Não foi possível ler os dados do arquivo: {e}")
        return pd.DataFrame()


# =============================================================================
# ETAPA DE TRANSFORMAÇÃO (TRANSFORM)
# =============================================================================

def transformar_dados_clima(df, colunas_numericas):
    """
    Limpa o DataFrame de clima horário:
    1. Converte 'Data' para datetime.
    2. Corrige valores numéricos (ex: ',8' -> '0,8').
    3. Converte colunas numéricas para float (lidando com ',' decimal).
    4. Interpola valores NaN (nulos) de forma temporal.

    Entrada:
    - df (pd.DataFrame): DataFrame bruto.
    - colunas_numericas (list): Lista de colunas para tratar.

    Saída:
    - (pd.DataFrame): DataFrame limpo e pronto para agregar.
    """
    if df.empty:
        return pd.DataFrame()

    # 1. Converter 'Data' para datetime
    df['Data'] = pd.to_datetime(df['Data'], format='%Y/%m/%d')
    
    # 2. Corrigir problema do ',8' (precipitação)
    df['precipitacao_total'] = df['precipitacao_total'].astype(str).str.replace(
        '^,', '0,', regex=True
    )

    # 3. Converter colunas numéricas (e tratar decimais com vírgula)
    for col in colunas_numericas:
        # Troca a vírgula decimal por ponto
        df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
        # Converte para numérico, forçando erros (textos) para NaN
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 4. Interpolar valores NaN (Nulos)
    # Esta é a forma correta: interpolar *depois* de converter para numérico.
    # Usamos 'time' para preencher NaNs baseando-se no tempo (requer índice de data)
    df = df.set_index('Data')
    df[colunas_numericas] = df[colunas_numericas].interpolate(method='time')
    
    # Preenche quaisquer NaNs restantes (no início ou fim do arquivo)
    df[colunas_numericas] = df[colunas_numericas].ffill()
    df[colunas_numericas] = df[colunas_numericas].bfill()
    
    return df.reset_index()


# =============================================================================
# ETAPA DE AGREGAÇÃO (AGGREGATE)
# =============================================================================

def agregar_dados_clima(df_limpo, dim_tempo, dim_local, metadata_local):
    """
    Agrega os dados limpos em 3 passos:
    1. Horário -> Diário
    2. Mapeia FKs (id_local, id_tempo)
    3. Diário -> Semanal (nível da Fato)

    Entrada:
    - df_limpo (pd.DataFrame): Dados horários limpos.
    - dim_tempo (pd.DataFrame): Dimensão de Tempo.
    - dim_local (pd.DataFrame): Dimensão de Local.
    - metadata_local (dict): {'uf': 'MG', 'cidade': 'BELO HORIZONTE'}

    Saída:
    - (pd.DataFrame): DataFrame agregado por semana, pronto para a Fato.
    """
    
    # --- PASSO 1: AGREGAR (HORÁRIO -> DIÁRIO) ---
    df_agregado_diario = df_limpo.groupby('Data').agg(
        temperatura_media=('temperatura', 'mean'),
        precipitacao_total=('precipitacao_total', 'sum')
    ).reset_index()

    # --- PASSO 2: ENRIQUECER (BUSCAR FKS) ---
    
    # 2.1. Buscar ID_Local
    cidade_atual = metadata_local['cidade']
    
    # Compara o nome da cidade (em maiúsculas) com a dim_local
    resultado_local = dim_local[
        dim_local['nome_municipio'].str.upper() == cidade_atual.upper()
    ]
    
    id_local = None
    if not resultado_local.empty:
        id_local = resultado_local.iloc[0]['id_local']
    else:
        print(f"  AVISO: Não foi possível encontrar o ID_local para '{cidade_atual}'. A saltar este arquivo.")
        return pd.DataFrame() # Retorna DF vazio

    # 2.2. Buscar ID_Tempo (e dados da semana)
    # Garante que as chaves de data estão no formato datetime
    dim_tempo['data_completa'] = pd.to_datetime(dim_tempo['data_completa'])
    df_agregado_diario['Data'] = pd.to_datetime(df_agregado_diario['Data'])

    df_diario_com_chaves = pd.merge(
        df_agregado_diario,
        dim_tempo,
        left_on='Data',
        right_on='data_completa',
        how='inner' # Garante que só mantemos dias que existem na dim_tempo
    )

    # --- PASSO 3: AGREGAR (DIÁRIO -> SEMANAL) ---
    # Agrupamos pelo ID_Local e pela Chave de Tempo Semanal (ano + semana)
    df_agregado_semanal = df_diario_com_chaves.groupby(
        ['ano_epidemiologico', 'semana_epidemiologica']
    ).agg(
        # Para o ID_Tempo(FK), pegamos o ID do último dia daquele grupo semanal
        id_tempo=('id_tempo', 'last'),
        
        # Para a temperatura, calculamos a MÉDIA das médias diárias
        temperatura_media=('temperatura_media', 'mean'),
        
        # Para a precipitação, calculamos a SOMA das somas diárias
        precipitacao_soma=('precipitacao_total', 'sum')
        
    ).reset_index()

    # Adiciona o ID_Local (FK) que encontrámos
    df_agregado_semanal['id_local'] = id_local
    
    # Arredonda e limpa
    df_agregado_semanal['temperatura_media'] = df_agregado_semanal['temperatura_media'].round(2)

    # Seleciona e reordena as colunas finais para a Fato
    colunas_fato_clima = ['id_tempo', 'id_local', 'temperatura_media', 'precipitacao_soma']
    
    return df_agregado_semanal[colunas_fato_clima]


# =============================================================================
# ETAPA DE CARGA (LOAD)
# =============================================================================

def carregar_fato_clima(df_final, output_path):
    """
    Salva (SOBRESCRVENDO) o DataFrame agregado final em um CSV.

    Entrada:
    - df_final (pd.DataFrame): O DataFrame completo da Fato Clima.
    - output_path (str): Caminho completo onde o CSV será salvo.
    """
    print(f"\nA salvar dados finais em: {output_path}")
    try:
        # Salva o arquivo final, sobrescrevendo (mode='w')
        # Usamos sep=';' para manter o padrão dos outros arquivos
        df_final.to_csv(output_path, index=False, mode='w', header=True, sep=';')
        
        print(f"--- SUCESSO! ---")
        print(f"'fato_clima.csv' salvo com {len(df_final)} linhas.")
    
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
    Função principal que orquestra todo o pipeline de ETL do Clima.
    """
    print("========= INICIANDO PIPELINE ETL CLIMA =========")
    
    # 0. Montar o Google Drive (apenas se estiver no Colab)
    mount_google_drive()
    
    # 1. Carregar Dimensões (apenas uma vez)
    try:
        dim_tempo = carregar_dimensao(PATH_DIM_TEMPO)
        dim_local = carregar_dimensao(PATH_DIM_LOCAL)
    except Exception as e:
        print(f"Pipeline interrompido: Falha ao carregar dimensões. Erro: {e}")
        return

    # 2. Encontrar todos os arquivos de dados brutos
    all_files = glob.glob(PADRAO_ARQUIVOS_CLIMA)
    if not all_files:
        print(f"Aviso: Nenhum arquivo de clima encontrado em: {PADRAO_ARQUIVOS_CLIMA}")
        return

    print(f"Encontrados {len(all_files)} arquivos de clima para processar...")
    
    # Lista para guardar os DataFrames processados (semanais)
    lista_dfs_semanais = []
    
    # 3. Loop de Processamento (Extract, Transform, Aggregate)
    for i, file_path in enumerate(all_files):
        print(f"\n--- Processando arquivo {i+1}/{len(all_files)} ---")
        print(f"  Arquivo: {os.path.basename(file_path)}")
        
        # 3.1. Extract (Metadados e Dados)
        metadata = extrair_metadata_clima(file_path)
        if metadata is None:
            continue # Salta para o próximo arquivo se não conseguir ler metadados

        print(f"  Metadados: {metadata['cidade']} - {metadata['uf']}")
        
        df_raw = extrair_dados_clima(file_path, MAPA_COLUNAS_CLIMA)
        if df_raw.empty:
            continue # Salta para o próximo arquivo se não conseguir ler os dados

        # 3.2. Transform
        df_clean = transformar_dados_clima(df_raw, COLUNAS_NUMERICAS)
        
        # 3.3. Aggregate
        df_semanal = agregar_dados_clima(
            df_clean, dim_tempo, dim_local, metadata
        )
        
        if not df_semanal.empty:
            lista_dfs_semanais.append(df_semanal)
            print(f"  Processado com sucesso: {len(df_semanal)} semanas agregadas.")

    # 4. Concatenar todos os resultados
    if not lista_dfs_semanais:
        print("\nNenhum dado foi processado com sucesso.")
        print("========= PIPELINE ETL CLIMA CONCLUÍDO (SEM DADOS) =========")
        return

    print(f"\nConcatenando resultados de {len(lista_dfs_semanais)} DataFrames...")
    fato_clima_final = pd.concat(lista_dfs_semanais, ignore_index=True)

    # 5. Carregar (Load) - Salva o arquivo final
    carregar_fato_clima(fato_clima_final, PATH_SAIDA_FATO)
    
    print("\n========= PIPELINE ETL CLIMA CONCLUÍDO =========")


if __name__ == "__main__":
    main()