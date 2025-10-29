# -*- coding: utf-8 -*-
"""
Pipeline de E-T (Extract-Transform) para Fato Socioeconômico.

Este script executa o processo de E-T:
1. Monta o Google Drive (se estiver no Colab).
2. Extrai dados brutos do SNIS (fonte principal).
3. Carrega as dimensões 'local' e 'tempo'.
4. Executa um pipeline de transformação para limpar, filtrar e enriquecer os dados.
5. Salva o DataFrame agregado final (fato) como um CSV.
"""

import os
import pandas as pd

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
PATH_PROCESSADOS = os.path.join(BASE_PATH, 'processados')
PATH_BRUTOS = os.path.join(BASE_PATH, 'brutos')


CAMINHOS_ETL = {
    'bruto_snis': os.path.join(PATH_BRUTOS, 'br_mdr_snis_municipio_agua_esgoto.csv'),
    'dim_local': os.path.join(PATH_PROCESSADOS, 'dim_local.csv'),
    'dim_tempo': os.path.join(PATH_PROCESSADOS, 'dim_tempo.csv'),
    'saida_fato_csv': os.path.join(PATH_PROCESSADOS, 'fato_socioeconomico_tratado.csv')
}

# --- Definições de Schema para Extração ---
# Otimização: ler apenas as colunas que realmente usamos.
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

# Regra de negócio para Fato Anual: buscar o ID do dia 1º de Janeiro
REGRA_TEMPO_ANUAL = {
    'mes': 1,
    'dia': 1
}

# Mapeamento final dos nomes das colunas da tabela Fato
COLUNAS_FATO_RENAME_MAP = {
    'id_local': 'ID_Local(FK)',
    'id_tempo': 'ID_tempo(FK)',
    'populacao_urbana': 'Num_populacao',
    'populacao_atentida_esgoto': 'Num_Esgoto',
    'populacao_atendida_agua': 'Num_agua Tratada'
}
# Colunas finais que devem estar no CSV
COLUNAS_FATO_FINAL = [
    'ID_Local(FK)', 'ID_tempo(FK)', 'Num_populacao', 
    'Num_Esgoto', 'Num_agua Tratada'
]

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

def extrair_dados_brutos(file_path, usecols):
    """
    Lê o arquivo CSV principal de dados brutos (SNIS).

    @param file_path (str): Caminho para o arquivo CSV.
    @param usecols (list): Lista de colunas para carregar (otimização).
    @return (pd.DataFrame): DataFrame bruto.
    """
    print(f"\n--- INICIANDO [E]XTRAÇÃO ---")
    print(f"A ler dados brutos: {file_path}")
    try:
        df = pd.read_csv(
            file_path,
            sep=',',
            usecols=usecols
        )
        print(f"Dados brutos lidos com sucesso ({len(df)} linhas).")
        return df
    except FileNotFoundError:
        print(f"ERRO: Arquivo de dados brutos não encontrado em: {file_path}")
        raise
    except Exception as e:
        print(f"ERRO ao ler dados brutos {file_path}: {e}")
        raise

def carregar_dimensao(file_path, usecols):
    """
    Função genérica para carregar um arquivo CSV de dimensão.

    @param file_path (str): Caminho completo para o arquivo CSV.
    @param usecols (list): Lista de colunas para carregar.
    @return (pd.DataFrame): DataFrame da dimensão carregada.
    """
    try:
        df = pd.read_csv(file_path, sep=';',usecols=usecols)
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
    df = dim_local.copy()
    
    # --- CORREÇÃO AQUI ---
    # Converte o cod_municipio (7 dígitos, ex: 2800308) para 6 dígitos INT
    # para que corresponda à chave que criamos para o SNIS.
    df['cod_municipio_6dig'] = df['cod_municipio'].astype(str).str[:-1].astype(int)
    
    # Mantém o original para referência, mas a chave de 6 dígitos é para o merge
    df['cod_municipio'] = df['cod_municipio'].astype(int)
    return df    


def _preparar_dim_tempo(dim_tempo, mes_regra, dia_regra):
    """Helper: Prepara a dim_tempo aplicando a regra de negócio anual."""
    df = dim_tempo.copy()
    
    # Regra de negócio: Esta Fato é anual. Usamos o ID do dia 1º de Janeiro.
    dim_tempo_anual = df[
        (df['mes'] == mes_regra) & (df['dia'] == dia_regra)
    ].copy()
    
    # Seleciona apenas colunas necessárias e garante o tipo
    dim_tempo_anual = dim_tempo_anual[['id_tempo', 'ano']]
    dim_tempo_anual['ano'] = dim_tempo_anual['ano'].astype(int)
    
    print(f"Regra de tempo (anual) aplicada. {len(dim_tempo_anual)} chaves de ano encontradas.")
    return dim_tempo_anual

def _ajustar_e_filtrar_dados(df_bruto, dim_local_clean, ano_filtro):
    """
    Helper: Aplica os filtros iniciais (ano, município) e ajusta a
    chave do município (7 para 6 dígitos).
    """
    print("A ajustar chave de município (7 para 6 dígitos)...")
    df = df_bruto.copy()
    df['id_municipio_6dig'] = df['id_municipio'].astype(str).str[:-1].astype(int)
    
    print(f"A filtrar por ano (>= {ano_filtro})...")
    df_filtrado_ano = df[df['ano'] >= ano_filtro].copy()
    print(f"Registos após filtro de ano: {len(df_filtrado_ano)}")
    
    # --- CORREÇÃO AQUI ---
    # Agora usamos a nova coluna 'cod_municipio_6dig' da dimensão
    municipios_validos = dim_local_clean['cod_municipio_6dig'].unique()

    print("A filtrar municípios (apenas os da dim_local)...")
    df_filtrado = df_filtrado_ano[
        df_filtrado_ano['id_municipio_6dig'].isin(municipios_validos)
    ].copy()
    
    print(f"Registos após filtros iniciais: {len(df_filtrado)}")
    if len(df_filtrado) == 0:
        print("AVISO: Nenhum registo encontrado após filtros. Pipeline pode falhar.")
        
    return df_filtrado

def _limpar_valores_nulos(df, colunas_metricas):
    """Helper: Preenche valores nulos (NaN -> 0) nas colunas de métrica."""
    print("A tratar valores nulos (NaN -> 0) nas métricas...")
    df_clean = df.copy()
    for col in colunas_metricas:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].fillna(0).astype(int)
    
    print("Nulos preenchidos.")
    return df_clean

def _mapear_dimensoes(df, dim_local_clean, dim_tempo_anual):
    """Helper: Faz o merge (join) com as dimensões local e tempo."""
    print("Mapeando dimensões (buscando Foreign Keys)...")
    
    # --- CORREÇÃO AQUI ---
    # O merge deve ser feito usando as duas chaves de 6 dígitos.
    df_com_local = pd.merge(
        df,
        dim_local_clean[['id_local', 'cod_municipio_6dig']], # Pedimos a coluna de 6 dígitos
        left_on='id_municipio_6dig',
        right_on='cod_municipio_6dig', # Fazemos o merge com a coluna de 6 dígitos
        how='left' 
    )
    
    # 2. Merge com Dim_Tempo (Esta parte estava correta)
    df_com_local['ano'] = df_com_local['ano'].astype(int)
    
    df_com_chaves = pd.merge(
        df_com_local,
        dim_tempo_anual,
        left_on='ano',
        right_on='ano',
        how='left'
    )
    
    # 3. Verificação
    nulos_tempo = df_com_chaves['id_tempo'].isnull().sum()
    nulos_local = df_com_chaves['id_local'].isnull().sum()
    if nulos_tempo > 0:
        print(f"Aviso: {nulos_tempo} registros não encontraram ID_tempo (anos fora da dim_tempo?).")
    if nulos_local > 0:
        print(f"Aviso: {nulos_local} registros não encontraram ID_Local (municípios fora da dim_local?).")
        
    df_final = df_com_chaves.dropna(subset=['id_local', 'id_tempo'])
    print(f"Mapeamento de dimensões concluído. {len(df_final)} registos válidos encontrados.")
    
    return df_final

def _finalizar_schema_fato(df, rename_map, colunas_finais):
    """Helper: Renomeia as colunas e seleciona apenas as colunas finais da Fato."""
    print("A renomear e selecionar colunas finais para a Fato...")
    
    df_renomeado = df.rename(columns=rename_map)
    
    # Filtra o DataFrame para ter apenas as colunas finais
    df_para_carga = df_renomeado[colunas_finais]
    
    return df_para_carga


def pipeline_transformacao(df_bruto, dim_local, dim_tempo, constantes):
    """
    Orquestra todas as etapas de transformação dos dados.

    @param df_bruto (pd.DataFrame): DataFrame bruto da extração.
    @param dim_local (pd.DataFrame): Dimensão de local.
    @param dim_tempo (pd.DataFrame): Dimensão de tempo.
    @param constantes (dict): Dicionário com regras de negócio.

    @return (pd.DataFrame): Tabela Fato final, pronta para carga.
    """
    print("\n--- INICIANDO ETAPA DE [T]RANSFORMAÇÃO ---")
    
    # 1. Preparar Dimensões
    dim_local_clean = _preparar_dim_local(dim_local)
    dim_tempo_anual = _preparar_dim_tempo(
        dim_tempo, 
        constantes['REGRA_TEMPO']['mes'],
        constantes['REGRA_TEMPO']['dia']
    )
    
    # 2. Ajustar e Filtrar Dados Brutos
    df_filtrado = _ajustar_e_filtrar_dados(
        df_bruto, 
        dim_local_clean, 
        constantes['ANO_FILTRO']
    )
    
    # 3. Limpar Nulos
    df_limpo = _limpar_valores_nulos(
        df_filtrado, 
        constantes['COLUNAS_METRICAS']
    )
    
    # 4. Mapear Dimensões (Merge)
    df_com_chaves = _mapear_dimensoes(
        df_limpo, 
        dim_local_clean, 
        dim_tempo_anual
    )
    
    # 5. Finalizar Schema (Renomear e Selecionar)
    # Nota: Este pipeline não precisa de agregação (GroupBy) porque
    # os dados do SNIS já parecem estar por 'ano' e 'município'.
    # Se precisássemos agregar, adicionaríamos um passo _agregar_dados_fato aqui.
    fato_socioeconomico = _finalizar_schema_fato(
        df_com_chaves,
        constantes['RENAME_MAP'],
        constantes['COLUNAS_FINAIS']
    )
    
    print("--- [T]RANSFORMAÇÃO CONCLUÍDA ---")
    return fato_socioeconomico

# =============================================================================
# ETAPA DE CARGA (LOAD)
# =============================================================================

def carregar_dados_processados(df, output_path):
    """
    Salva o DataFrame final (tabela Fato) em um arquivo CSV.
    Esta é a etapa 'Load' do nosso pipeline E-T, persistindo o dado processado
    para o próximo script (L) usar.

    @param df (pd.DataFrame): O DataFrame final e agregado.
    @param output_path (str): Caminho completo onde o CSV será salvo.
    """
    print("\n--- INICIANDO ETAPA DE [L]OAD (Salvando CSV) ---")
    
    if df is None or df.empty:
        print("AVISO: O DataFrame final está vazio. Nenhum arquivo CSV será salvo.")
        return

    try:
        # Garante que o diretório de saída existe
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Salva o CSV final
        df.to_csv(output_path, index=False, sep=';') # Mudei para sep=';'
        
        print(f"\n--- SUCESSO! ---")
        print(f"O DataFrame 'fato_socioeconomico_tratado' ({len(df)} linhas) foi salvo em:")
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
    Função principal que orquestra todo o pipeline de E-T.
    """
    print("========= INICIANDO PIPELINE E-T: Fato_Socioeconomico =========")
    
    # 0. Montar o Google Drive (apenas se estiver no Colab)
    mount_google_drive()
    
    # 1. ETAPA DE EXTRAÇÃO
    df_bruto = extrair_dados_brutos(
        CAMINHOS_ETL['bruto_snis'],
        SCHEMA_SNIS_COLS
    )
    
    if df_bruto.empty:
        print("Pipeline interrompido: Nenhum dado bruto foi carregado.")
        return

    dim_local = carregar_dimensao(
        CAMINHOS_ETL['dim_local'],
        SCHEMA_DIM_LOCAL_COLS
    )
    dim_tempo = carregar_dimensao(
        CAMINHOS_ETL['dim_tempo'],
        SCHEMA_DIM_TEMPO_COLS
    )
    
    # 2. ETAPA DE TRANSFORMAÇÃO
    # Junta todas as constantes de transformação em um dicionário
    transform_constants = {
        'ANO_FILTRO': ANO_FILTRO_INICIAL,
        'COLUNAS_METRICAS': COLUNAS_METRICAS_PARA_LIMPAR,
        'REGRA_TEMPO': REGRA_TEMPO_ANUAL,
        'RENAME_MAP': COLUNAS_FATO_RENAME_MAP,
        'COLUNAS_FINAIS': COLUNAS_FATO_FINAL
    }
    
    fato_final = pipeline_transformacao(
        df_bruto,
        dim_local,
        dim_tempo,
        transform_constants
    )
    
    # 3. ETAPA DE CARGA (Salvar CSV)
    carregar_dados_processados(fato_final, CAMINHOS_ETL['saida_fato_csv'])
    
    print("\n========= PIPELINE E-T Fato_Socioeconomico CONCLUÍDO =========")
    print(f"O ficheiro {CAMINHOS_ETL['saida_fato_csv']} está pronto para o script de carga (LOAD).")


if __name__ == "__main__":
    main()