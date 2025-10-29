# -*- coding: utf-8 -*-
"""
Script de Carga de Dados (Passo 2).

Este script lê TODOS os ficheiros CSV da pasta 'processados'
e carrega-os para as tabelas correspondentes no Data Warehouse.

Ele deve ser executado APÓS o 'create_tables.py'.
"""

import os
import sys
import pandas as pd
import yaml
from sqlalchemy import create_engine

# =============================================================================
# 1. CONFIGURAÇÃO CENTRALIZADA
# =============================================================================

# Caminhos para os dados
PATH_CONFIG = os.path.join('configs', 'db_config.yml')
PATH_PROCESSADOS = os.path.join('dados', 'processados')
# A variável PATH_INTERMEDIARIOS foi removida, pois tudo vem de 'processados'.

# --- Mapeamento das Tarefas de Carga ---
# Lista de tuplos (caminho_do_csv, nome_da_tabela_no_dw)

TAREFAS_DE_CARGA = [
    
    # --- 1. DIMENSÕES (Devem ser carregadas primeiro) ---
    {
        'caminho_csv': os.path.join(PATH_PROCESSADOS, 'dim_local.csv'),
        'tabela_dw': 'dim_local'
    },
    {
        'caminho_csv': os.path.join(PATH_PROCESSADOS, 'dim_tempo.csv'),
        'tabela_dw': 'dim_tempo'
    },
    
    # --- 2. FATOS (Carregadas depois das dimensões) ---
    {
        'caminho_csv': os.path.join(PATH_PROCESSADOS, 'fato_casos_dengue.csv'),
        'tabela_dw': 'fato_casos_dengue'
    },
    {
        'caminho_csv': os.path.join(PATH_PROCESSADOS, 'fato_clima.csv'),
        'tabela_dw': 'fato_clima'
    },
    {
        # --- CAMINHO CORRIGIDO ---
        'caminho_csv': os.path.join(PATH_PROCESSADOS, 'fato_socioeconomico_tratado.csv'),
        'tabela_dw': 'fato_saneamento' # Mapeamento
    }
]

# Modo de carga para o .to_sql()
MODO_DE_CARGA = 'append'

# =============================================================================

def carregar_config_dw(path_yaml):
    """
    Lê o ficheiro 'db_config.yml' e constrói a string de conexão do MySQL.
    """
    print(f"A ler configuração do DW de: {path_yaml}")
    try:
        with open(path_yaml, 'r') as f:
            config_yaml = yaml.safe_load(f)
        
        db_config = config_yaml['mysql']
        
        string_conexao = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}"
            f"/{db_config['database']}"
        )
        
        print(f"Configuração lida com sucesso. (Host: {db_config['host']}:{db_config['port']})")
        return string_conexao
        
    except FileNotFoundError:
        print(f"ERRO: Ficheiro de configuração não encontrado em: {path_yaml}")
        return None
    except KeyError as e:
        print(f"ERRO: A chave {e} não foi encontrada no 'db_config.yml'.")
        return None

def carregar_csv_para_dw(engine, caminho_csv, nome_tabela, modo_carga):
    """
    Função genérica que lê um CSV e o carrega para uma tabela do DW.
    
    @param engine (SQLAlchemy Engine): A conexão com o banco.
    @param caminho_csv (str): Caminho para o ficheiro CSV.
    @param nome_tabela (str): Nome da tabela de destino.
    @param modo_carga (str): Modo de inserção ('append', 'replace').
    """
    print(f"\n--- A processar: {os.path.basename(caminho_csv)} ---")
    
    try:
        # 1. Ler o CSV (usando separador ';')
        df = pd.read_csv(caminho_csv, sep=';')
        
        if df.empty:
            print(f"AVISO: O ficheiro {caminho_csv} está vazio. A ignorar.")
            return

        print(f"Lidas {len(df)} linhas de {os.path.basename(caminho_csv)}")

        # 2. Carregar para o DW
        print(f"A carregar para a tabela '{nome_tabela}' (Modo: {modo_carga})...")
        
        df.to_sql(
            nome_tabela,
            con=engine,
            if_exists=modo_carga,
            index=False 
        )
        
        print(f"SUCESSO: Dados de {os.path.basename(caminho_csv)} carregados em '{nome_tabela}'.")

    except FileNotFoundError:
        print(f"ERRO: Ficheiro não encontrado: {caminho_csv}")
    except pd.errors.EmptyDataError:
        print(f"AVISO: O ficheiro {caminho_csv} está vazio. A ignorar.")
    except Exception as e:
        print(f"ERRO ao carregar '{nome_tabela}': {e}")

# =============================================================================
# ORQUESTRADOR PRINCIPAL (MAIN)
# =============================================================================

def main():
    """Orquestra a carga de todos os CSVs para o DW."""
    print("========= INICIANDO SCRIPT DE CARGA (Passo 2) =========")
            
    # 1. Ler a configuração do YAML
    STRING_CONEXAO_DW = carregar_config_dw(PATH_CONFIG)
    
    if not STRING_CONEXAO_DW:
        print("Pipeline abortado: Falha ao ler a configuração do DW.")
        sys.exit()
        
    try:
        engine = create_engine(STRING_CONEXAO_DW)
    except ImportError:
         print("ERRO DE CONEXÃO (DW): Biblioteca 'pymysql' não encontrada.")
         print("Por favor, instala-a com: pip install pymysql")
         sys.exit()
    except Exception as e:
        print(f"ERRO ao criar a 'engine' de conexão: {e}")
        sys.exit()

    print("\nA iniciar tarefas de carga (Dimensões primeiro, depois Fatos)...")
    
    # 2. Executar todas as tarefas de carga em ordem
    for tarefa in TAREFAS_DE_CARGA:
        carregar_csv_para_dw(
            engine=engine,
            caminho_csv=tarefa['caminho_csv'],
            nome_tabela=tarefa['tabela_dw'], # <--- ESTA É A CORREÇÃO
            modo_carga=MODO_DE_CARGA
        )
    
    print("\n========= SCRIPT DE CARGA CONCLUÍDO =========")
    print("Todos os dados CSV foram carregados para o Data Warehouse.")

if __name__ == "__main__":
    main()