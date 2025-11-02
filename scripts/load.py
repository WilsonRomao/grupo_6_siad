# -*- coding: utf-8 -*-
"""
Script de Carga de Dados (Passo 2) - VERSÃO SIMPLIFICADA

Este script usa uma única transação (All-or-Nothing) para:
1. Limpar (DELETE) todas as tabelas.
2. Carregar (INSERT) todos os ficheiros CSV processados.
"""

import os
import sys
import pandas as pd
import yaml
from sqlalchemy import create_engine, text, Engine

# =============================================================================
# 1. CONFIGURAÇÃO CENTRALIZADA
# =============================================================================

PATH_CONFIG = os.path.join('configs', 'db_config.yml')
PATH_PROCESSADOS = os.path.join('dados', 'processados')

# Define a ordem correta da carga (Dimensões primeiro, Fatos depois)
TAREFAS_DE_CARGA = [
    {
        'caminho_csv': os.path.join(PATH_PROCESSADOS, 'dim_local.csv'),
        'tabela_dw': 'dim_local'
    },
    {
        'caminho_csv': os.path.join(PATH_PROCESSADOS, 'dim_tempo.csv'),
        'tabela_dw': 'dim_tempo'
    },
    {
        'caminho_csv': os.path.join(PATH_PROCESSADOS, 'fato_casos_dengue.csv'),
        'tabela_dw': 'fato_casos_dengue'
    },
    {
        'caminho_csv': os.path.join(PATH_PROCESSADOS, 'fato_clima.csv'),
        'tabela_dw': 'fato_clima'
    },
    {
        'caminho_csv': os.path.join(PATH_PROCESSADOS, 'fato_socioeconomico.csv'),
        'tabela_dw': 'fato_socioeconomico'
    }
]

# Modo de carga (append) é necessário porque limpamos primeiro
MODO_DE_CARGA = 'append'

# =============================================================================

def carregar_config_dw(path_yaml):
    """Lê o ficheiro 'db_config.yml' e constrói a string de conexão."""
    print(f"A ler configuração do DW de: {path_yaml}")
    try:
        with open(path_yaml, 'r') as f:
            config_yaml = yaml.safe_load(f)
        db_config = config_yaml['mysql']
        
        # Conecta-se diretamente à base de dados
        string_conexao = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}"
            f"/{db_config['database']}"
        )
        print(f"Configuração lida com sucesso. (Host: {db_config['host']})")
        return string_conexao
    except Exception as e:
        print(f"ERRO ao ler a configuração '{path_yaml}': {e}")
        return None

def _esvaziar_tabelas(conexao, tarefas_carga: list):
    """
    Esvazia todas as tabelas usando DELETE FROM (dentro da transação principal).
    Limpa pela ordem inversa (Fatos primeiro, Dimensões depois).
    """
    print("\nPASSO 1: A esvaziar tabelas (Fatos primeiro)...")
    
    # Inverte a lista de tarefas para apagar Fatos antes das Dimensões
    tabelas_para_limpar = [tarefa['tabela_dw'] for tarefa in reversed(tarefas_carga)]
    
    for tabela in tabelas_para_limpar:
        try:
            print(f"  A esvaziar tabela: {tabela}...")
            # Usa a conexão que foi passada pela 'main'
            conexao.execute(text(f"DELETE FROM {tabela};"))
        
        except Exception as e:
            # Se a tabela não existir, não é um erro crítico
            if "doesn't exist" in str(e) or "Unknown table" in str(e):
                print(f"  Aviso: Tabela {tabela} não existe (será criada). A saltar.")
            else:
                print(f"  ERRO ao esvaziar tabela {tabela}: {e}")
                raise # Força o rollback da transação principal


def carregar_csv_para_dw(conexao, caminho_csv: str, nome_tabela: str, modo_carga: str):
    """
    Lê um CSV e carrega-o para a tabela do DW (dentro da transação principal).
    """
    print(f"\n--- A processar: {os.path.basename(caminho_csv)} ---")
    
    try:
        df = pd.read_csv(caminho_csv, sep=';')
        
        if df.empty:
            print(f"AVISO: O ficheiro {caminho_csv} está vazio. A ignorar.")
            return

        print(f"Lidas {len(df)} linhas de {os.path.basename(caminho_csv)}")
        print(f"A carregar ({modo_carga}) para a tabela '{nome_tabela}'...")

        # Usa a conexão que foi passada pela 'main'
        df.to_sql(
            nome_tabela,
            con=conexao,            # Passa a conexão da transação
            if_exists=modo_carga,   # 'append'
            index=False,
            chunksize=1000          # Bom para tabelas grandes
        )
        
        print(f"SUCESSO: Dados de {os.path.basename(caminho_csv)} carregados.")

    except FileNotFoundError:
        print(f"ERRO: Ficheiro não encontrado: {caminho_csv}")
        raise # Força o rollback da transação principal
    except pd.errors.EmptyDataError:
        print(f"AVISO: O ficheiro {caminho_csv} está vazio. A ignorar.")
    except Exception as e:
        print(f"ERRO ao carregar '{nome_tabela}': {e}")
        raise # Força o rollback da transação principal

# =============================================================================
# ORQUESTRADOR PRINCIPAL (MAIN)
# =============================================================================

def main():
    """Orquestra a carga de todos os CSVs para o DW numa transação ÚNICA."""
    print("========= INICIANDO SCRIPT DE CARGA (Estratégia: Transação Única) =========")
            
    STRING_CONEXAO_DW = carregar_config_dw(PATH_CONFIG)
    
    if not STRING_CONEXAO_DW:
        print("Pipeline abortado: Falha ao ler a configuração do DW.")
        sys.exit()
        
    try:
        engine = create_engine(STRING_CONEXAO_DW)        
        
        # --- ESTA É A MUDANÇA PRINCIPAL ---
        # 'with engine.begin() as conexao:' inicia UMA transação.
        # Se o bloco 'with' terminar sem erros, faz COMMIT automático.
        # Se ocorrer qualquer erro (Exception), faz ROLLBACK automático.
        
        print("\nA iniciar transação 'All-or-Nothing'...")
        with engine.begin() as conexao:
            
            # PASSO 1: LIMPAR
            # Passamos a 'conexao' para a função
            _esvaziar_tabelas(conexao, TAREFAS_DE_CARGA)
            
            # PASSO 2: CARREGAR
            print("\nPASSO 2: A carregar dados (Dimensões primeiro)...")
            for tarefa in TAREFAS_DE_CARGA:
                # Passamos a mesma 'conexao' para a função
                carregar_csv_para_dw(
                    conexao=conexao,
                    caminho_csv=tarefa['caminho_csv'],
                    nome_tabela=tarefa['tabela_dw'],
                    modo_carga=MODO_DE_CARGA
                )
        
        # Se o script chegou aqui, o bloco 'with' terminou sem erros.
        print("\n--- SUCESSO! ---")
        print("A transação foi concluída e o 'commit' foi realizado automaticamente.")

    except ImportError:
         print("ERRO DE CONEXÃO (DW): Biblioteca 'pymysql' não encontrada.")
         print("Por favor, instala-a com: pip install pymysql")
         sys.exit()
    except Exception as e:
        # Se qualquer função (esvaziar ou carregar) lançar um 'raise',
        # o 'with' faz o rollback e saltamos para aqui.
        print(f"\n!!!!!!!! ERRO DURANTE A TRANSAÇÃO: {e} !!!!!!!!")
        print("A transação foi revertida (rollback) automaticamente.")
        print("A base de dados não foi alterada.")
        sys.exit() # Para o pipeline
    
    print("\n========= SCRIPT DE CARGA CONCLUÍDO =========")
    print("Todos os dados CSV foram carregados para o Data Warehouse.")

if __name__ == "__main__":
    main()