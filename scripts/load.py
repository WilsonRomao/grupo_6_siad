# -*- coding: utf-8 -*-
"""
Script de Carga de Dados (Passo 2).
(VERSÃO CORRIGIDA v4.1 - Com limpeza (DELETE) ativa)
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
        # Assumindo que já removeu o _tratado do etl_socioeconomico
        'caminho_csv': os.path.join(PATH_PROCESSADOS, 'fato_socioeconomico.csv'),
        'tabela_dw': 'fato_socioeconomico'
    }
]

MODO_DE_CARGA = 'append'

# =============================================================================

def carregar_config_dw(path_yaml):
    """Lê o ficheiro 'db_config.yml' e constrói a string de conexão."""
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
    except Exception as e:
        print(f"ERRO ao ler a configuração '{path_yaml}': {e}")
        return None

def _esvaziar_tabelas(engine: Engine, tarefas_carga: list):
    """
    Esvazia todas as tabelas usando DELETE FROM.
    Executa todos os DELETES dentro de UMA ÚNICA TRANSAÇÃO,
    garantindo o COMMIT ao final.
    """
    print("\nA iniciar limpeza (DELETE) das tabelas (Fatos primeiro)...")
    
    tabelas_para_limpar = [tarefa['tabela_dw'] for tarefa in reversed(tarefas_carga)]
    
    # AQUI ESTÁ A CORREÇÃO:
    # Usamos 'with engine.begin()' para criar uma nova transação
    # que será AUTOMATICAMENTE 'commitada' se o bloco for
    # bem-sucedido, ou 'revertida' (rollback) se ocorrer um erro.
    try:
        with engine.begin() as conexao: # Inicia a transação e nos dá a conexão
            for tabela in tabelas_para_limpar:
                try:
                    print(f"  A esvaziar tabela: {tabela}...")
                    # Usamos a 'conexao' fornecida pelo 'with'
                    conexao.execute(text(f"DELETE FROM {tabela};"))
                
                except Exception as e:
                    # O 'if' para tabelas que não existem é uma boa prática
                    if "doesn't exist" in str(e) or "Unknown table" in str(e): # Adicionei 'Unknown table' (comum no MySQL)
                        print(f"  Aviso: Tabela {tabela} não existe (será criada). A saltar.")
                    else:
                        print(f"  ERRO ao esvaziar tabela {tabela}: {e}")
                        raise # Isso irá forçar o 'with' a fazer ROLLBACK

        # Se o script chegou aqui, o 'with' bloco terminou sem erros
        # e o COMMIT foi feito automaticamente.
        print("Limpeza das tabelas concluída.")
    
    except Exception as e:
        # Se o 'raise' dentro do loop foi acionado, caímos aqui.
        # O ROLLBACK já foi feito automaticamente pelo 'with'.
        print(f"ERRO GERAL durante a limpeza. A transação foi revertida (rollback). Erro: {e}")
        raise # Para o script principal


def carregar_csv_para_dw(engine: Engine, caminho_csv: str, nome_tabela: str, modo_carga: str):
    """
    Função genérica que lê um CSV e o carrega para uma tabela do DW.
    
    Usa um engine do SQLAlchemy para performance otimizada no df.to_sql().
    As transações são gerenciadas explicitamente com commit/rollback.
    """
    print(f"\n--- A processar: {os.path.basename(caminho_csv)} ---")
    
    # O engine do SQLAlchemy gerencia a conexão automaticamente
    # Usamos um bloco 'with' para garantir que a transação seja tratada
    try:
        df = pd.read_csv(caminho_csv, sep=';')
        
        if df.empty:
            print(f"AVISO: O ficheiro {caminho_csv} está vazio. A ignorar.")
            return

        print(f"Lidas {len(df)} linhas de {os.path.basename(caminho_csv)}")

        # Abrimos uma conexão/transação com o 'engine'
        with engine.begin() as conexao:
            print(f"A carregar ({modo_carga}) para a tabela '{nome_tabela}'...")
            
            df.to_sql(
                nome_tabela,
                con=conexao,            # Passa a conexão da transação
                if_exists=modo_carga,   # 'append' ou 'replace'
                index=False,
                chunksize=1000          # (Opcional, mas bom para tabelas grandes)
            )
        
        # Se o bloco 'with' terminar sem erro, o commit é FEITO AUTOMATICAMENTE.
        print(f"SUCESSO: Dados de {os.path.basename(caminho_csv)} carregados em '{nome_tabela}'.")

    except FileNotFoundError:
        print(f"ERRO: Ficheiro não encontrado: {caminho_csv}")
        raise
    except pd.errors.EmptyDataError:
        print(f"AVISO: O ficheiro {caminho_csv} está vazio. A ignorar.")
    except Exception as e:
        # Se um erro ocorrer dentro do 'with', o rollback é FEITO AUTOMATICAMENTE.
        print(f"ERRO ao carregar '{nome_tabela}': {e}")
        print("A transação foi revertida (rollback).")
        raise

# =============================================================================
# ORQUESTRADOR PRINCIPAL (MAIN)
# =============================================================================

def main():
    """Orquestra a carga de todos os CSVs para o DW."""
    print("========= INICIANDO SCRIPT DE CARGA (Estratégia: Commit Explícito) =========")
            
    STRING_CONEXAO_DW = carregar_config_dw(PATH_CONFIG)
    
    if not STRING_CONEXAO_DW:
        print("Pipeline abortado: Falha ao ler a configuração do DW.")
        sys.exit()
        
    engine = None
    try:
        # echo=True é ótimo para depuração
        engine = create_engine(STRING_CONEXAO_DW, echo=True)        
        conexao = engine.connect() 
        print("\nConexão estabelecida.")
            
        try:
            # --- CORREÇÃO 2 AQUI ---
            # A limpeza DEVE estar ativa para o pipeline ser re-executável
            _esvaziar_tabelas(engine, TAREFAS_DE_CARGA)
            
            # PASSO 2: CARREGAR TABELAS (Dims primeiro, Fatos depois)
            for tarefa in TAREFAS_DE_CARGA:
                carregar_csv_para_dw(
                    engine=engine,
                    caminho_csv=tarefa['caminho_csv'],
                    nome_tabela=tarefa['tabela_dw'],
                    modo_carga=MODO_DE_CARGA
                )
            
            # PASSO 3: FAZER O COMMIT (Como em create_tables.py)
            print("\nTarefas de carga concluídas. A fazer commit das alterações...")
            conexao.commit()
            print("--- COMMIT REALIZADO COM SUCESSO ---")

        except Exception as e:
            # PASSO 4: FAZER O ROLLBACK EM CASO DE ERRO
            print(f"\n!!!!!!!! ERRO DURANTE A CARGA: {e} !!!!!!!!")
            print("A reverter alterações (rollback)...")
            conexao.rollback()
            print("Rollback concluído. A base de dados não foi alterada.")
            sys.exit() # Para o pipeline
        
        finally:
            # PASSO 5: FECHAR A CONEXÃO
            conexao.close()
            print("Conexão com a base de dados fechada.")

    except ImportError:
         print("ERRO DE CONEXÃO (DW): Biblioteca 'pymysql' não encontrada.")
         print("Por favor, instala-a com: pip install pymysql")
         sys.exit()
    except Exception as e:
        print(f"ERRO ao estabelecer conexão inicial com o DW: {e}")
        sys.exit()

    
    print("\n========= SCRIPT DE CARGA CONCLUÍDO =========")
    print("Todos os dados CSV foram carregados para o Data Warehouse.")

if __name__ == "__main__":
    main()    