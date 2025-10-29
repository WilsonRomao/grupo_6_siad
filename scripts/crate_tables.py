# -*- coding: utf-8 -*-
"""
Script para Criação da Estrutura do DW (Passo 1).

Este script lê o ficheiro 'create_dw.sql' e executa os comandos
para criar todas as tabelas (Dimensões e Fatos) no Data Warehouse.

Ele obtém as credenciais de forma segura do ficheiro 'config/mysql.yml'.

Isto só precisa de ser executado UMA VEZ.
"""

import os
import sys
import yaml  # Importa a biblioteca para ler YAML
from sqlalchemy import create_engine, text

# =============================================================================
# 1. CONFIGURAÇÃO CENTRALIZADA
# =============================================================================

# Assume que o script está na pasta 'scripts/' e é executado da raiz do projeto.
PATH_SQL_CREATE = os.path.join('dw_schema','create_dw.sql') 
PATH_YAML_CONFIG = os.path.join('configs', 'db_config.yml')

# =============================================================================

def carregar_config_dw(path_yaml):
    """
    Lê o ficheiro YAML e constrói a string de conexão do MySQL.
    
    @param path_yaml (str): Caminho para o ficheiro 'mysql.yml'.
    @return (str): A string de conexão do SQLAlchemy, ou None se falhar.
    """
    print(f"A ler configuração do DW de: {path_yaml}")
    try:
        with open(path_yaml, 'r') as f:
            # Carrega o YAML de forma segura
            config_yaml = yaml.safe_load(f)
        
        # Assume a estrutura 'mysql:' dentro do YAML
        db_config = config_yaml['mysql']
        
        # Constrói a string de conexão para MySQL
        string_conexao = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}"
            f"/{db_config['database']}"
        )
        print("Configuração do DW lida com sucesso.")
        return string_conexao
        
    except FileNotFoundError:
        print(f"ERRO: Ficheiro de configuração não encontrado em: {path_yaml}")
        return None
    except KeyError as e:
        print(f"ERRO: A chave {e} não foi encontrada no ficheiro YAML.")
        print("Verifica se 'config', 'user', 'password', 'host', 'port', e 'database' existem.")
        return None
    except Exception as e:
        print(f"ERRO ao ler o ficheiro YAML: {e}")
        return None

def ler_sql(caminho_sql):
    """
    Lê o conteúdo completo de um ficheiro .sql.
    
    @param caminho_sql (str): Caminho para o ficheiro 'create_dw.sql'.
    @return (str): O conteúdo do ficheiro como uma string.
    """
    print(f"A ler o ficheiro SQL de: {caminho_sql}")
    try:
        with open(caminho_sql, 'r', encoding='utf-8') as f:
            sql_completo = f.read()
        
        if not sql_completo.strip():
            print(f"AVISO: O ficheiro SQL '{caminho_sql}' está vazio.")
            return None
            
        return sql_completo
    except FileNotFoundError:
        print(f"ERRO: Ficheiro SQL não encontrado em: {caminho_sql}")
        return None
    except Exception as e:
        print(f"ERRO ao ler o ficheiro SQL: {e}")
        return None

def executar_sql_no_dw(str_conexao, sql_commands):
    """
    Conecta-se ao DW e executa os comandos SQL UM POR UM.
    (Versão corrigida que EXECUTA o comando 'USE')
    
    @param str_conexao (str): String de conexão do SQLAlchemy.
    @param sql_commands (str): Comandos SQL a serem executados.
    """
    if not sql_commands:
        print("Nenhum comando SQL para executar.")
        return
        
    # 1. Separar os comandos SQL pelo ponto e vírgula
    comandos_individuais = [
        cmd.strip() for cmd in sql_commands.split(';') 
        if cmd.strip()  # Garante que não executamos comandos vazios
    ]
        
    print(f"A ligar ao Data Warehouse... ({len(comandos_individuais)} comandos a executar)")
    try:
        engine = create_engine(str_conexao)
        
        with engine.connect() as conexao:
            
            # 2. Executar comandos em loop, um de cada vez
            for comando in comandos_individuais:
                
                # --- CORREÇÃO ---
                # REMOVEMOS a lógica que ignorava o 'USE'.
                # Agora, ele irá executar o 'USE dw_siad'
                # e mudar o contexto do banco de dados.
                
                print(f"A executar: {comando[:60]}...") # Mostra os primeiros 60 chars
                conexao.execute(text(comando))
            
            # 3. Commit de todas as alterações no final
            conexao.commit()
        
        print("\n--- SUCESSO! ---")
        print("A estrutura do Data Warehouse (tabelas) foi criada com sucesso.")
        
    except ImportError:
         print("ERRO DE EXECUÇÃO (DW): Biblioteca 'pymysql' não encontrada.")
         print("Por favor, instala-a com: pip install pymysql")
    except Exception as e:
        print(f"ERRO ao executar SQL no Data Warehouse: {e}")
        print("\nPossíveis causas:")
        print("1. O container Docker do MySQL está a correr?")
        print("2. As credenciais em 'config/db_config.yml' estão corretas?")
        print("3. O utilizador do banco tem permissão de 'CREATE TABLE'?")
        print("4. A base de dados (database) especificada no YAML já existe?")

# =============================================================================
# ORQUESTRADOR PRINCIPAL (MAIN)
# =============================================================================

def main():
    """Orquestra a leitura da configuração e a execução do script SQL."""
    print("========= INICIANDO SCRIPT DE CRIAÇÃO DO DW (Passo 1) =========")
            
    # 1. Ler a configuração do YAML para obter a string de conexão
    STRING_CONEXAO_DW = carregar_config_dw(PATH_YAML_CONFIG)
    
    if not STRING_CONEXAO_DW:
        print("Pipeline abortado: Falha ao ler a configuração do DW.")
        sys.exit() # Para o script se não conseguir ler a config
        
    # 2. Ler o ficheiro 'create_dw.sql'
    comandos_sql = ler_sql(PATH_SQL_CREATE)
    
    # 3. Executar os comandos SQL no DW
    if comandos_sql:
        executar_sql_no_dw(STRING_CONEXAO_DW, comandos_sql)
    
    print("\n========= SCRIPT DE CRIAÇÃO DO DW CONCLUÍDO =========")

if __name__ == "__main__":
    main()