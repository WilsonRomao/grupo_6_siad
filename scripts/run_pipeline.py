# -*- coding: utf-8 -*-
"""
Script Mestre (Orquestrador) para o Pipeline de ETL da Dengue.

Este script executa todos os passos do pipeline na ordem correta
de dependência.

ORDEM DE EXECUÇÃO:
1. crate_tables: Cria a estrutura do DW.
2. cria_dimensoes: Gera os CSVs das dimensões (local, tempo).
3. etl_dengue: Processa dados brutos da dengue.
4. etl_clima: Processa dados brutos de clima.
5. etl_socioeconomico: Processa dados brutos socioeconômicos.
6. load: Carrega todos os CSVs processados para o DW.
"""

import time
import sys

# 1. Importar todos os scripts como módulos
# O "try-except" garante que o utilizador é notificado se
# os arquivos não estiverem na mesma pasta.
try:
    import create_tables
    import cria_dimensoes
    import etl_dengue
    import etl_clima
    import etl_socioeconomico
    import load
except ImportError as e:
    print(f"ERRO: Não foi possível importar um módulo. {e}")
    print("Certifique-se que 'run_pipeline.py' está na mesma pasta que os outros scripts.")
    sys.exit() # Para o script se não conseguir importar

# 2. Definir a lista de tarefas na ordem correta
# Cada tarefa é um tuplo (modulo, nome_descritivo)
TAREFAS_PIPELINE = [
    (create_tables, "create_tables.py"),
    (cria_dimensoes, "cria_dimensoes.py"),
    (etl_dengue, "etl_dengue.py"),
    (etl_clima, "etl_clima.py"),
    (etl_socioeconomico, "etl_socioeconomico.py"),
    (load, "load.py")
]

# -----------------------------------------------------------------------------
# Função Principal do Orquestrador
# -----------------------------------------------------------------------------

def main():
    """
    Executa cada script do pipeline em sequência, parando se
    um deles falhar.
    """
    print("========= INICIANDO ORQUESTRADOR DO PIPELINE DE ETL =========")
    start_time_total = time.time()
    num_tarefas = len(TAREFAS_PIPELINE)
    
    # 3. Iterar sobre a lista de tarefas
    for i, (modulo, nome_script) in enumerate(TAREFAS_PIPELINE):
        
        print(f"\n[PASSO {i+1}/{num_tarefas}] Executando: {nome_script}")
        print("-" * (len(nome_script) + 24)) # Linha decorativa
        
        start_time_script = time.time()
        
        try:
            # 4. Chamar a função 'main()' do módulo importado
            modulo.main()
            
            # Medir o tempo de execução do script
            end_time_script = time.time()
            print(f"--- Sucesso! ({nome_script} demorou {end_time_script - start_time_script:.2f}s)")
        
        except Exception as e:
            # 5. Se qualquer script falhar, parar o pipeline
            print(f"\n!!!!!!!! ERRO CRÍTICO NO PIPELINE !!!!!!!!")
            print(f"Falha ao executar o script: {nome_script}")
            print(f"Erro: {e}")
            print("Pipeline interrompido para evitar mais erros.")
            sys.exit() # Termina o orquestrador
    
    # 6. Se tudo correu bem
    end_time_total = time.time()
    print("\n============================================================")
    print(f"TODAS AS {num_tarefas} TAREFAS CONCLUÍDAS COM SUCESSO!")
    print(f"Tempo total de execução: {end_time_total - start_time_total:.2f}s")
    print("============================================================")


if __name__ == "__main__":
    main()