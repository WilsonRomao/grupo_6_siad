# -*- coding: utf-8 -*-
"""
Pipeline de Criação de Dimensões (Tempo e Local)

Este script é responsável por gerar e persistir as tabelas de dimensão
usadas no pipeline de ETL da Dengue.

1.  Cria a 'dim_tempo' baseada num intervalo de anos.
2.  Cria a 'dim_local' a partir de um arquivo do IBGE, filtrando e
    tratando ambiguidades para manter apenas as 27 capitais.
3.  Salva ambas as dimensões como arquivos CSV na pasta 'processados'.
"""

import os
import pandas as pd
import numpy as np

# Tenta importar o 'drive' do Colab, mas não quebra se não estiver no Colab
try:
    from google.colab import drive
except ImportError:
    print("Aviso: Módulo 'google.colab.drive' não encontrado. "
          "A saltar a montagem do drive. Certifique-se que os caminhos são acessíveis.")
    drive = None

# =============================================================================
# 1. CONFIGURAÇÃO E CONSTANTES
# =============================================================================
# Centralizar todas as configurações aqui facilita a manutenção.

# --- Caminhos (Paths) ---
# Caminho base onde estão as pastas 'brutos' e 'processados'
CAMINHO_BASE = 'dados'
PATH_PROCESSADOS = os.path.join(CAMINHO_BASE, 'processados')
PATH_BRUTOS = os.path.join(CAMINHO_BASE, 'brutos')

# Caminho de entrada do arquivo XLS do IBGE
PATH_BRUTOS_LOCAL = os.path.join(PATH_BRUTOS, 'local') 
PATH_RAW_LOCAL = os.path.join(PATH_BRUTOS_LOCAL,'RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.xls')

# Caminhos de saída para as dimensões
PATH_DIM_TEMPO_SAIDA = os.path.join(PATH_PROCESSADOS, 'dim_tempo.csv')
PATH_DIM_LOCAL_SAIDA = os.path.join(PATH_PROCESSADOS, 'dim_local.csv')

# --- Configurações da Dim_Tempo ---
ANO_INICIO = 2017
ANO_FIM = 2022

# --- Configurações da Dim_Local ---
# Lista das 27 capitais que queremos manter
LISTA_CAPITAIS = [
    'Aracaju', 'Belém', 'Belo Horizonte', 'Boa Vista', 'Brasília',
    'Campo Grande', 'Cuiabá', 'Curitiba', 'Florianópolis', 'Fortaleza',
    'Goiânia', 'João Pessoa', 'Macapá', 'Maceió', 'Manaus', 'Natal',
    'Palmas', 'Porto Alegre', 'Porto Velho', 'Recife', 'Rio Branco',
    'Rio de Janeiro', 'Salvador', 'São Luís', 'São Paulo', 'Teresina',
    'Vitória'
]

# Colunas que queremos extrair do arquivo do IBGE
COLUNAS_IBGE_RAW = [
    'Nome_UF',
    'Código Município Completo',
    'Nome_Município'
]

# Renomeia colunas para o padrão final do DW
MAPA_RENOMEAR_LOCAL = {
    'Nome_UF': 'uf',
    'Nome_Município': 'nome_municipio',
    'Código Município Completo': 'cod_municipio'
}

# REGRA DE NEGÓCIO PARA DESAMBIGUIDADE
# Mapeia nomes de capitais ambíguos para seus estados (UF) corretos.
# Isto substitui a remoção manual por índices, tornando o script robusto.
MAPA_CAPITAIS_AMBIGUAS = {
    'Belém': 'Pará',
    'Boa Vista': 'Roraima',
    'Campo Grande': 'Mato Grosso do Sul',
    'Palmas': 'Tocantins',
    'Rio Branco': 'Acre'
}


# =============================================================================
# FUNÇÃO AUXILIAR - AMBIENTE COLAB
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


# =============================================================================
# ETAPA 1: CRIAÇÃO DA DIMENSÃO TEMPO
# =============================================================================

def criar_dimensao_tempo(ano_inicio, ano_fim):
    """
    Cria um DataFrame de dimensão de tempo com granularidade diária,
    incluindo ano/semana civil e epidemiológica.

    Entrada:
    - ano_inicio (int): O primeiro ano da dimensão (ex: 2017).
    - ano_fim (int): O último ano da dimensão (ex: 2022).

    Saída:
    - (pd.DataFrame): DataFrame da dim_tempo, pronto para ser salvo.
    """
    print(f"Iniciando criação da dim_tempo (Anos: {ano_inicio}-{ano_fim})...")
    
    # 1. Criar o range de datas completo
    start_date = f'{ano_inicio}-01-01'
    end_date = f'{ano_fim}-12-31'
    datas = pd.date_range(start=start_date, end=end_date)
    
    dim_tempo = pd.DataFrame({'data_completa': datas})

    # 2. Extrair atributos de data civil (Ano, Mês, Dia)
    dim_tempo['ano_civil'] = dim_tempo['data_completa'].dt.year
    dim_tempo['mes_civil'] = dim_tempo['data_completa'].dt.month
    dim_tempo['dia_civil'] = dim_tempo['data_completa'].dt.day

    # 3. Calcular atributos epidemiológicos (Ano e Semana)
    # A lógica lida com a "semana 0" (dias no início de janeiro
    # que podem pertencer à última semana do ano anterior).
    
    # strftime('%U') retorna a semana do ano (00-53), começando no Domingo.
    semana_ano_atual = dim_tempo['data_completa'].dt.strftime('%U').astype(int)
    eh_semana_zero = (semana_ano_atual == 0)

    # Ano Epidemiológico: Se for semana 0, pertence ao ano anterior
    dim_tempo['ano_epidemiologico'] = np.where(
        eh_semana_zero,
        dim_tempo['ano_civil'] - 1,  # Verdadeiro: Ano anterior
        dim_tempo['ano_civil']       # Falso: Ano atual
    )

    # Semana Epidemiológica:
    # Se for semana 0, busca qual foi a última semana (ex: 52 ou 53) do ano anterior
    ultima_semana_ano_anterior = pd.to_datetime(
        dim_tempo['ano_epidemiologico'].astype(str) + '-12-31'
    ).dt.strftime('%U').astype(int)
    
    dim_tempo['semana_epidemiologica'] = np.where(
        eh_semana_zero,
        ultima_semana_ano_anterior, # Verdadeiro: Última semana do ano anterior
        semana_ano_atual            # Falso: Semana do ano atual
    )

    # 4. Criar a Chave Primária (PK)
    dim_tempo['id_tempo'] = dim_tempo.index + 1

    # 5. Reordenar colunas para o schema final
    colunas_ordenadas = [
        'id_tempo', 'data_completa', 'ano_civil', 'mes_civil', 'dia_civil',
        'ano_epidemiologico', 'semana_epidemiologica'
    ]
    dim_tempo = dim_tempo[colunas_ordenadas]
    
    print(f"dim_tempo criada com {len(dim_tempo)} linhas.")
    return dim_tempo


# =============================================================================
# ETAPA 2: CRIAÇÃO DA DIMENSÃO LOCAL
# =============================================================================

def _resolver_ambiguidade(row, mapa_ambiguas):
    """
    Função auxiliar para aplicar a regra de negócio de desambiguação.
    Retorna True se a linha for a capital correta, False caso contrário.
    
    Entrada:
    - row (pd.Series): Uma linha do DataFrame.
    - mapa_ambiguas (dict): O mapa de regras (ex: {'Belém': 'Pará'}).
    
    Saída:
    - (bool): True se for a capital correta, False se for uma homónima.
    """
    nome_municipio = row['Nome_Município']
    nome_uf = row['Nome_UF']
    
    if nome_municipio not in mapa_ambiguas:
        # Se o nome não está no mapa, não é ambíguo. É uma capital.
        return True
    
    # Se o nome ESTÁ no mapa, verificamos se a UF é a correta
    # Ex: É 'Belém' (município) E 'Pará' (UF)?
    return nome_uf == mapa_ambiguas[nome_municipio]


def criar_dimensao_local(raw_file_path, lista_capitais, colunas_raw, mapa_ambiguas, mapa_rename):
    """
    Cria um DataFrame de dimensão de local, focado apenas nas 27 capitais.
    Lê um arquivo XLS do IBGE, filtra pelas capitais e usa um mapa de
    regras para resolver nomes de municípios duplicados (ex: Belém, Palmas).

    Entrada:
    - raw_file_path (str): Caminho para o arquivo XLS do IBGE.
    - lista_capitais (list): Lista de nomes das 27 capitais.
    - colunas_raw (list): Colunas a serem selecionadas do XLS.
    - mapa_ambiguas (dict): Regras para resolver nomes duplicados.
    - mapa_rename (dict): Dicionário para renomear colunas.

    Saída:
    - (pd.DataFrame): DataFrame da dim_local (apenas capitais), pronto para ser salvo.
    """
    print(f"Iniciando criação da dim_local a partir de: {raw_file_path}")
    
    try:
        # 1. Extrair dados brutos do IBGE
        # 'header=6' pula as 6 primeiras linhas do XLS, que são cabeçalho
        df_ibge_raw = pd.read_excel(raw_file_path, header=6)
    except FileNotFoundError:
        print(f"ERRO: Arquivo XLS do IBGE não encontrado em: {raw_file_path}")
        raise
    except Exception as e:
        print(f"ERRO ao ler o arquivo XLS: {e}")
        raise

    # 2. Selecionar apenas as colunas de interesse
    df_local = df_ibge_raw[colunas_raw].copy()

    # 3. Primeiro filtro: Manter apenas linhas cujo nome está na lista de capitais
    df_filtrado = df_local[df_local['Nome_Município'].isin(lista_capitais)].copy()
    print(f"Arquivo IBGE filtrado, {len(df_filtrado)} linhas de capitais (incluindo homónimas).")

    # 4. Segundo filtro (Robusto): Resolver ambiguidades
    # Aplicamos a função _resolver_ambiguidade a cada linha
    df_filtrado['is_capital_real'] = df_filtrado.apply(
        _resolver_ambiguidade, 
        axis=1, 
        mapa_ambiguas=mapa_ambiguas
    )
    
    # Mantemos apenas as linhas marcadas como 'True'
    df_final_capitais = df_filtrado[df_filtrado['is_capital_real'] == True].copy()
    
    # Verificação de segurança
    if len(df_final_capitais) != 27:
        print(f"ATENÇÃO! Esperava 27 capitais, mas encontrei {len(df_final_capitais)}.")
    else:
        print("Desambiguação concluída. 27 capitais únicas isoladas.")

    # 5. Renomear colunas para o padrão do DW
    df_final_capitais.rename(columns=mapa_rename, inplace=True)

    # 6. Criar a Chave Primária (PK)
    # Ordenamos por nome para garantir que o ID seja sempre o mesmo
    df_final_capitais.sort_values('nome_municipio', inplace=True)
    df_final_capitais.reset_index(drop=True, inplace=True)
    df_final_capitais['id_local'] = df_final_capitais.index + 1

    # 7. Reordenar colunas para o schema final
    colunas_finais_local = ['id_local', 'uf', 'cod_municipio', 'nome_municipio']
    dim_local = df_final_capitais[colunas_finais_local]
    
    return dim_local


# =============================================================================
# ETAPA 3: CARGA (LOAD) / PERSISTÊNCIA
# =============================================================================

def salvar_dimensao_csv(df, output_path):
    """
    Função genérica para salvar um DataFrame de dimensão em um arquivo CSV.

    Entrada:
    - df (pd.DataFrame): A dimensão a ser salva.
    - output_path (str): Caminho completo onde o CSV será salvo.
    """
    print(f"A salvar dados em: {output_path}")
    try:
        # index=False é crucial para não salvar o índice do pandas
        # Usamos separador ';' para evitar problemas com vírgulas em decimais
        df.to_csv(output_path, index=False, sep=';')
        print(f"--- SUCESSO! ---")
        print(f"'{os.path.basename(output_path)}' salvo com {len(df)} linhas.")
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
    Função principal que orquestra todo o pipeline de criação das dimensões.
    """
    print("========= INICIANDO PIPELINE DE CRIAÇÃO DE DIMENSÕES =========")
    
    # 0. Montar o Google Drive (apenas se estiver no Colab)
    mount_google_drive()
    
    # 1. Processar e Salvar Dimensão Tempo
    try:
        dim_tempo = criar_dimensao_tempo(ANO_INICIO, ANO_FIM)
        salvar_dimensao_csv(dim_tempo, PATH_DIM_TEMPO_SAIDA)
    except Exception as e:
        print(f"Falha ao processar Dimensão Tempo: {e}")
        return # Interrompe o script se a dim_tempo falhar

    # 2. Processar e Salvar Dimensão Local
    try:
        dim_local = criar_dimensao_local(
            PATH_RAW_LOCAL,
            LISTA_CAPITAIS,
            COLUNAS_IBGE_RAW,
            MAPA_CAPITAIS_AMBIGUAS,
            MAPA_RENOMEAR_LOCAL
        )
        salvar_dimensao_csv(dim_local, PATH_DIM_LOCAL_SAIDA)
    except Exception as e:
        print(f"Falha ao processar Dimensão Local: {e}")
        return # Interrompe o script se a dim_local falhar
    
    print("\n========= PIPELINE DE DIMENSÕES CONCLUÍDO =========")


if __name__ == "__main__":
    # Esta linha garante que a função main() só seja executada
    # quando o script é chamado diretamente (python cria_dimensoes.py)
    main()