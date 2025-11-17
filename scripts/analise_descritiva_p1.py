"""
Performa uma análise descritiva dos dados
Buscando responder à pergunta 1

Quais capitais consistentemente apresentam as maiores taxas de incidência de dengue (casos por 100 mil habitantes) ao longo dos anos, e
existe uma correlação visível entre essas altas taxas e fatores climáticos (como média de temperatura e chuva) e
socioeconômicos (como saneamento básico e densidade populacional)?
"""
# =============================================================================
import sys
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
# =============================================================================
NUM_EQUALS = 40
# =============================================================================
# Caminho dos arquivos CSV

path_to_csv = "dados/processados/"

files = {
    'local': 'dim_local.csv',
    'tempo': 'dim_tempo.csv',
    'casos': 'fato_casos_dengue.csv',
    'clima': 'fato_clima.csv',
    'socio': 'fato_socioeconomico.csv'
}
# =============================================================================
# CARREGAMENTO DOS DADOS

print("Carregando arquivos CSV para DataFrames...")
print("=" * NUM_EQUALS)

try:
    # Carrega as dimensões
    df_local = pd.read_csv(path_to_csv + files['local'], sep=';')
    df_tempo = pd.read_csv(path_to_csv + files['tempo'], sep=';')

    # Carrega as tabelas Fato
    df_casos = pd.read_csv(path_to_csv + files['casos'], sep=';')
    df_clima = pd.read_csv(path_to_csv + files['clima'], sep=';')
    df_socio = pd.read_csv(path_to_csv + files['socio'], sep=';')

    print("Arquivos carregados com sucesso.\n")

except FileNotFoundError as e:
    print(f"ERRO: Arquivo não encontrado. Verifique o nome: {e.filename}")
    print("Script interrompido.")

    sys.exit()

except Exception as e:
    print(f"Ocorreu um erro ao ler os arquivos: {e}")

    sys.exit()
# =============================================================================
# PREPARAÇÃO E AGREGAÇÃO DOS DADOS

# Desafio: Casos e Clima são mensais, mas Socioeconômico é anual
# É preciso agregá-los em uma base anual para responder à pergunta

print("Iniciando transformação dos dados (ETL)...")

# Une-se com df_tempo para obter o 'ano'

# Agrupa-se por local e ano, somando os casos
casos_com_ano = pd.merge(df_casos, df_tempo[['id_tempo', 'ano']], on='id_tempo')

casos_anual = casos_com_ano.groupby(['id_local', 'ano']).agg(
                                                            num_casos=('num_casos', 'sum')
                                                        ).reset_index()

# Agrupa-se por local e ano, calculando a média das temperaturas e somando as precipitações
clima_com_ano = pd.merge(df_clima, df_tempo[['id_tempo', 'ano']], on='id_tempo')

clima_anual = clima_com_ano.groupby(['id_local', 'ano']).agg(
                                                            temperatura_media_anual=('temperatura_media', 'mean'),
                                                            precipitacao_soma_anual=('precipitacao_total', 'sum')
                                                        ).reset_index()

socio_com_ano = pd.merge(df_socio, df_tempo[['id_tempo', 'ano']], on='id_tempo')

# Seleciona-se as colunas relevantes
socio_anual = socio_com_ano[['id_local', 'ano', 'num_populacao', 'densidade_demografica', 'num_esgoto', 'num_agua_tratada']]
# =============================================================================
# CRIAÇÃO DO DATAFRAME ANUAL

print("Consolidando dados anuais...\n")

# Une-se casos, clima e dados socioeconômicos
df_final = pd.merge(casos_anual, clima_anual, on=['id_local', 'ano'], how='inner')
df_final = pd.merge(df_final, socio_anual, on=['id_local', 'ano'], how='inner')

# Por fim, une-se com df_local para obter os nomes das capitais
df_final = pd.merge(df_final, df_local[['id_local', 'nome_municipio', 'uf']], on='id_local', how='inner')
# =============================================================================
# TAXA DE INCIDÊNCIA

print("=" * NUM_EQUALS)
print("Capitais com maiores taxas de incidência")
print("=" * NUM_EQUALS)

# Cálculo da taxa de incidência por 100 mil habitantes
df_final['taxa_incidencia'] = (df_final['num_casos'] / df_final['num_populacao']) * 100000

# Ordena os dados para melhor visualização
df_final = df_final.sort_values(by=['ano', 'taxa_incidencia'], ascending=[True, False])

# Exibe "Top 5" de incidência para cada ano no período
print("Top 5 a cada ano:\n")

anos_disponiveis = df_final['ano'].unique()
anos_disponiveis.sort()

for ano in anos_disponiveis:
    print(f"--- Ano: {ano} ---")

    top_5 = df_final[df_final['ano'] == ano].head(5)

    print(top_5[['nome_municipio', 'uf', 'num_casos', 'num_populacao', 'taxa_incidencia']].to_string(index=False))
    print("\n")

# Capitais consistentemente altas
# Calcula-se a média da taxa no período
print("--- Ranking Geral (média da taxa de incidência no período) ---")

# ranking_geral = df_final.groupby(['nome_municipio', 'uf'])['taxa_incidencia'].mean().sort_values(ascending=False)
ranking_geral = df_final.groupby(['nome_municipio', 'uf']).agg(
                                                               taxa_incidencia_media=('taxa_incidencia', 'mean')
                                                          ).sort_values(by=['taxa_incidencia_media'],ascending=False)

print(ranking_geral.head(10).to_string())
# =============================================================================
# CORRELAÇÃO

print("\n")
print("=" * NUM_EQUALS)
print("Correlação com fatores climáticos e socioeconômicos")
print("=" * NUM_EQUALS)

# Seleciona as colunas de interesse para a correlação
colunas_correlacao = [
    'taxa_incidencia',
    'temperatura_media_anual',
    'precipitacao_soma_anual',
    'densidade_demografica',
    'num_esgoto',
    'num_agua_tratada'
]

# Renomeia colunas para melhor visualização no gráfico
df_correlacao = df_final[colunas_correlacao].rename(columns={
    'taxa_incidencia': 'Taxa Incidência',
    'temperatura_media_anual': 'Temp. Média',
    'precipitacao_soma_anual': 'Chuva Total',
    'densidade_demografica': 'Densidade Pop.',
    'num_esgoto': '% esgoto',
    'num_agua_tratada': '% Água Tratada'
})

# Calcula a matriz de correlação (Pearson)
print("\nExibindo gráfico de correlação (heatmap)...")

matriz_corr = df_correlacao.corr()

plt.figure(figsize=(10, 7))

sns.heatmap(
    matriz_corr,
    annot=True,     # Mostrar os números dentro dos quadrados
    cmap='coolwarm',# Esquema de cores (azul = fraco/negativo, vermelho = forte/positivo)
    fmt=".2f",      # Formatar os números com 2 casas decimais
    linewidths=.5
)

plt.title('Correlação entre Taxa de Incidência de Dengue e Fatores Socioeconômicos/Climáticos (Anual)')
plt.tight_layout()

# Exibe o gráfico
plt.show()

print("\nAnálise concluída.")
