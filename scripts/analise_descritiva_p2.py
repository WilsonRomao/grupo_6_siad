"""
Performa uma análise descritiva dos dados
Buscando responder as perguntas 2
"""
# =============================================================================
import sys
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
# =============================================================================
NUM_TABS = 40
# =============================================================================
# Caminho dos arquivos CSV

path_to_csv = "dados/processados/"

files = {
    'tempo': 'dim_tempo.csv',
    'casos': 'fato_casos_dengue.csv',
}
# =============================================================================
# 1. Carregamento dos dados

print("Carregando arquivos CSV para DataFrames...")
print("=" * NUM_TABS)

try:
    # Carrega as dimensões
    df_tempo = pd.read_csv(path_to_csv + files['tempo'], sep=';')
    
    # Carrega as tabelas Fato
    df_casos = pd.read_csv(path_to_csv + files['casos'], sep=';')
    
    print("Arquivos carregados com sucesso.")
    
except FileNotFoundError as e:
    print(f"ERRO: Arquivo não encontrado. Verifique o nome: {e.filename}")
    print("Script interrompido.")

    sys.exit()

except Exception as e:
    print(f"Ocorreu um erro ao ler os arquivos: {e}")

    sys.exit()
# =============================================================================
# Configura o estilo dos gráficos
sns.set_theme(style="whitegrid")
# =============================================================================
# 2. Perfil demográfico
print("\n")
print("Perfil demográfico (genero e faixa etária)")
print("=" * NUM_TABS)

# Análise por gênero
print("Analisando perfil por gênero...")

total_masculino = df_casos['num_masculino'].sum()
total_feminino = df_casos['num_feminino'].sum()

total_genero = total_masculino + total_feminino

# Gráfico de Pizza por Genero
plt.figure(figsize=(7, 5))

labels_genero = ['Feminino', 'Masculino']
sizes_genero = [total_feminino, total_masculino]

plt.pie(sizes_genero, labels=labels_genero, autopct='%1.1f%%', startangle=90, colors=['#FF9999', '#66B2FF'])

plt.axis('equal') # Garante que o gráfico seja um círculo
plt.title('Distribuição de Casos de Dengue por Genero (Total do Período)')

# Análise por Faixa Etária
print("Analisando perfil por faixa etária...")

perfis_etarios = {
    'Crianças': df_casos['num_criancas'].sum(),
    'Adolescentes': df_casos['num_adolescentes'].sum(),
    'Adultos': df_casos['num_adultos'].sum(),
    'Idosos': df_casos['num_idosos'].sum()
}

# Converte para um DataFrame para facilitar a ordenação e plotagem
df_etario = pd.DataFrame(perfis_etarios.items(), columns=['Faixa Etária', 'Total de Casos'])

df_etario = df_etario.sort_values(by='Total de Casos', ascending=False)

# Gráfico de Barras por Faixa Etária
plt.figure(figsize=(10, 6))

sns.barplot(x='Total de Casos', y='Faixa Etária', data=df_etario)

plt.title('Perfil de Casos de Dengue por Faixa Etária (Total do Período)')

plt.xlabel('Número Total de Casos')
plt.ylabel('Faixa Etária')

# =============================================================================
# 3. Sazonalidade e Picos
print("\n")
print("Sazonalidade e Picos de Notificação")
print("=" * NUM_TABS)

# Precisamos juntar os casos com o tempo para analisar a sazonalidade
df_sazonal = pd.merge(
    df_casos[['id_tempo', 'num_casos']], 
    df_tempo[['id_tempo', 'ano', 'mes', 'semana_epidemiologica']], 
    on='id_tempo'
)

# Análise de sazonalidade
print("Agregando dados por mês e ano...")

# Agrupa os casos por ano e mês
casos_mensais = df_sazonal.groupby(['ano', 'mes'])['num_casos'].sum().reset_index()

# Gráfico de Linha da Sazonalidade Mensal
plt.figure(figsize=(12, 7))

sns.lineplot(
    data=casos_mensais, 
    x='mes', 
    y='num_casos', 
    hue='ano',  # Uma linha para cada ano
    palette='Spectral', 
    linewidth=2.5,
    marker='o'
)

plt.title('Sazonalidade da Dengue: Casos Totais por Mês (2017-2022)')

plt.xlabel('Mês do Ano')
plt.ylabel('Número Total de Casos')
plt.xticks(range(1, 13)) # Garante que todos os 12 meses sejam mostrados

plt.legend(title='Ano')

# Análise de picos de notificação (semana epidemiológica)
print("Agregando dados por semana epidemiológica e ano...")

# Agrupa os casos por ano e semana epidemiológica
casos_semanais = df_sazonal.groupby(['ano', 'semana_epidemiologica'])['num_casos'].sum().reset_index()

# Gráfico de linha dos picos de notificação
plt.figure(figsize=(14, 7))

sns.lineplot(
    data=casos_semanais, 
    x='semana_epidemiologica', 
    y='num_casos', 
    hue='ano', # Uma linha para cada ano
    palette='coolwarm', 
    linewidth=2
)

plt.title('Picos de Notificação de Dengue por Semana Epidemiológica (2017-2022)')

plt.xlabel('Semana Epidemiológica (1-53)')
plt.ylabel('Número Total de Casos')

plt.xlim(1, 53) # Define o limite do eixo X

plt.legend(title='Ano')

# 4. Exibição dos Gráficos
print("\n")
print("Exibindo todos os gráficos gerados...")
print("=" * NUM_TABS)

plt.tight_layout() # Ajusta os gráficos para evitar sobreposição
plt.show()

print("\n")
print("Análise concluída.")
