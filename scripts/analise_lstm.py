""" 
Esse script baseia-se em responder a questão 3: 

Analisando as séries históricas de casos e de dados climáticos, é possível identificar um padrão que antecede os surtos de dengue? Quais combinações de fatores (ex: aumento de chuva e temperatura) historicamente indicam um maior risco de epidemia para um município nas semanas seguintes? 

A ideia do código abaixo tem como objetivo treinar os dados com utilização de uma rede neural recorrente, treinando-a com dados de casos de dengue, média de temperaturas e de chuva (precipitação) e gerar gráficos onde será possível observar como o treinamento comportar-se e realizar uma previsão de ocorrências futuras,

""" 
# =============================================================================
import sys
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error
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
}
# =============================================================================
print("Carregando arquivos CSV para DataFrames...")
print("=" * NUM_EQUALS)

try:
    # Carrega as dimensões
    df_local = pd.read_csv(path_to_csv + files['local'], sep=';')
    df_tempo = pd.read_csv(path_to_csv + files['tempo'], sep=';')

    # Carrega as tabelas Fato
    df_casos = pd.read_csv(path_to_csv + files['casos'], sep=';')
    df_clima = pd.read_csv(path_to_csv + files['clima'], sep=';')

    print("Arquivos carregados com sucesso.\n")
except FileNotFoundError as e:
    print(f"ERRO: Arquivo não encontrado. Verifique o nome: {e.filename}")
    print("Script interrompido.")
    sys.exit()
except Exception as e:
    print(f"Ocorreu um erro ao ler os arquivos: {e}")
    sys.exit()
# =============================================================================

df_casos = pd.merge(df_casos, df_local[['id_local','nome_municipio']], on='id_local')
df_casos = pd.merge(df_casos, df_tempo[['id_tempo','ano','semana_epidemiologica']], on='id_tempo')

df_clima = df_clima[
    (df_clima["temperatura_media"] >= -14 ) & (df_clima["precipitacao_total"] >= 0)
]
df_clima = pd.merge(df_clima, df_local[['id_local','nome_municipio']], on='id_local')
df_clima = pd.merge(df_clima, df_tempo[['id_tempo','ano','semana_epidemiologica']], on='id_tempo')

casos_semana_capital = df_casos.groupby(
    ['nome_municipio', 'ano','semana_epidemiologica'], as_index=False
)['num_casos'].sum()

#DataFrame principal para casos de dengue
media_semanal_casos = casos_semana_capital.groupby(
    ['ano', 'semana_epidemiologica'],as_index=False
)['num_casos'].sum()

temperatura_semanal_capital = df_clima.groupby(
    ['nome_municipio', 'ano','semana_epidemiologica'], as_index=False
)['temperatura_media'].mean().dropna()

precipitacao_semanal_capital = df_clima.groupby(
    ['nome_municipio', 'ano','semana_epidemiologica'], as_index=False
)['precipitacao_total'].sum().dropna()

#DataFrame principal para temperatura média
media_semanal_temperatura = temperatura_semanal_capital.groupby(
    ['ano', 'semana_epidemiologica'],as_index=False
)['temperatura_media'].mean()

#DataFrame principal para precipitação
media_semanal_precipitacao = precipitacao_semanal_capital.groupby(
    ['ano', 'semana_epidemiologica'],as_index=False
)['precipitacao_total'].mean()

# =============================================================================

#Classe que implementa a rede neural recorrente (LSTM)
class LSTMModel(nn.Module):
    def __init__(self, input_size=1, hidden_size=32, num_layers=1, output_size=1):
        super(LSTMModel, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers

        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, x):
        h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)

        out, _ = self.lstm(x, (h0, c0))

        out = self.fc(out[:, -1, :]) 
        return out

# =============================================================================

def sequencias(DataFrame, Tamanho_Sequencia):
    valores_analisados, valor_predito = [], []

    for i in range(Tamanho_Sequencia, len(DataFrame)):
        x = DataFrame[i-Tamanho_Sequencia:i]
        y = DataFrame[i]

        valores_analisados.append(x)
        valor_predito.append(y)

    return np.array(valores_analisados), np.array(valor_predito)

# =============================================================================

def train_and_eval_lstm(num_epocas,dataframe,df_do_momento):

    series = dataframe.values

    #Sequência de treinamento da rede
    seq = 50 if df_do_momento == 0 else 10

    scaler = MinMaxScaler()

    split_raw = int(len(series) * 0.8)   # split antes das sequências
    train_raw = series[:split_raw].reshape(-1,1)
    scaler.fit(train_raw)                # fit SÓ no treino

    series_scaled = scaler.transform(series.reshape(-1,1)).ravel()

    if seq == 50:
        X, y = sequencias(series_scaled, seq)
    else:
        X, y = sequencias(series_scaled, seq)      

    split = int(len(X) * 0.8)

    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    X_train_t = torch.tensor(X_train, dtype=torch.float32).unsqueeze(-1)
    y_train_t = torch.tensor(y_train, dtype=torch.float32).unsqueeze(-1)
    X_test_t  = torch.tensor(X_test,  dtype=torch.float32).unsqueeze(-1)
    y_test_t  = torch.tensor(y_test,  dtype=torch.float32).unsqueeze(-1)

    train_loader = DataLoader(
        TensorDataset(X_train_t, y_train_t),
        batch_size=32,
        shuffle=True
    )

    model = LSTMModel()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)

    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    #Treinamento
    print("Começando o treinamento com " + str(num_epocas) + " épocas...")

    loss_history = []

    for epoch in range(num_epocas):
        model.train()
        epoch_loss = 0

        for xb, yb in train_loader:
            xb, yb = xb.to(device), yb.to(device)

            pred = model(xb)
            loss = criterion(pred, yb)

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            epoch_loss += loss.item()

        loss_history.append(epoch_loss / len(train_loader))


    #Avaliando
    model.eval()
    with torch.no_grad():
        y_pred = model(X_test_t.to(device)).cpu().numpy()

    y_test_real = scaler.inverse_transform(y_test.reshape(-1,1))
    y_pred_real = scaler.inverse_transform(y_pred)

    mae  = mean_absolute_error(y_test_real, y_pred_real)
    rmse = mean_squared_error(y_test_real, y_pred_real) ** 0.5

    print("\n===== Avaliadores do modelo =====")
    print(f"Erro médio absoluto: {mae:.2f}")
    print(f"Erro quadrático médio: {rmse:.2f}\n")

    #Gráficos
    print("Gerando gráficos...")

    plt.figure(figsize=(10,5))
    plt.plot(y_test_real, label="Real")
    plt.plot(y_pred_real, label="Predito")
    plt.title("Real vs Predito")
    plt.legend()
    plt.grid()
    plt.show()

    plt.figure(figsize=(10,4))
    plt.plot(y_test_real - y_pred_real, label="Resíduo")
    plt.title("Erro de Predição (Resíduo)")
    plt.grid()
    plt.legend()
    plt.show()

    plt.figure(figsize=(8,4))
    plt.plot(loss_history)
    plt.title("Histórico de Perda")
    plt.xlabel("Épocas")
    plt.ylabel("Perdas")
    plt.grid()
    plt.show()

    #Prevendo dados "futuros" em 52 semanas (aprox. 1 ano)
    semanas_futuras = 52
    ultimas = series_scaled[-seq:]

    entrada = ultimas.copy()
    previsoes = []

    with torch.no_grad():
        for _ in range(semanas_futuras):
            x_input = torch.tensor(
                entrada[-seq:], dtype=torch.float32
            ).unsqueeze(0).unsqueeze(-1).to(device)

            future_scaled = model(x_input).cpu().numpy()[0,0]

            previsoes.append(future_scaled)
            entrada = np.append(entrada, future_scaled)

    previsoes = np.array(previsoes).reshape(-1,1)
    previsoes = scaler.inverse_transform(previsoes)

    plt.figure(figsize=(17,9))
    plt.plot(range(1, semanas_futuras+1), previsoes, marker='o')
    plt.title("Previsão das Próximas 52 Semanas")
    plt.xlabel("Semanas")
    plt.ylabel("Estimativa")
    plt.grid()
    plt.show()

    print("Gráficos gerados!")

dataframes = [
    media_semanal_casos['num_casos'],
    media_semanal_temperatura['temperatura_media'],
    media_semanal_precipitacao['precipitacao_total']
]

for i,df in enumerate(dataframes):
    print(NUM_EQUALS * "=")
    print("Preparando o treinamento para o dataframe", end=" ")

    print(
        "de casos de dengue nas capitais ..." if i == 0 else
        "da média de temperatura ..." if i == 1 else
        "da precipitação média ..."
    )

    train_and_eval_lstm(800,df,i) #Mandando os dados alterados para treinamento

    print(NUM_EQUALS * "=")
    if i != 2:
        print("")
