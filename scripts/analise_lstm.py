"""
Esse script baseia-se em responder a questão 3:

Analisando as séries históricas de casos e de dados climáticos, é possível identificar um padrão que antecede os surtos de dengue? Quais combinações de fatores (ex: aumento de chuva e temperatura) historicamente indicam um maior risco de epidemia para um município nas semanas seguintes?

Haverá dois scripts para essa pergunta: um que irá predizer os dados através das métricas de Machine Learning (Regressão Linear e SVR) e outro que irá predizer através de Redes Neurais Recorrentes (LSTM). Esse é o baseado no LSTM.
"""

# =============================================================================
import sys
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error
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

df_clima = pd.merge(df_clima, df_local[['id_local','nome_municipio']], on='id_local')
df_clima = pd.merge(df_clima, df_tempo[['id_tempo','ano','semana_epidemiologica']], on='id_tempo')

casos_semana_capital = df_casos.groupby(['nome_municipio', 'ano','semana_epidemiologica'], as_index=False)['num_casos'].sum()

#DataFrame principal para casos de dengue
media_semanal_casos = casos_semana_capital.groupby(['ano', 'semana_epidemiologica'],as_index=False)['num_casos'].sum()

temperatura_semanal_capital = df_clima.groupby(['nome_municipio', 'ano','semana_epidemiologica'], as_index=False)['temperatura_media'].sum().dropna()

precipitacao_semanal_capital = df_clima.groupby(['nome_municipio', 'ano','semana_epidemiologica'], as_index=False)['precipitacao_total'].sum().dropna()

#DataFrame principal para temperatura média
media_semanal_temperatura = temperatura_semanal_capital.groupby(['ano', 'semana_epidemiologica'],as_index=False)['temperatura_media'].mean()

#DataFrame principal para precipitação
media_semanal_precipitacao = precipitacao_semanal_capital.groupby(['ano', 'semana_epidemiologica'],as_index=False)['precipitacao_total'].sum()

# =============================================================================



class LSTMModel(nn.Module):
    def __init__(self, input_size=1, hidden_size=100, num_layers=5, output_size=1):
        super(LSTMModel, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers

        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, x):
        h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)

        # Executa a LSTM sobre a sequência de entrada `x`, utilizando os estados iniciais h0 e c0
        # `out` contém todas as saídas da LSTM para cada passo da sequência (shape: [batch_size, seq_len, hidden_size])
        # O segundo retorno (_) seria a tupla (h_n, c_n) com os estados finais, que não estamos usando aqui
        out, _ = self.lstm(x, (h0, c0))

        # Seleciona apenas a saída da última posição da sequência (out[:, -1, :])
        # Essa saída representa a "memória final" da LSTM após ver toda a sequência
        # Em seguida, passa essa saída pela camada linear final (fc) para gerar a predição
        out = self.fc(out[:, -1, :])  # usa apenas a última saída da sequência
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

def train_and_eval_lstm(num_epocas,dataframe):
    series = dataframe.values

    # Normalização
    scaler = MinMaxScaler()
    series_scaled = scaler.fit_transform(series.reshape(-1,1)).ravel()

    # Transformar para sequência (supervisionado)
    X, y = sequencias(series_scaled, 4)


    # Split
    split = int(len(X) * 0.8)
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    # Tensores
    X_train_t = torch.tensor(X_train, dtype=torch.float32).unsqueeze(-1)  # (batch, seq, 1)
    y_train_t = torch.tensor(y_train, dtype=torch.float32).unsqueeze(-1)
    X_test_t = torch.tensor(X_test, dtype=torch.float32).unsqueeze(-1)
    y_test_t = torch.tensor(y_test, dtype=torch.float32).unsqueeze(-1)

    # DataLoader
    train_loader = DataLoader(TensorDataset(X_train_t, y_train_t), batch_size=32, shuffle=True)

    # Modelo
    model = LSTMModel()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)

    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    # Treinamento
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
        

    model.eval()
    with torch.no_grad():
        y_pred = model(X_test_t.to(device)).cpu().numpy()

    y_test_real = scaler.inverse_transform(y_test.reshape(-1,1))
    y_pred_real = scaler.inverse_transform(y_pred)

    # Gráfico Real vs Predito
    plt.figure(figsize=(10,5))
    plt.plot(y_test_real, label="Real")
    plt.plot(y_pred_real, label="Predito")
    plt.title("Casos de Dengue — Real vs Predito")
    plt.legend()
    plt.grid()
    plt.show()

    # Resíduos
    plt.figure(figsize=(10,4))
    plt.plot(y_test_real - y_pred_real, label="Resíduo")
    plt.title("Erro de Predição (Resíduo)")
    plt.grid()
    plt.legend()
    plt.show()

    # Histórico de perda
    plt.figure(figsize=(8,4))
    plt.plot(loss_history)
    plt.title("Histórico de Perda")
    plt.xlabel("Epoch")
    plt.ylabel("Loss")
    plt.grid()
    plt.show()

    FUTURO = 52
    ultimas_4 = series_scaled[-4:]
    entrada = ultimas_4.copy()
    previsoes = []

    with torch.no_grad():
        for _ in range(FUTURO):
            x_input = torch.tensor(entrada[-4:], dtype=torch.float32).unsqueeze(0).unsqueeze(-1).to(device)
            future_scaled = model(x_input).cpu().numpy()[0,0]
            previsoes.append(future_scaled)
            entrada = np.append(entrada, future_scaled)

    previsoes = np.array(previsoes).reshape(-1,1)
    previsoes = scaler.inverse_transform(previsoes)

    plt.figure(figsize=(11,5))
    plt.plot(range(1, FUTURO+1), previsoes, marker='o')
    plt.title("Previsão das Próximas 104 Semanas")
    plt.xlabel("Semanas")
    plt.ylabel("Casos Estimados")
    plt.grid()
    plt.show()

print(NUM_EQUALS * "=")
print("Preparando o treinamento para casos de dengue nas capitais ...")
train_and_eval_lstm(200,media_semanal_casos['num_casos'])
print(NUM_EQUALS * "=")
print("")
dataframes = [media_semanal_temperatura['temperatura_media'],media_semanal_precipitacao['precipitacao_total']]
for i,df in enumerate(dataframes):
    print(NUM_EQUALS * "=")
    print("Preparando o treinamento para o dataframe", end=" ")
    print("da média da temperatura ..." if i == 0 else "da precipitação total ...")
    train_and_eval_lstm(300,df)
    print(NUM_EQUALS * "=")
    if i == 0:
        print("")
