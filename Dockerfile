FROM python:3.9-slim

WORKDIR /app

# Instala somente o necessário (ajuste os pacotes se precisar compilar dependências C)
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copia apenas o requirements para aproveitar o cache do Docker
COPY requirements.txt .

# Atualiza o pip e instala dependências
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copia o restante da aplicação
COPY . .

# Expõe a porta padrão do Streamlit
EXPOSE 5002

# Comando de entrada

CMD ["python", "sync_api.py"]
