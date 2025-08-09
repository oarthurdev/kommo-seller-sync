
# Sistema de Sincronização Kommo CRM

Este projeto implementa um sistema robusto de sincronização contínua entre o Kommo CRM e uma base de dados Supabase, com funcionalidades avançadas de gamificação e análise de desempenho para corretores.

## 🚀 Funcionalidades Principais

### 1. Sincronização Automática e Contínua
- **Sincronização Multi-empresa**: Suporte simultâneo para múltiplas empresas com configurações independentes
- **Sync Incremental**: Otimização que sincroniza apenas dados novos ou alterados
- **Rate Limiting Inteligente**: Respeita os limites da API Kommo (7 req/s) com monitoramento automático
- **Recuperação de Falhas**: Sistema resiliente com retry automático e backoff exponencial

### 2. Processamento de Dados
O sistema sincroniza os seguintes tipos de dados:

#### Corretores (Brokers)
- Informações pessoais e profissionais
- Status ativo/inativo
- Cargos e permissões

#### Leads
- Dados completos dos leads
- Histórico de mudanças de status
- Informações de contato e pipeline
- Valores de negociação

#### Atividades
- Mudanças de status dos leads
- Mensagens enviadas e recebidas
- Alterações de responsável
- Histórico completo de interações

#### Etapas (Stages)
- Estrutura completa dos pipelines
- Posicionamento das etapas
- Mapeamento de status

### 3. Sistema de Gamificação
Calcula automaticamente pontuações baseadas em regras configuráveis:

- **Leads Visitados**: Pontuação por interações com leads
- **Propostas Enviadas**: Pontos por propostas comerciais
- **Vendas Realizadas**: Bonificação por fechamentos
- **Leads Perdidos**: Penalização por oportunidades perdidas

### 4. Métricas Dinâmicas
- Configuração flexível de metas por empresa
- Acompanhamento de performance em tempo real
- Relatórios automáticos de atingimento de objetivos

### 5. Webhook Integration
- Processamento em tempo real de mensagens do Kommo
- Vinculação automática de mensagens aos corretores responsáveis
- Suporte para diferentes tipos de webhook (mensagens, mudanças de status, etc.)

## 🏗️ Arquitetura do Sistema

### Componentes Principais

#### `sync_api.py`
Servidor Flask principal que gerencia:
- API REST para controle da sincronização
- Worker threads para cada empresa
- Monitoramento de status global
- Processamento de webhooks

#### `libs/kommo_api.py`
Cliente para interação com a API do Kommo:
- Autenticação Bearer Token
- Paginação automática
- Rate limiting integrado
- Processamento de diferentes tipos de dados

#### `libs/supabase_db.py`
Gerenciador da base de dados:
- Operações CRUD otimizadas
- Cálculo de pontuações
- Gerenciamento de configurações
- Processamento de métricas

#### `libs/sync_manager.py`
Coordenador da sincronização:
- Detecção de mudanças (hash comparison)
- Processamento em batches
- Validação de integridade de dados
- Snapshots periódicos

#### `libs/rate_limit_monitor.py`
Monitor de limites da API:
- Controle de 7 req/s
- Backoff inteligente
- Tratamento de erros 429/403/504

## 🛠️ Configuração e Instalação

### Variáveis de Ambiente Necessárias

```bash
# Supabase Configuration
VITE_SUPABASE_URL=your_supabase_url
VITE_SUPABASE_ANON_KEY=your_supabase_anon_key

# Kommo CRM Configuration (opcional - pode ser configurado via database)
KOMMO_API_URL=your_kommo_subdomain.kommo.com
ACCESS_TOKEN_KOMMO=your_access_token
```

### Instalação

1. **Clone o repositório**
```bash
git clone <repository-url>
cd <project-directory>
```

2. **Instale as dependências**
```bash
pip install -r requirements.txt
```

3. **Configure as variáveis de ambiente**
- Crie um arquivo `.env` com as variáveis necessárias
- Ou configure diretamente no Replit Secrets

4. **Execute o sistema**
```bash
python sync_api.py
```

## 📊 Configuração de Empresas

### Via Base de Dados (Recomendado)

Adicione registros na tabela `kommo_config`:

```sql
INSERT INTO kommo_config (
    api_url,
    access_token,
    company_id,
    pipeline_id,
    active,
    sync_interval
) VALUES (
    'https://yoursubdomain.kommo.com/api/v4',
    'your_access_token',
    123456,
    '[789, 790, 791]',  -- IDs dos pipelines a sincronizar
    true,
    3  -- Intervalo em minutos
);
```

### Configuração de Regras de Pontuação

Personalize as regras na tabela `rules`:

```sql
INSERT INTO rules (coluna_nome, pontos, company_id) VALUES
('leads_visitados', 10, 123456),
('propostas_enviadas', 25, 123456),
('vendas_realizadas', 100, 123456),
('leads_perdidos', -15, 123456);
```

## 🔄 API Endpoints

### Status da Sincronização
```http
GET /status
```
Retorna o status detalhado de todas as sincronizações ativas.

### Controle de Sincronização
```http
POST /restart/{company_id}
```
Reinicia a sincronização para uma empresa específica.

```http
POST /stop/{company_id}
```
Para a sincronização para uma empresa específica.

### Webhook
```http
POST /webhook
```
Endpoint para receber webhooks do Kommo CRM.

## 📈 Monitoramento e Logs

### Logs Estruturados
O sistema gera logs detalhados incluindo:
- Timestamps de todas as operações
- Contadores de registros processados
- Erros e warnings com contexto
- Performance metrics

### Métricas de Performance
- Tempo de sincronização por empresa
- Taxa de sucesso das requisições
- Volumes de dados processados
- Health checks automáticos

## 🔧 Configurações Avançadas

### Rate Limiting
```python
SYNC_CONFIG = {
    'base_interval': 180,      # 3 minutos entre sincronizações
    'max_interval': 900,       # 15 minutos máximo
    'min_interval': 60,        # 1 minuto mínimo
    'api_rate_limit': 0.143,   # ~7 req/s
    'max_retries': 3,
    'backoff_multiplier': 1.5
}
```

### Paginação Segura
```python
limits = {
    'max_pages_per_request': 500,
    'max_total_records': 50000,
    'page_size': 100,
    'delay_between_pages': 0.0
}
```

## 🚦 Docker Support

### Dockerfile Incluído
```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt
COPY . .
EXPOSE 5002
CMD ["python", "sync_api.py"]
```

### Docker Compose
```bash
docker-compose up -d
```

## 📋 Estrutura de Tabelas Principais

### kommo_config
Configurações por empresa (API URLs, tokens, pipelines)

### brokers
Dados dos corretores

### leads
Informações dos leads e oportunidades

### activities
Histórico de atividades e interações

### broker_points
Pontuações calculadas por corretor

### stages_list
Estrutura de pipelines e etapas

### from_webhook
Mensagens recebidas via webhook

## 🔍 Troubleshooting

### Problemas Comuns

1. **Rate Limit Exceeded**
   - O sistema aguarda automaticamente e reativa
   - Verificar logs para detalhes

2. **Dados Inconsistentes**
   - Sistema valida foreign keys automaticamente
   - Filtra registros inválidos

3. **Falhas de Conexão**
   - Retry automático com backoff
   - Logs detalhados para debugging

### Logs Importantes
```bash
# Ver status em tempo real
tail -f sync.log | grep "Status:"

# Verificar erros
grep "ERROR" sync.log

# Monitorar performance
grep "completed in" sync.log
```

## 🤝 Contribuição

Para contribuir com o projeto:

1. Fork o repositório
2. Crie uma branch para sua feature
3. Implemente as mudanças
4. Teste thoroughly
5. Submita um Pull Request

## 📄 Licença

Este projeto está sob licença [especificar licença].

## 📞 Suporte

Para suporte técnico:
- Abra uma issue no repositório
- Consulte os logs detalhados
- Verifique a documentação da API Kommo

---

**Desenvolvido para otimizar a gestão de CRM e gamificação de equipes de vendas**
