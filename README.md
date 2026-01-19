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

## 🛠️ Instalação e Configuração

### 1. Pré-requisitos

- Python >= 3.9
- Conta e projeto Supabase (https://supabase.com)
- Conta e acesso ao Kommo CRM com API habilitada
- Docker (Opcional, para deploy simplificado)

### 2. Instalação

Clone o repositório do projeto: