-- WARNING: This schema is for context only and is not meant to be run.
-- Table order and constraints may not be valid for execution.
CREATE TABLE public.companies (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  name text NOT NULL,
  subdomain text NOT NULL UNIQUE,
  created_at timestamp without time zone DEFAULT now(),
  CONSTRAINT companies_pkey PRIMARY KEY (id)
);

CREATE TABLE public.brokers (
  id bigint NOT NULL,
  nome text NOT NULL,
  email text,
  foto_url text,
  cargo text,
  criado_em timestamp without time zone,
  updated_at timestamp without time zone DEFAULT now(),
  active boolean DEFAULT true,
  company_id uuid,
  CONSTRAINT brokers_pkey PRIMARY KEY (id),
  CONSTRAINT fk_brokers_company FOREIGN KEY (company_id) REFERENCES public.companies(id)
);

CREATE TABLE public.activities (
  id text NOT NULL,
  lead_id bigint,
  user_id bigint,
  tipo text,
  valor_anterior text,
  valor_novo text,
  criado_em timestamp without time zone,
  dia_semana text,
  hora integer,
  updated_at timestamp without time zone DEFAULT now(),
  company_id uuid,
  status_anterior bigint,
  status_novo bigint,
  texto_mensagem character varying,
  fonte_mensagem character varying,
  texto_tarefa character varying,
  tipo_tarefa character varying,
  texto_nota character varying,
  duracao_chamada character varying,
  resultado_chamada character varying,
  texto_sms character varying,
  responsavel_anterior bigint,
  responsavel_novo bigint,
  nome_tag character varying,
  entity_type character varying,
  entity_id bigint,
  CONSTRAINT activities_pkey PRIMARY KEY (id),
  CONSTRAINT fk_activities_company FOREIGN KEY (company_id) REFERENCES public.companies(id)
);
CREATE TABLE public.broker_points (
  id bigint NOT NULL,
  nome text NOT NULL,
  pontos integer DEFAULT 0,
  leads_visitados integer DEFAULT 0,
  propostas_enviadas integer DEFAULT 0,
  vendas_realizadas integer DEFAULT 0,
  leads_perdidos integer DEFAULT 0,
  updated_at timestamp without time zone DEFAULT now(),
  company_id uuid,
  leads_descartados integer,
  CONSTRAINT broker_points_pkey PRIMARY KEY (id),
  CONSTRAINT fk_bp_broker FOREIGN KEY (id) REFERENCES public.brokers(id),
  CONSTRAINT fk_bp_company FOREIGN KEY (company_id) REFERENCES public.companies(id)
);

CREATE TABLE public.company_branding (
  id integer GENERATED ALWAYS AS IDENTITY NOT NULL,
  company_id uuid NOT NULL UNIQUE,
  primary_color text DEFAULT '#3b82f6'::text,
  secondary_color text DEFAULT '#1e40af'::text,
  accent_color text DEFAULT '#22c55e'::text,
  logo_url text,
  favicon_url text,
  company_name_display text,
  dashboard_title text DEFAULT 'Ranking de Corretores'::text,
  theme_mode text DEFAULT 'light'::text,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT company_branding_pkey PRIMARY KEY (id),
  CONSTRAINT company_branding_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id)
);
CREATE TABLE public.rules (
  id SERIAL PRIMARY KEY,
  nome text NOT NULL,
  pontos integer NOT NULL,
  coluna_nome text NOT NULL UNIQUE,
  created_at timestamp without time zone DEFAULT now(),
  updated_at timestamp without time zone DEFAULT now(),
  descricao text
);

CREATE TABLE public.company_rules (
  id integer GENERATED ALWAYS AS IDENTITY NOT NULL,
  company_id uuid NOT NULL,
  rule_id integer NOT NULL,
  pontos integer NOT NULL,
  active boolean DEFAULT true,
  created_at timestamp without time zone DEFAULT now(),
  updated_at timestamp without time zone DEFAULT now(),
  CONSTRAINT company_rules_pkey PRIMARY KEY (id),
  CONSTRAINT company_rules_rule_id_rules_id_fk FOREIGN KEY (rule_id) REFERENCES public.rules(id),
  CONSTRAINT company_rules_company_id_companies_id_fk FOREIGN KEY (company_id) REFERENCES public.companies(id)
);
CREATE TABLE public.component_filters (
  id SERIAL PRIMARY KEY,
  company_id uuid NOT NULL,
  component_name character varying NOT NULL,
  filter_type character varying NOT NULL,
  start_date timestamp without time zone,
  end_date timestamp without time zone,
  created_at timestamp without time zone DEFAULT now(),
  updated_at timestamp without time zone DEFAULT now(),
  month integer,
  year integer,
  CONSTRAINT fk_component_company FOREIGN KEY (company_id) REFERENCES public.companies(id)
);
CREATE TABLE public.custom_rules (
  id integer NOT NULL,
  company_id uuid NOT NULL,
  nome text NOT NULL,
  coluna_nome text NOT NULL,
  pontos integer NOT NULL,
  descricao text,
  active boolean DEFAULT true,
  created_at timestamp without time zone DEFAULT now(),
  updated_at timestamp without time zone DEFAULT now(),
  CONSTRAINT custom_rules_company_id_companies_id_fk FOREIGN KEY (company_id) REFERENCES public.companies(id)
);
CREATE TABLE public.dynamic_metrics (
  id SERIAL PRIMARY KEY,
  company_id uuid NOT NULL,
  nome text NOT NULL,
  pipeline_stage_id integer NOT NULL,
  pipeline_stage_name text NOT NULL,
  valor_minimo integer NOT NULL,
  cor_sucesso text DEFAULT '#22c55e'::text,
  cor_alerta text DEFAULT '#ef4444'::text,
  active boolean DEFAULT true,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT dynamic_metrics_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id)
);
CREATE TABLE public.from_webhook (
  id SERIAL PRIMARY KEY,
  webhook_type text,
  payload_id text,
  chat_id text,
  talk_id bigint,
  contact_id text,
  text text,
  created_at text,
  element_type text,
  entity_type text,
  element_id bigint,
  entity_id bigint,
  message_type text,
  author_id text,
  author_type text,
  author_name text,
  author_avatar_url text,
  origin text,
  raw_payload jsonb,
  broker_id bigint,
  lead_id bigint,
  inserted_at timestamp without time zone DEFAULT now(),
  CONSTRAINT from_webhook_broker_id_fkey FOREIGN KEY (broker_id) REFERENCES public.brokers(id)
);
CREATE TABLE public.kommo_config (
  id bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
  api_url character varying DEFAULT 'http://dicasaindaial.kommo.com/'::character varying,
  access_token character varying DEFAULT 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0a...'::character varying,
  custom_endpoint character varying,
  sync_interval integer,
  last_sync timestamp without time zone,
  next_sync timestamp without time zone,
  created_at timestamp with time zone DEFAULT now(),
  sync_start_date numeric,
  sync_end_date numeric,
  active boolean DEFAULT true,
  company_id uuid,
  pipeline_id jsonb,
  client_id character varying,
  client_secret character varying,
  refresh_token character varying,
  token_expires_at date,
  updated_at timestamp without time zone,
  CONSTRAINT kommo_config_pkey PRIMARY KEY (id),
  CONSTRAINT fk_kc_company FOREIGN KEY (company_id) REFERENCES public.companies(id)
);
CREATE TABLE public.leads (
  id bigint NOT NULL,
  nome text NOT NULL,
  responsavel_id bigint,
  contato_nome text,
  valor numeric,
  status_id bigint,
  pipeline_id bigint,
  etapa text,
  criado_em timestamp without time zone,
  atualizado_em timestamp without time zone,
  fechado boolean DEFAULT false,
  status text,
  updated_at timestamp without time zone DEFAULT now(),
  company_id uuid,
  CONSTRAINT leads_pkey PRIMARY KEY (id),
  CONSTRAINT fk_leads_broker FOREIGN KEY (responsavel_id) REFERENCES public.brokers(id),
  CONSTRAINT fk_leads_company FOREIGN KEY (company_id) REFERENCES public.companies(id)
);
CREATE TABLE public.metric_results (
  id SERIAL PRIMARY KEY,
  dynamic_metric_id integer NOT NULL,
  company_id uuid NOT NULL,
  valor_atual integer NOT NULL,
  status text NOT NULL,
  periodo_referencia text,
  leads_count integer NOT NULL DEFAULT 0,
  atingiu_meta boolean NOT NULL DEFAULT false,
  calculado_em timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT metric_results_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id),
  CONSTRAINT metric_results_dynamic_metric_id_fkey FOREIGN KEY (dynamic_metric_id) REFERENCES public.dynamic_metrics(id)
);
CREATE TABLE public.sla_metrics (
  id SERIAL PRIMARY KEY,
  company_id uuid NOT NULL DEFAULT gen_random_uuid(),
  broker_id bigint NOT NULL,
  broker_name text,
  leads_perdidos_inatividade integer DEFAULT 0,
  sla_status text CHECK (sla_status = ANY (ARRAY['OK'::text, 'ATENÇÃO'::text, 'CRÍTICO'::text])),
  calculated_at timestamp without time zone NOT NULL,
  period_start timestamp without time zone,
  period_end timestamp without time zone,
  created_at timestamp without time zone DEFAULT now(),
  updated_at timestamp without time zone DEFAULT now(),
  CONSTRAINT sla_metrics_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id),
  CONSTRAINT sla_metrics_broker_id_fkey FOREIGN KEY (broker_id) REFERENCES public.brokers(id)
);
CREATE TABLE public.stages_list (
  id bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
  stage_id bigint,
  stage_name character varying,
  pipeline_id bigint,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp without time zone DEFAULT now(),
  company_id uuid,
  pipeline_name character varying,
  position bigint,
  CONSTRAINT stages_list_pkey PRIMARY KEY (id),
  CONSTRAINT stages_list_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id)
);
CREATE TABLE public.sync_control (
  company_id uuid NOT NULL,
  last_sync timestamp with time zone,
  next_sync timestamp with time zone,
  status text,
  error text,
  CONSTRAINT sync_control_pkey PRIMARY KEY (company_id)
);
CREATE TABLE public.sync_logs (
  id integer NOT NULL,
  company_id uuid,
  timestamp timestamp without time zone DEFAULT now(),
  type text NOT NULL,
  message text NOT NULL,
  CONSTRAINT sync_logs_pkey PRIMARY KEY (id),
  CONSTRAINT sync_logs_company_id_companies_id_fk FOREIGN KEY (company_id) REFERENCES public.companies(id)
);
CREATE TABLE public.weekly_logs (
  id uuid NOT NULL,
  week_start timestamp without time zone,
  week_end timestamp without time zone,
  company_id uuid,
  total_leads integer,
  total_points integer,
  created_at timestamp without time zone,
  CONSTRAINT weekly_logs_pkey PRIMARY KEY (id)
);
