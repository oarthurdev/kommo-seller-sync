
-- Tabela para logs detalhados da função RPC calculate_sla_leads_perdidos
CREATE TABLE sla_calculation_logs (
    id SERIAL PRIMARY KEY,
    company_id TEXT NOT NULL,
    broker_id BIGINT NOT NULL,
    broker_name TEXT,
    execution_id UUID DEFAULT gen_random_uuid(),
    log_level TEXT NOT NULL CHECK (log_level IN ('INFO', 'WARNING', 'ERROR', 'DEBUG')),
    step_name TEXT NOT NULL,
    message TEXT NOT NULL,
    leads_processed INTEGER DEFAULT 0,
    activities_processed INTEGER DEFAULT 0,
    current_lead_id TEXT,
    time_threshold_minutes INTEGER DEFAULT 27,
    execution_time_ms INTEGER,
    additional_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Índices para performance
CREATE INDEX idx_sla_logs_company_broker ON sla_calculation_logs(company_id, broker_id);
CREATE INDEX idx_sla_logs_execution_id ON sla_calculation_logs(execution_id);
CREATE INDEX idx_sla_logs_created_at ON sla_calculation_logs(created_at);
CREATE INDEX idx_sla_logs_step_name ON sla_calculation_logs(step_name);

-- Comentários para documentação
COMMENT ON TABLE sla_calculation_logs IS 'Logs detalhados da função RPC calculate_sla_leads_perdidos para debugging e monitoramento';
COMMENT ON COLUMN sla_calculation_logs.execution_id IS 'UUID único para agrupar logs de uma mesma execução da função';
COMMENT ON COLUMN sla_calculation_logs.step_name IS 'Nome da etapa sendo executada (ex: INIT, FETCH_LEADS, PROCESS_LEAD, CALCULATE_SLA)';
COMMENT ON COLUMN sla_calculation_logs.additional_data IS 'Dados adicionais em formato JSON para debug específico de cada step';
