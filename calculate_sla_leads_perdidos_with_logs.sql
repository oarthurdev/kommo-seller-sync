
-- Função RPC otimizada para calcular leads perdidos por inatividade COM LOGS DETALHADOS
CREATE OR REPLACE FUNCTION calculate_sla_leads_perdidos(
    p_company_id TEXT,
    p_broker_id BIGINT
) RETURNS INTEGER AS $$
DECLARE
    v_execution_id UUID := gen_random_uuid();
    v_broker_name TEXT;
    v_leads_perdidos INTEGER := 0;
    v_total_leads INTEGER := 0;
    v_total_activities INTEGER := 0;
    v_start_time TIMESTAMP := clock_timestamp();
    v_current_lead_id TEXT;
    v_lead_record RECORD;
    v_activity_record RECORD;
    v_last_client_message TIMESTAMP;
    v_last_broker_response TIMESTAMP;
    v_time_diff_minutes NUMERIC;
    v_execution_time INTEGER;
BEGIN
    -- Log inicial
    INSERT INTO sla_calculation_logs (
        company_id, broker_id, execution_id, log_level, step_name, message,
        additional_data
    ) VALUES (
        p_company_id, p_broker_id, v_execution_id, 'INFO', 'INIT',
        'Iniciando cálculo SLA para broker ' || p_broker_id,
        jsonb_build_object('company_id', p_company_id, 'broker_id', p_broker_id)
    );

    -- Buscar nome do broker
    BEGIN
        SELECT nome INTO v_broker_name 
        FROM brokers 
        WHERE id = p_broker_id AND company_id = p_company_id;
        
        INSERT INTO sla_calculation_logs (
            company_id, broker_id, broker_name, execution_id, log_level, step_name, message
        ) VALUES (
            p_company_id, p_broker_id, v_broker_name, v_execution_id, 'INFO', 'BROKER_FOUND',
            'Broker encontrado: ' || COALESCE(v_broker_name, 'Nome não encontrado')
        );
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO sla_calculation_logs (
            company_id, broker_id, execution_id, log_level, step_name, message,
            additional_data
        ) VALUES (
            p_company_id, p_broker_id, v_execution_id, 'WARNING', 'BROKER_NOT_FOUND',
            'Broker não encontrado na tabela brokers',
            jsonb_build_object('error', SQLERRM)
        );
    END;

    -- Buscar leads do broker com status "Sem Contato" (stage_id = 70766295)
    INSERT INTO sla_calculation_logs (
        company_id, broker_id, broker_name, execution_id, log_level, step_name, message
    ) VALUES (
        p_company_id, p_broker_id, v_broker_name, v_execution_id, 'INFO', 'FETCH_LEADS',
        'Buscando leads com status "Sem Contato" para o broker'
    );

    SELECT COUNT(*) INTO v_total_leads
    FROM leads 
    WHERE responsavel_id = p_broker_id 
      AND company_id = p_company_id 
      AND status_id = 70766295;

    INSERT INTO sla_calculation_logs (
        company_id, broker_id, broker_name, execution_id, log_level, step_name, message,
        leads_processed, additional_data
    ) VALUES (
        p_company_id, p_broker_id, v_broker_name, v_execution_id, 'INFO', 'LEADS_COUNT',
        'Total de leads "Sem Contato" encontrados: ' || v_total_leads,
        v_total_leads,
        jsonb_build_object('status_id', 70766295, 'total_leads', v_total_leads)
    );

    -- Se não há leads, retornar zero
    IF v_total_leads = 0 THEN
        INSERT INTO sla_calculation_logs (
            company_id, broker_id, broker_name, execution_id, log_level, step_name, message
        ) VALUES (
            p_company_id, p_broker_id, v_broker_name, v_execution_id, 'INFO', 'NO_LEADS',
            'Nenhum lead "Sem Contato" encontrado - retornando 0'
        );
        
        RETURN 0;
    END IF;

    -- Loop através dos leads para verificar SLA
    FOR v_lead_record IN 
        SELECT id, nome, criado_em
        FROM leads 
        WHERE responsavel_id = p_broker_id 
          AND company_id = p_company_id 
          AND status_id = 70766295
        ORDER BY criado_em DESC
        LIMIT 100 -- Limitar para performance
    LOOP
        v_current_lead_id := v_lead_record.id;
        
        INSERT INTO sla_calculation_logs (
            company_id, broker_id, broker_name, execution_id, log_level, step_name, message,
            current_lead_id, additional_data
        ) VALUES (
            p_company_id, p_broker_id, v_broker_name, v_execution_id, 'DEBUG', 'PROCESS_LEAD',
            'Processando lead: ' || COALESCE(v_lead_record.nome, 'Nome não disponível'),
            v_current_lead_id,
            jsonb_build_object('lead_id', v_current_lead_id, 'lead_criado_em', v_lead_record.criado_em)
        );

        -- Buscar última mensagem do cliente (webhook incoming)
        SELECT MAX(inserted_at) INTO v_last_client_message
        FROM from_webhook
        WHERE lead_id = v_current_lead_id
          AND message_type = 'incoming'
          AND created_at >= (NOW() - INTERVAL '30 days');

        -- Buscar última resposta do broker (webhook outgoing)
        SELECT MAX(inserted_at) INTO v_last_broker_response
        FROM from_webhook
        WHERE lead_id = v_current_lead_id
          AND broker_id = p_broker_id::TEXT
          AND message_type = 'outgoing'
          AND created_at >= (NOW() - INTERVAL '30 days');

        INSERT INTO sla_calculation_logs (
            company_id, broker_id, broker_name, execution_id, log_level, step_name, message,
            current_lead_id, additional_data
        ) VALUES (
            p_company_id, p_broker_id, v_broker_name, v_execution_id, 'DEBUG', 'MESSAGE_TIMES',
            'Timestamps encontrados para lead ' || v_current_lead_id,
            v_current_lead_id,
            jsonb_build_object(
                'last_client_message', v_last_client_message,
                'last_broker_response', v_last_broker_response
            )
        );

        -- Verificar SLA: se há mensagem do cliente sem resposta há mais de 27 minutos
        IF v_last_client_message IS NOT NULL THEN
            IF v_last_broker_response IS NULL OR v_last_client_message > v_last_broker_response THEN
                v_time_diff_minutes := EXTRACT(EPOCH FROM (NOW() - v_last_client_message)) / 60;
                
                INSERT INTO sla_calculation_logs (
                    company_id, broker_id, broker_name, execution_id, log_level, step_name, message,
                    current_lead_id, time_threshold_minutes, additional_data
                ) VALUES (
                    p_company_id, p_broker_id, v_broker_name, v_execution_id, 'DEBUG', 'SLA_CHECK',
                    'Verificando SLA - Tempo sem resposta: ' || ROUND(v_time_diff_minutes, 2) || ' minutos',
                    v_current_lead_id, 27,
                    jsonb_build_object(
                        'time_diff_minutes', v_time_diff_minutes,
                        'threshold', 27,
                        'sla_violated', v_time_diff_minutes > 27
                    )
                );

                IF v_time_diff_minutes > 27 THEN
                    v_leads_perdidos := v_leads_perdidos + 1;
                    
                    INSERT INTO sla_calculation_logs (
                        company_id, broker_id, broker_name, execution_id, log_level, step_name, message,
                        current_lead_id, time_threshold_minutes, additional_data
                    ) VALUES (
                        p_company_id, p_broker_id, v_broker_name, v_execution_id, 'WARNING', 'SLA_VIOLATION',
                        'SLA VIOLADO! Lead perdido por inatividade - ' || ROUND(v_time_diff_minutes, 2) || ' min sem resposta',
                        v_current_lead_id, 27,
                        jsonb_build_object(
                            'time_diff_minutes', v_time_diff_minutes,
                            'leads_perdidos_count', v_leads_perdidos
                        )
                    );
                END IF;
            ELSE
                INSERT INTO sla_calculation_logs (
                    company_id, broker_id, broker_name, execution_id, log_level, step_name, message,
                    current_lead_id
                ) VALUES (
                    p_company_id, p_broker_id, v_broker_name, v_execution_id, 'DEBUG', 'SLA_OK',
                    'SLA OK - Broker respondeu após última mensagem do cliente',
                    v_current_lead_id
                );
            END IF;
        ELSE
            INSERT INTO sla_calculation_logs (
                company_id, broker_id, broker_name, execution_id, log_level, step_name, message,
                current_lead_id
            ) VALUES (
                p_company_id, p_broker_id, v_broker_name, v_execution_id, 'DEBUG', 'NO_CLIENT_MESSAGE',
                'Nenhuma mensagem do cliente encontrada nos últimos 30 dias',
                v_current_lead_id
            );
        END IF;

    END LOOP;

    -- Calcular tempo de execução
    v_execution_time := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time)) * 1000;

    -- Log final
    INSERT INTO sla_calculation_logs (
        company_id, broker_id, broker_name, execution_id, log_level, step_name, message,
        leads_processed, execution_time_ms, additional_data
    ) VALUES (
        p_company_id, p_broker_id, v_broker_name, v_execution_id, 'INFO', 'RESULT',
        'Cálculo SLA finalizado - ' || v_leads_perdidos || ' leads perdidos de ' || v_total_leads || ' analisados',
        v_total_leads, v_execution_time,
        jsonb_build_object(
            'leads_perdidos', v_leads_perdidos,
            'total_leads', v_total_leads,
            'execution_time_ms', v_execution_time,
            'success', true
        )
    );

    RETURN v_leads_perdidos;

EXCEPTION WHEN OTHERS THEN
    -- Log de erro
    INSERT INTO sla_calculation_logs (
        company_id, broker_id, broker_name, execution_id, log_level, step_name, message,
        additional_data
    ) VALUES (
        p_company_id, p_broker_id, v_broker_name, v_execution_id, 'ERROR', 'EXCEPTION',
        'Erro durante execução: ' || SQLERRM,
        jsonb_build_object(
            'error_code', SQLSTATE,
            'error_message', SQLERRM,
            'success', false
        )
    );
    
    -- Re-raise a exceção
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Comentário da função
COMMENT ON FUNCTION calculate_sla_leads_perdidos IS 'Calcula leads perdidos por inatividade (>27min sem resposta) com logging detalhado';
