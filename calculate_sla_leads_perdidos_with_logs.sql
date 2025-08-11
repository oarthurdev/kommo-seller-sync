
-- Função RPC para calcular leads perdidos por inatividade baseado na atribuição de responsável
CREATE OR REPLACE FUNCTION calculate_sla_leads_perdidos(
    p_company_id TEXT,
    p_broker_id BIGINT
) RETURNS INTEGER AS $$
DECLARE
    v_execution_id UUID := gen_random_uuid();
    v_broker_name TEXT;
    v_leads_perdidos INTEGER := 0;
    v_total_assignments INTEGER := 0;
    v_start_time TIMESTAMP := clock_timestamp();
    v_current_lead_id BIGINT;
    v_activity_record RECORD;
    v_assignment_time TIMESTAMP;
    v_next_assignment_time TIMESTAMP;
    v_first_broker_message TIMESTAMP;
    v_time_diff_minutes NUMERIC;
    v_execution_time INTEGER;
BEGIN
    -- Log inicial
    INSERT INTO sla_calculation_logs (
        company_id, broker_id, execution_id, log_level, step_name, message,
        additional_data
    ) VALUES (
        p_company_id, p_broker_id, v_execution_id, 'INFO', 'INIT',
        'Iniciando cálculo SLA - leads perdidos por inatividade após atribuição',
        jsonb_build_object('company_id', p_company_id, 'broker_id', p_broker_id)
    );

    -- Buscar nome do broker
    BEGIN
        SELECT nome INTO v_broker_name 
        FROM brokers 
        WHERE id = p_broker_id AND company_id = p_company_id::uuid;
        
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

    -- Buscar atividades de atribuição de responsável para o broker nos últimos 30 dias
    INSERT INTO sla_calculation_logs (
        company_id, broker_id, broker_name, execution_id, log_level, step_name, message
    ) VALUES (
        p_company_id, p_broker_id, v_broker_name, v_execution_id, 'INFO', 'FETCH_ASSIGNMENTS',
        'Buscando atribuições de responsável para o broker nos últimos 30 dias'
    );

    SELECT COUNT(*) INTO v_total_assignments
    FROM activities 
    WHERE company_id = p_company_id::uuid
      AND tipo = 'mudança_responsavel'
      AND responsavel_novo = p_broker_id
      AND criado_em >= (NOW() - INTERVAL '30 days');

    INSERT INTO sla_calculation_logs (
        company_id, broker_id, broker_name, execution_id, log_level, step_name, message,
        leads_processed, additional_data
    ) VALUES (
        p_company_id, p_broker_id, v_broker_name, v_execution_id, 'INFO', 'ASSIGNMENTS_COUNT',
        'Total de atribuições encontradas: ' || v_total_assignments,
        v_total_assignments,
        jsonb_build_object('total_assignments', v_total_assignments, 'period_days', 30)
    );

    -- Se não há atribuições, retornar zero
    IF v_total_assignments = 0 THEN
        INSERT INTO sla_calculation_logs (
            company_id, broker_id, broker_name, execution_id, log_level, step_name, message
        ) VALUES (
            p_company_id, p_broker_id, v_broker_name, v_execution_id, 'INFO', 'NO_ASSIGNMENTS',
            'Nenhuma atribuição encontrada - retornando 0'
        );
        
        RETURN 0;
    END IF;

    -- Loop através das atribuições para verificar SLA
    FOR v_activity_record IN 
        SELECT lead_id, criado_em, responsavel_anterior
        FROM activities 
        WHERE company_id = p_company_id::uuid
          AND tipo = 'mudança_responsavel'
          AND responsavel_novo = p_broker_id
          AND criado_em >= (NOW() - INTERVAL '30 days')
        ORDER BY criado_em DESC
        LIMIT 200 -- Limitar para performance
    LOOP
        v_current_lead_id := v_activity_record.lead_id;
        v_assignment_time := v_activity_record.criado_em;
        
        INSERT INTO sla_calculation_logs (
            company_id, broker_id, broker_name, execution_id, log_level, step_name, message,
            current_lead_id, additional_data
        ) VALUES (
            p_company_id, p_broker_id, v_broker_name, v_execution_id, 'DEBUG', 'PROCESS_ASSIGNMENT',
            'Processando atribuição do lead ' || v_current_lead_id || ' em ' || v_assignment_time,
            v_current_lead_id::text,
            jsonb_build_object(
                'lead_id', v_current_lead_id, 
                'assignment_time', v_assignment_time,
                'responsavel_anterior', v_activity_record.responsavel_anterior
            )
        );

        -- Buscar próxima mudança de responsável deste lead (indicando que o broker perdeu o lead)
        SELECT MIN(criado_em) INTO v_next_assignment_time
        FROM activities
        WHERE company_id = p_company_id::uuid
          AND lead_id = v_current_lead_id
          AND tipo = 'mudança_responsavel'
          AND responsavel_anterior = p_broker_id
          AND criado_em > v_assignment_time;

        -- Se houve mudança de responsável, verificar se foi por inatividade
        IF v_next_assignment_time IS NOT NULL THEN
            -- Buscar primeira mensagem do broker após ser atribuído (webhook outgoing)
            SELECT MIN(inserted_at) INTO v_first_broker_message
            FROM from_webhook
            WHERE lead_id = v_current_lead_id
              AND broker_id = p_broker_id::text
              AND message_type = 'outgoing'
              AND inserted_at >= v_assignment_time
              AND inserted_at < v_next_assignment_time;

            INSERT INTO sla_calculation_logs (
                company_id, broker_id, broker_name, execution_id, log_level, step_name, message,
                current_lead_id, additional_data
            ) VALUES (
                p_company_id, p_broker_id, v_broker_name, v_execution_id, 'DEBUG', 'MESSAGE_CHECK',
                'Verificando mensagens entre ' || v_assignment_time || ' e ' || v_next_assignment_time,
                v_current_lead_id::text,
                jsonb_build_object(
                    'assignment_time', v_assignment_time,
                    'next_assignment_time', v_next_assignment_time,
                    'first_broker_message', v_first_broker_message
                )
            );

            -- Se não enviou mensagem, calcular tempo até a troca de responsável
            IF v_first_broker_message IS NULL THEN
                v_time_diff_minutes := EXTRACT(EPOCH FROM (v_next_assignment_time - v_assignment_time)) / 60;
                
                INSERT INTO sla_calculation_logs (
                    company_id, broker_id, broker_name, execution_id, log_level, step_name, message,
                    current_lead_id, time_threshold_minutes, additional_data
                ) VALUES (
                    p_company_id, p_broker_id, v_broker_name, v_execution_id, 'DEBUG', 'INACTIVITY_CHECK',
                    'Sem mensagem enviada - Tempo até troca: ' || ROUND(v_time_diff_minutes, 2) || ' minutos',
                    v_current_lead_id::text, 27,
                    jsonb_build_object(
                        'time_diff_minutes', v_time_diff_minutes,
                        'threshold', 27,
                        'sla_violated', v_time_diff_minutes >= 27
                    )
                );

                -- Se passou de 27 minutos sem enviar mensagem, contabilizar como lead perdido
                IF v_time_diff_minutes >= 27 THEN
                    v_leads_perdidos := v_leads_perdidos + 1;
                    
                    INSERT INTO sla_calculation_logs (
                        company_id, broker_id, broker_name, execution_id, log_level, step_name, message,
                        current_lead_id, time_threshold_minutes, additional_data
                    ) VALUES (
                        p_company_id, p_broker_id, v_broker_name, v_execution_id, 'WARNING', 'SLA_VIOLATION',
                        'LEAD PERDIDO! ' || ROUND(v_time_diff_minutes, 2) || ' min sem resposta após atribuição',
                        v_current_lead_id::text, 27,
                        jsonb_build_object(
                            'time_diff_minutes', v_time_diff_minutes,
                            'leads_perdidos_count', v_leads_perdidos
                        )
                    );
                END IF;
            ELSE
                -- Verificar se a primeira mensagem foi enviada dentro de 27 minutos
                v_time_diff_minutes := EXTRACT(EPOCH FROM (v_first_broker_message - v_assignment_time)) / 60;
                
                INSERT INTO sla_calculation_logs (
                    company_id, broker_id, broker_name, execution_id, log_level, step_name, message,
                    current_lead_id, additional_data
                ) VALUES (
                    p_company_id, p_broker_id, v_broker_name, v_execution_id, 'DEBUG', 'MESSAGE_TIMING',
                    'Primeira mensagem enviada após ' || ROUND(v_time_diff_minutes, 2) || ' minutos da atribuição',
                    v_current_lead_id::text,
                    jsonb_build_object(
                        'response_time_minutes', v_time_diff_minutes,
                        'within_sla', v_time_diff_minutes < 27
                    )
                );

                -- Se a primeira mensagem foi após 27 minutos, ainda assim perdeu o lead
                IF v_time_diff_minutes >= 27 THEN
                    v_leads_perdidos := v_leads_perdidos + 1;
                    
                    INSERT INTO sla_calculation_logs (
                        company_id, broker_id, broker_name, execution_id, log_level, step_name, message,
                        current_lead_id, time_threshold_minutes, additional_data
                    ) VALUES (
                        p_company_id, p_broker_id, v_broker_name, v_execution_id, 'WARNING', 'SLA_VIOLATION',
                        'LEAD PERDIDO! Primeira mensagem após ' || ROUND(v_time_diff_minutes, 2) || ' min (SLA = 27min)',
                        v_current_lead_id::text, 27,
                        jsonb_build_object(
                            'response_time_minutes', v_time_diff_minutes,
                            'leads_perdidos_count', v_leads_perdidos
                        )
                    );
                ELSE
                    INSERT INTO sla_calculation_logs (
                        company_id, broker_id, broker_name, execution_id, log_level, step_name, message,
                        current_lead_id
                    ) VALUES (
                        p_company_id, p_broker_id, v_broker_name, v_execution_id, 'INFO', 'SLA_OK_BUT_LOST',
                        'Respondeu dentro do prazo mas ainda assim perdeu o lead (outros motivos)',
                        v_current_lead_id::text
                    );
                END IF;
            END IF;
        ELSE
            INSERT INTO sla_calculation_logs (
                company_id, broker_id, broker_name, execution_id, log_level, step_name, message,
                current_lead_id
            ) VALUES (
                p_company_id, p_broker_id, v_broker_name, v_execution_id, 'DEBUG', 'STILL_RESPONSIBLE',
                'Broker ainda é responsável por este lead',
                v_current_lead_id::text
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
        'Cálculo SLA finalizado - ' || v_leads_perdidos || ' leads perdidos de ' || v_total_assignments || ' atribuições',
        v_total_assignments, v_execution_time,
        jsonb_build_object(
            'leads_perdidos', v_leads_perdidos,
            'total_assignments', v_total_assignments,
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
COMMENT ON FUNCTION calculate_sla_leads_perdidos IS 'Calcula leads perdidos por inatividade baseado no fluxo: atribuição → 27min sem resposta → troca de responsável';
