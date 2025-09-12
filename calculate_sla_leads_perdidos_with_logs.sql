-- Função RPC para calcular leads perdidos por inatividade baseado na atribuição de responsável
CREATE OR REPLACE FUNCTION calculate_sla_leads_perdidos(
    p_company_id TEXT,
    p_broker_id INT8
) RETURNS INTEGER AS $$
DECLARE
    v_execution_id UUID := gen_random_uuid();
    v_broker_name TEXT;
    v_leads_perdidos INTEGER := 0;
    v_total_assignments INTEGER := 0;
    v_start_time TIMESTAMP := clock_timestamp();
    v_current_lead_id INT8;
    v_activity_record RECORD;
    v_assignment_time TIMESTAMP;
    v_next_assignment_time TIMESTAMP;
    v_first_broker_message TIMESTAMP;
    v_time_diff_minutes NUMERIC;
    v_execution_time INTEGER;
    v_company_uuid UUID;
    v_broker_int INT8 := p_broker_id; -- Manter o broker_id como INT8
BEGIN
    -- Converter company_id para UUID
    BEGIN
        v_company_uuid := p_company_id::UUID; -- A conversão para UUID está correta
    EXCEPTION WHEN OTHERS THEN
        RETURN 0;
    END;

    -- Buscar nome do broker
    BEGIN
        SELECT nome INTO v_broker_name 
        FROM brokers 
        WHERE id = v_broker_int
          AND company_id = v_company_uuid; -- Comparação correta de UUID com UUID
    END;

    SELECT COUNT(*) INTO v_total_assignments
    FROM activities 
    WHERE company_id = v_company_uuid
      AND tipo = 'mudança_responsável'
      AND responsavel_novo = v_broker_int
      AND criado_em >= (NOW() - INTERVAL '30 days');

    -- Se não há atribuições, retornar zero
    IF v_total_assignments = 0 THEN
        RETURN 0;
    END IF;

    -- Loop através das atribuições para verificar SLA
    FOR v_activity_record IN 
        SELECT lead_id, criado_em, responsavel_anterior
        FROM activities 
        WHERE company_id = v_company_uuid
          AND tipo = 'mudança_responsável'
          AND responsavel_novo = v_broker_int
          AND criado_em >= (NOW() - INTERVAL '30 days')
        ORDER BY criado_em DESC
        LIMIT 200 -- Limitar para performance
    LOOP
        v_current_lead_id := v_activity_record.lead_id;
        v_assignment_time := v_activity_record.criado_em;

        -- Buscar próxima mudança de responsável deste lead (indicando que o broker perdeu o lead)
        SELECT MIN(criado_em) INTO v_next_assignment_time
        FROM activities
        WHERE company_id = v_company_uuid
          AND lead_id = v_current_lead_id
          AND tipo = 'mudança_responsável'
          AND responsavel_anterior = v_broker_int
          AND criado_em > v_assignment_time;

        -- Se houve mudança de responsável, verificar se foi por inatividade
        IF v_next_assignment_time IS NOT NULL THEN
            -- Buscar primeira mensagem do broker após ser atribuído
            SELECT MIN(criado_em) INTO v_first_broker_message
            FROM activities
            WHERE company_id = v_company_uuid
              AND lead_id = v_current_lead_id
              AND user_id = v_broker_int
              AND tipo = 'mensagem_enviada'
              AND criado_em >= v_assignment_time
              AND criado_em < v_next_assignment_time;

            -- Se não enviou mensagem, calcular tempo até a troca de responsável
            IF v_first_broker_message IS NULL THEN
                v_time_diff_minutes := EXTRACT(EPOCH FROM (v_next_assignment_time - v_assignment_time)) / 60;
                
                -- Se passou de 27 minutos sem enviar mensagem, contabilizar como lead perdido
                IF v_time_diff_minutes >= 27 THEN
                    v_leads_perdidos := v_leads_perdidos + 1;
                END IF;
            ELSE
                -- Verificar se a primeira mensagem foi enviada dentro de 27 minutos
                v_time_diff_minutes := EXTRACT(EPOCH FROM (v_first_broker_message - v_assignment_time)) / 60;
                
                -- Se a primeira mensagem foi após 27 minutos, ainda assim perdeu o lead
                IF v_time_diff_minutes >= 27 THEN
                    v_leads_perdidos := v_leads_perdidos + 1;
                END IF;
            END IF;
        END IF;
    END LOOP;

    -- Calcular tempo de execução
    v_execution_time := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time)) * 1000;

    RETURN v_leads_perdidos;
END
$$ LANGUAGE plpgsql;

-- Comentário da função
COMMENT ON FUNCTION calculate_sla_leads_perdidos IS 'Calcula leads perdidos por inatividade baseado no fluxo: atribuição → 27min sem resposta → troca de responsável';
