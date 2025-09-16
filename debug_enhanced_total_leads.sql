-- Script SQL com debug detalhado para contagem de total_leads
-- Execute este script para ter debugging completo do processo

-- Função principal com debug extensivo
CREATE OR REPLACE FUNCTION get_corretor_responsavel_from_custom_fields(custom_fields_input jsonb)
RETURNS text AS $$
DECLARE
    field jsonb;
    corretor_name text;
    custom_fields_json jsonb;
    field_count integer;
    debug_info text;
BEGIN
    -- Debug: Input inicial
    debug_info := format('INPUT: %s, TIPO: %s', 
                        COALESCE(custom_fields_input::text, 'NULL'), 
                        COALESCE(jsonb_typeof(custom_fields_input), 'NULL'));
    
    -- Se input é nulo, retornar null
    IF custom_fields_input IS NULL THEN
        RAISE DEBUG 'DEBUG get_corretor: Input é NULL';
        RETURN NULL;
    END IF;
    
    -- Verificar se o input é uma string que precisa ser parseada
    IF jsonb_typeof(custom_fields_input) = 'string' THEN
        BEGIN
            RAISE DEBUG 'DEBUG get_corretor: Input é string, tentando parsear: %', custom_fields_input::text;
            -- Tentar parsear a string como JSON
            custom_fields_json := (custom_fields_input #>> '{}')::jsonb;
            RAISE DEBUG 'DEBUG get_corretor: String parseada com sucesso, novo tipo: %', jsonb_typeof(custom_fields_json);
        EXCEPTION WHEN OTHERS THEN
            RAISE DEBUG 'DEBUG get_corretor: ERRO ao parsear string como JSON: %', SQLERRM;
            RETURN NULL;
        END;
    ELSE
        custom_fields_json := custom_fields_input;
        RAISE DEBUG 'DEBUG get_corretor: Input já é JSON válido, tipo: %', jsonb_typeof(custom_fields_json);
    END IF;
    
    -- Verificar se é um array
    IF jsonb_typeof(custom_fields_json) != 'array' THEN
        RAISE DEBUG 'DEBUG get_corretor: Não é um array, tipo encontrado: %', jsonb_typeof(custom_fields_json);
        RETURN NULL;
    END IF;
    
    -- Contar elementos do array
    field_count := jsonb_array_length(custom_fields_json);
    RAISE DEBUG 'DEBUG get_corretor: Array com % elementos', field_count;
    
    -- Percorrer o array JSON
    FOR field IN SELECT jsonb_array_elements(custom_fields_json)
    LOOP
        RAISE DEBUG 'DEBUG get_corretor: Processando field: %', field::text;
        RAISE DEBUG 'DEBUG get_corretor: field_name encontrado: %', COALESCE(field->>'field_name', 'NULL');
        
        -- Verificar se é o campo "Corretor responsável"
        IF field->>'field_name' = 'Corretor responsável' THEN
            RAISE DEBUG 'DEBUG get_corretor: Campo "Corretor responsável" encontrado!';
            
            -- Verificar se existe o array values e se tem elementos
            IF field ? 'values' AND jsonb_typeof(field->'values') = 'array' AND jsonb_array_length(field->'values') > 0 THEN
                corretor_name := field->'values'->0->>'value';
                RAISE DEBUG 'DEBUG get_corretor: Corretor extraído: %', COALESCE(corretor_name, 'NULL');
                RETURN corretor_name;
            ELSE
                RAISE DEBUG 'DEBUG get_corretor: Campo encontrado mas sem values válidos. Values: %', COALESCE(field->'values'::text, 'NULL');
            END IF;
        END IF;
    END LOOP;
    
    RAISE DEBUG 'DEBUG get_corretor: Nenhum campo "Corretor responsável" encontrado após percorrer todos os % elementos', field_count;
    RETURN NULL;
    
EXCEPTION WHEN OTHERS THEN
    RAISE DEBUG 'DEBUG get_corretor: EXCEÇÃO: %', SQLERRM;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Função de debug detalhado para custom_fields_values
CREATE OR REPLACE FUNCTION debug_custom_fields_detailed(p_company_id uuid, p_limit integer DEFAULT 5)
RETURNS TABLE(
    lead_id bigint,
    lead_nome text,
    criado_em timestamp,
    custom_fields_type text,
    custom_fields_length integer,
    raw_sample text,
    parsed_corretor text,
    field_names text[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        l.id::bigint as lead_id,
        l.nome as lead_nome,
        l.criado_em,
        jsonb_typeof(l.custom_fields_values) as custom_fields_type,
        CASE 
            WHEN jsonb_typeof(l.custom_fields_values) = 'array' THEN jsonb_array_length(l.custom_fields_values)
            WHEN jsonb_typeof(l.custom_fields_values) = 'string' THEN 
                CASE 
                    WHEN jsonb_typeof((l.custom_fields_values #>> '{}')::jsonb) = 'array' 
                    THEN jsonb_array_length((l.custom_fields_values #>> '{}')::jsonb)
                    ELSE NULL
                END
            ELSE NULL
        END as custom_fields_length,
        LEFT(l.custom_fields_values::text, 200) as raw_sample,
        get_corretor_responsavel_from_custom_fields(l.custom_fields_values) as parsed_corretor,
        CASE 
            WHEN jsonb_typeof(l.custom_fields_values) = 'array' THEN
                ARRAY(SELECT jsonb_array_elements_text(
                    jsonb_path_query_array(l.custom_fields_values, '$[*].field_name')
                ))
            WHEN jsonb_typeof(l.custom_fields_values) = 'string' THEN
                ARRAY(SELECT jsonb_array_elements_text(
                    jsonb_path_query_array((l.custom_fields_values #>> '{}')::jsonb, '$[*].field_name')
                ))
            ELSE ARRAY[]::text[]
        END as field_names
    FROM public.leads l 
    WHERE l.custom_fields_values IS NOT NULL
    AND l.company_id = p_company_id
    ORDER BY l.criado_em DESC
    LIMIT p_limit;
    
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'ERRO na função debug_custom_fields_detailed: %', SQLERRM;
    RETURN;
END;
$$ LANGUAGE plpgsql;

-- Função principal com debug completo
CREATE OR REPLACE FUNCTION update_total_leads_count_debug(p_company_id uuid)
RETURNS void AS $$
DECLARE
    filter_record record;
    broker_record record;
    leads_count integer;
    start_date_filter date;
    end_date_filter date;
    total_leads_with_custom_fields integer;
    valid_custom_fields_count integer;
    total_brokers integer;
    total_filters integer;
    debug_sample record;
BEGIN
    RAISE NOTICE '=== INICIANDO DEBUG PARA COMPANY_ID: % ===', p_company_id;
    
    -- Debug: Verificar se a empresa existe
    SELECT COUNT(*) INTO total_filters
    FROM public.companies c
    WHERE c.id = p_company_id;
    
    IF total_filters = 0 THEN
        RAISE NOTICE 'ERRO: Company_id % não encontrado na tabela companies', p_company_id;
        RETURN;
    END IF;
    
    RAISE NOTICE 'DEBUG: Company encontrada';
    
    -- Debug: Verificar filtros
    SELECT COUNT(*) INTO total_filters
    FROM public.component_filters cf
    WHERE cf.company_id = p_company_id 
    AND cf.component_name = 'ranking_metrics' 
    AND cf.filter_type = 'month';
    
    RAISE NOTICE 'DEBUG: Filtros encontrados: %', total_filters;
    
    IF total_filters = 0 THEN
        RAISE NOTICE 'AVISO: Nenhum filtro ranking_metrics/month encontrado para company_id %', p_company_id;
        RETURN;
    END IF;
    
    -- Debug: Verificar brokers
    SELECT COUNT(*) INTO total_brokers
    FROM public.brokers b
    WHERE b.company_id = p_company_id AND b.active = true;
    
    RAISE NOTICE 'DEBUG: Brokers ativos encontrados: %', total_brokers;
    
    -- Debug: Verificar leads com custom_fields_values
    SELECT COUNT(*) INTO total_leads_with_custom_fields
    FROM public.leads l
    WHERE l.company_id = p_company_id
    AND l.custom_fields_values IS NOT NULL;
    
    RAISE NOTICE 'DEBUG: Leads com custom_fields_values: %', total_leads_with_custom_fields;
    
    -- Debug: Mostrar amostra dos custom_fields
    IF total_leads_with_custom_fields > 0 THEN
        FOR debug_sample IN 
            SELECT * FROM debug_custom_fields_detailed(p_company_id, 3)
        LOOP
            RAISE NOTICE 'DEBUG SAMPLE: Lead_ID=%, Nome=%, Tipo=%, Length=%, Corretor=%, Fields=%', 
                debug_sample.lead_id, debug_sample.lead_nome, debug_sample.custom_fields_type, 
                debug_sample.custom_fields_length, debug_sample.parsed_corretor, debug_sample.field_names;
        END LOOP;
    END IF;
    
    -- Verificar quantos têm o campo "Corretor responsável" válido
    SELECT COUNT(*) INTO valid_custom_fields_count
    FROM public.leads l
    WHERE l.company_id = p_company_id
    AND l.custom_fields_values IS NOT NULL
    AND get_corretor_responsavel_from_custom_fields(l.custom_fields_values) IS NOT NULL;
    
    RAISE NOTICE 'DEBUG: Leads com "Corretor responsável" válido: %', valid_custom_fields_count;
    
    -- Processar filtros
    FOR filter_record IN 
        SELECT cf.month, cf.year, cf.company_id, cf.id as filter_id
        FROM public.component_filters cf
        WHERE cf.company_id = p_company_id 
        AND cf.component_name = 'ranking_metrics' 
        AND cf.filter_type = 'month'
        AND cf.month IS NOT NULL 
        AND cf.year IS NOT NULL
        ORDER BY cf.year DESC, cf.month DESC
    LOOP
        -- Calcular data inicial e final do filtro
        start_date_filter := make_date(filter_record.year, filter_record.month, 1);
        end_date_filter := (start_date_filter + interval '1 month' - interval '1 day')::date;
        
        RAISE NOTICE '=== PROCESSANDO FILTRO ID=% ===', filter_record.filter_id;
        RAISE NOTICE 'DEBUG: Período: mês=% ano=% (% a %)', 
            filter_record.month, filter_record.year, start_date_filter, end_date_filter;
        
        -- Verificar quantos leads existem no período
        SELECT COUNT(*) INTO leads_count
        FROM public.leads l
        WHERE l.company_id = p_company_id
        AND l.criado_em >= start_date_filter 
        AND l.criado_em <= end_date_filter + interval '23:59:59';
        
        RAISE NOTICE 'DEBUG: Total leads no período: %', leads_count;
        
        -- Para cada corretor da empresa
        FOR broker_record IN 
            SELECT b.id, b.nome, b.company_id
            FROM public.brokers b
            WHERE b.company_id = p_company_id
            AND b.active = true
            ORDER BY b.nome
        LOOP
            RAISE NOTICE '--- Processando corretor: ID=% Nome=% ---', broker_record.id, broker_record.nome;
            
            -- Contar leads do corretor no período baseado nos custom_fields_values
            BEGIN
                SELECT COUNT(*) INTO leads_count
                FROM public.leads l
                WHERE l.company_id = p_company_id
                AND l.criado_em >= start_date_filter 
                AND l.criado_em <= end_date_filter + interval '23:59:59'
                AND l.custom_fields_values IS NOT NULL
                AND get_corretor_responsavel_from_custom_fields(l.custom_fields_values) = broker_record.nome;
                
                RAISE NOTICE 'DEBUG: Corretor % - Leads encontrados: %', broker_record.nome, leads_count;
                
                -- Atualizar ou inserir na tabela broker_points
                INSERT INTO public.broker_points (id, nome, total_leads, company_id, updated_at)
                VALUES (broker_record.id, broker_record.nome, leads_count, p_company_id, now())
                ON CONFLICT (id) 
                DO UPDATE SET 
                    total_leads = EXCLUDED.total_leads,
                    updated_at = EXCLUDED.updated_at;
                
                RAISE NOTICE 'DEBUG: broker_points atualizado para corretor %', broker_record.nome;
                    
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'ERRO ao processar corretor %: %', broker_record.nome, SQLERRM;
            END;
        END LOOP;
    END LOOP;
    
    RAISE NOTICE '=== CONCLUÍDO PARA COMPANY_ID: % ===', p_company_id;
END;
$$ LANGUAGE plpgsql;

-- Função para mostrar resultados de forma organizada
CREATE OR REPLACE FUNCTION show_total_leads_results(p_company_id uuid)
RETURNS TABLE(
    broker_id bigint,
    broker_nome text,
    total_leads integer,
    pontos integer,
    last_updated timestamp
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        bp.id,
        bp.nome,
        bp.total_leads,
        bp.pontos,
        bp.updated_at
    FROM public.broker_points bp
    WHERE bp.company_id = p_company_id
    ORDER BY bp.total_leads DESC, bp.nome;
END;
$$ LANGUAGE plpgsql;

-- Exemplos de uso com debug:

-- 1. Debug detalhado dos custom_fields:
-- SELECT * FROM debug_custom_fields_detailed('uuid-da-empresa', 10);

-- 2. Executar com debug completo:
-- SELECT update_total_leads_count_debug('uuid-da-empresa');

-- 3. Ver resultados:
-- SELECT * FROM show_total_leads_results('uuid-da-empresa');