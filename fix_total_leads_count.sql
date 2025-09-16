-- Correção da função para lidar com custom_fields_values como string JSON
-- Execute este script para corrigir o erro "cannot extract elements from a scalar"

-- Função corrigida para extrair nome do corretor dos custom_fields_values
CREATE OR REPLACE FUNCTION get_corretor_responsavel_from_custom_fields(custom_fields_input jsonb)
RETURNS text AS $$
DECLARE
    field jsonb;
    corretor_name text;
    custom_fields_json jsonb;
BEGIN
    -- Se input é nulo, retornar null
    IF custom_fields_input IS NULL THEN
        RETURN NULL;
    END IF;
    
    -- Verificar se o input é uma string que precisa ser parseada
    IF jsonb_typeof(custom_fields_input) = 'string' THEN
        BEGIN
            -- Tentar parsear a string como JSON
            custom_fields_json := (custom_fields_input #>> '{}')::jsonb;
        EXCEPTION WHEN OTHERS THEN
            -- Se falhar ao parsear, retornar null
            RETURN NULL;
        END;
    ELSE
        custom_fields_json := custom_fields_input;
    END IF;
    
    -- Verificar se é um array
    IF jsonb_typeof(custom_fields_json) != 'array' THEN
        RETURN NULL;
    END IF;
    
    -- Percorrer o array JSON
    FOR field IN SELECT jsonb_array_elements(custom_fields_json)
    LOOP
        -- Verificar se é o campo "Corretor responsável"
        IF field->>'field_name' = 'Corretor responsável' THEN
            -- Verificar se existe o array values e se tem elementos
            IF field ? 'values' AND jsonb_typeof(field->'values') = 'array' AND jsonb_array_length(field->'values') > 0 THEN
                corretor_name := field->'values'->0->>'value';
                RETURN corretor_name;
            END IF;
        END IF;
    END LOOP;
    
    RETURN NULL;
EXCEPTION WHEN OTHERS THEN
    -- Em caso de qualquer erro, retornar null
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Função auxiliar para debug - mostra o tipo e conteúdo dos custom_fields_values
CREATE OR REPLACE FUNCTION debug_custom_fields_values(p_company_id uuid, p_limit integer DEFAULT 5)
RETURNS TABLE(
    lead_id bigint,
    lead_nome text,
    custom_fields_type text,
    raw_custom_fields text,
    parsed_corretor text
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        l.id::bigint as lead_id,
        l.nome as lead_nome,
        jsonb_typeof(l.custom_fields_values) as custom_fields_type,
        l.custom_fields_values::text as raw_custom_fields,
        get_corretor_responsavel_from_custom_fields(l.custom_fields_values) as parsed_corretor
    FROM public.leads l 
    WHERE l.custom_fields_values IS NOT NULL
    AND l.company_id = p_company_id
    ORDER BY l.id DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- Função principal atualizada com melhor tratamento de erros
CREATE OR REPLACE FUNCTION update_total_leads_count(p_company_id uuid)
RETURNS void AS $$
DECLARE
    filter_record record;
    broker_record record;
    leads_count integer;
    start_date_filter date;
    end_date_filter date;
    total_leads_with_custom_fields integer;
    valid_custom_fields_count integer;
BEGIN
    -- Verificar quantos leads têm custom_fields_values
    SELECT COUNT(*) INTO total_leads_with_custom_fields
    FROM public.leads l
    WHERE l.company_id = p_company_id
    AND l.custom_fields_values IS NOT NULL;
    
    RAISE NOTICE 'Total de leads com custom_fields_values: %', total_leads_with_custom_fields;
    
    -- Verificar quantos têm o campo "Corretor responsável" válido
    SELECT COUNT(*) INTO valid_custom_fields_count
    FROM public.leads l
    WHERE l.company_id = p_company_id
    AND l.custom_fields_values IS NOT NULL
    AND get_corretor_responsavel_from_custom_fields(l.custom_fields_values) IS NOT NULL;
    
    RAISE NOTICE 'Leads com campo "Corretor responsável" válido: %', valid_custom_fields_count;
    
    -- Buscar filtros ativos para ranking_metrics do tipo month
    FOR filter_record IN 
        SELECT cf.month, cf.year, cf.company_id
        FROM public.component_filters cf
        WHERE cf.company_id = p_company_id 
        AND cf.component_name = 'ranking_metrics' 
        AND cf.filter_type = 'month'
        AND cf.month IS NOT NULL 
        AND cf.year IS NOT NULL
    LOOP
        -- Calcular data inicial e final do filtro
        start_date_filter := make_date(filter_record.year, filter_record.month, 1);
        end_date_filter := (start_date_filter + interval '1 month' - interval '1 day')::date;
        
        RAISE NOTICE 'Processando filtro: mês % ano % (% a %)', 
            filter_record.month, filter_record.year, start_date_filter, end_date_filter;
        
        -- Para cada corretor da empresa
        FOR broker_record IN 
            SELECT b.id, b.nome, b.company_id
            FROM public.brokers b
            WHERE b.company_id = p_company_id
            AND b.active = true
        LOOP
            -- Contar leads do corretor no período baseado nos custom_fields_values
            BEGIN
                SELECT COUNT(*) INTO leads_count
                FROM public.leads l
                WHERE l.company_id = p_company_id
                AND l.criado_em >= start_date_filter 
                AND l.criado_em <= end_date_filter + interval '23:59:59'
                AND l.custom_fields_values IS NOT NULL
                AND get_corretor_responsavel_from_custom_fields(l.custom_fields_values) = broker_record.nome;
                
                RAISE NOTICE 'Corretor: % - Total leads: %', broker_record.nome, leads_count;
                
                -- Atualizar ou inserir na tabela broker_points
                INSERT INTO public.broker_points (id, nome, total_leads, company_id, updated_at)
                VALUES (broker_record.id, broker_record.nome, leads_count, p_company_id, now())
                ON CONFLICT (id) 
                DO UPDATE SET 
                    total_leads = EXCLUDED.total_leads,
                    updated_at = EXCLUDED.updated_at;
                    
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Erro ao processar corretor %: %', broker_record.nome, SQLERRM;
            END;
        END LOOP;
    END LOOP;
    
    RAISE NOTICE 'Atualização de total_leads concluída para company_id: %', p_company_id;
END;
$$ LANGUAGE plpgsql;

-- Exemplo de uso para debug:
-- SELECT * FROM debug_custom_fields_values('uuid-da-empresa-aqui', 10);

-- Teste da função de extração isoladamente:
-- SELECT 
--     l.id,
--     l.nome,
--     jsonb_typeof(l.custom_fields_values) as tipo,
--     get_corretor_responsavel_from_custom_fields(l.custom_fields_values) as corretor
-- FROM public.leads l 
-- WHERE l.custom_fields_values IS NOT NULL 
-- LIMIT 5;