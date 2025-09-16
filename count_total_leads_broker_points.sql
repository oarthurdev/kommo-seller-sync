-- Script SQL para contagem de total_leads em broker_points baseado em custom_fields_values
-- Execute este script no seu banco de dados PostgreSQL

-- 1. Adicionar coluna total_leads na tabela broker_points se não existir
ALTER TABLE public.broker_points 
ADD COLUMN IF NOT EXISTS total_leads integer DEFAULT 0;

-- 2. Comentário na nova coluna
COMMENT ON COLUMN public.broker_points.total_leads IS 'Contagem total de leads do corretor baseada nos custom fields "Corretor responsável"';

-- 3. Função para extrair nome do corretor dos custom_fields_values
CREATE OR REPLACE FUNCTION get_corretor_responsavel_from_custom_fields(custom_fields_json jsonb)
RETURNS text AS $$
DECLARE
    field jsonb;
    corretor_name text;
BEGIN
    -- Percorrer o array JSON
    FOR field IN SELECT jsonb_array_elements(custom_fields_json)
    LOOP
        -- Verificar se é o campo "Corretor responsável"
        IF field->>'field_name' = 'Corretor responsável' THEN
            -- Extrair o valor do primeiro item do array values
            IF jsonb_array_length(field->'values') > 0 THEN
                corretor_name := field->'values'->0->>'value';
                RETURN corretor_name;
            END IF;
        END IF;
    END LOOP;
    
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 4. Função principal para atualizar contagem de total_leads
CREATE OR REPLACE FUNCTION update_total_leads_count(p_company_id uuid)
RETURNS void AS $$
DECLARE
    filter_record record;
    broker_record record;
    leads_count integer;
    start_date_filter date;
    end_date_filter date;
BEGIN
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
                
        END LOOP;
    END LOOP;
    
    RAISE NOTICE 'Atualização de total_leads concluída para company_id: %', p_company_id;
END;
$$ LANGUAGE plpgsql;

-- 5. Função auxiliar para processar todas as empresas
CREATE OR REPLACE FUNCTION update_all_companies_total_leads()
RETURNS void AS $$
DECLARE
    company_record record;
BEGIN
    -- Para cada empresa que tem filtros configurados
    FOR company_record IN 
        SELECT DISTINCT cf.company_id
        FROM public.component_filters cf
        WHERE cf.component_name = 'ranking_metrics' 
        AND cf.filter_type = 'month'
        AND cf.month IS NOT NULL 
        AND cf.year IS NOT NULL
    LOOP
        PERFORM update_total_leads_count(company_record.company_id);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- 6. Exemplo de uso das funções criadas:

-- Para uma empresa específica:
-- SELECT update_total_leads_count('uuid-da-empresa-aqui');

-- Para todas as empresas:
-- SELECT update_all_companies_total_leads();

-- 7. Query de exemplo para verificar os resultados
-- SELECT 
--     bp.nome,
--     bp.total_leads,
--     bp.pontos,
--     bp.leads_visitados,
--     bp.updated_at
-- FROM public.broker_points bp
-- JOIN public.brokers b ON bp.id = b.id
-- WHERE bp.company_id = 'uuid-da-empresa'
-- ORDER BY bp.total_leads DESC;

-- 8. Query para debugar custom_fields_values
-- SELECT 
--     l.id,
--     l.nome,
--     l.criado_em,
--     get_corretor_responsavel_from_custom_fields(l.custom_fields_values) as corretor_responsavel,
--     l.custom_fields_values
-- FROM public.leads l 
-- WHERE l.custom_fields_values IS NOT NULL
-- AND l.company_id = 'uuid-da-empresa'
-- LIMIT 10;

-- Comentários sobre o funcionamento:
-- 
-- 1. A função get_corretor_responsavel_from_custom_fields() extrai o nome do corretor dos custom fields
-- 2. A função update_total_leads_count() processa uma empresa específica:
--    - Busca filtros de ranking_metrics do tipo month
--    - Para cada filtro, calcula o período (mês/ano)
--    - Conta leads de cada corretor no período baseado nos custom fields
--    - Atualiza a tabela broker_points
-- 3. A função update_all_companies_total_leads() executa para todas as empresas
--
-- Uso recomendado:
-- Execute periodicamente (diário/semanal) ou após sincronização de leads para manter os dados atualizados