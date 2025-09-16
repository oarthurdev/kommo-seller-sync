-- Script SQL para adicionar suporte aos custom_fields_values dos leads
-- Execute este script no seu banco de dados PostgreSQL

-- 1. Adicionar coluna para armazenar custom_fields_values na tabela leads
ALTER TABLE public.leads 
ADD COLUMN IF NOT EXISTS custom_fields_values JSONB;

-- 2. Criar índice na coluna JSONB para melhorar performance das consultas
CREATE INDEX IF NOT EXISTS idx_leads_custom_fields_values_gin 
ON public.leads USING GIN (custom_fields_values);

-- 3. Adicionar comentário na coluna para documentar seu propósito
COMMENT ON COLUMN public.leads.custom_fields_values IS 'Armazena os valores dos campos customizados retornados pelo endpoint de leads da API Kommo em formato JSON';

-- 4. Exemplo de como consultar dados específicos dos custom fields
-- (Execute apenas como exemplo, não é necessário para a estrutura)
/*
-- Exemplo 1: Buscar leads com um campo customizado específico
SELECT id, nome, custom_fields_values->'campo_exemplo' as campo_exemplo
FROM public.leads 
WHERE custom_fields_values ? 'campo_exemplo';

-- Exemplo 2: Buscar leads onde um campo customizado tem valor específico
SELECT id, nome, custom_fields_values
FROM public.leads 
WHERE custom_fields_values->>'campo_exemplo' = 'valor_procurado';

-- Exemplo 3: Buscar todos os leads com custom fields não nulos
SELECT id, nome, custom_fields_values
FROM public.leads 
WHERE custom_fields_values IS NOT NULL;
*/

-- 5. Opcional: Criar uma view para facilitar consultas dos custom fields
CREATE OR REPLACE VIEW public.leads_with_custom_fields AS
SELECT 
    l.id,
    l.nome,
    l.responsavel_id,
    l.contato_nome,
    l.valor,
    l.status_id,
    l.pipeline_id,
    l.etapa,
    l.criado_em,
    l.atualizado_em,
    l.fechado,
    l.status,
    l.company_id,
    l.custom_fields_values,
    -- Extrair campos customizados mais comuns (ajuste conforme necessário)
    l.custom_fields_values->>'telefone' as telefone_custom,
    l.custom_fields_values->>'email' as email_custom,
    l.custom_fields_values->>'origem' as origem_custom,
    l.custom_fields_values->>'observacoes' as observacoes_custom
FROM public.leads l;

-- 6. Comentário na view
COMMENT ON VIEW public.leads_with_custom_fields IS 'View que facilita o acesso aos dados dos leads incluindo os custom fields extraídos';

-- Instruções para uso:
-- 
-- 1. Execute este script no seu banco de dados PostgreSQL
-- 2. Modifique o código da API (libs/kommo_api.py) no método get_leads() para incluir 'custom_fields' no parâmetro 'with'
-- 3. Atualize o processamento dos leads para salvar o campo custom_fields_values
-- 
-- Exemplo de modificação no código Python:
-- params = {
--     "page": page,
--     "limit": per_page,
--     "with": "contacts,pipeline_id,loss_reason,catalog_elements,company,custom_fields",
--     "filter[pipeline_id]": pipeline_id
-- }
--
-- E no processamento do lead:
-- processed_leads.append({
--     ...outros campos...
--     "custom_fields_values": lead.get("custom_fields_values", {})
-- })