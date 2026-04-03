-- Migration: Add indexes to support advanced filter queries
-- Related story: cnpj-api-advanced-filters

-- Indexes on empresas table (used in JOIN + WHERE)
CREATE INDEX IF NOT EXISTS idx_empresas_natureza_juridica ON empresas(natureza_juridica);
CREATE INDEX IF NOT EXISTS idx_empresas_porte ON empresas(porte);
CREATE INDEX IF NOT EXISTS idx_empresas_capital_social ON empresas(capital_social);

-- Indexes on estabelecimentos table (new filter columns)
CREATE INDEX IF NOT EXISTS idx_estabelecimentos_data_inicio ON estabelecimentos(data_inicio_atividade);
CREATE INDEX IF NOT EXISTS idx_estabelecimentos_bairro ON estabelecimentos(bairro);

-- Trigram index for ILIKE searches (requires pg_trgm extension)
-- Uncomment if pg_trgm is available:
-- CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- CREATE INDEX IF NOT EXISTS idx_empresas_razao_social_trgm ON empresas USING gin(razao_social gin_trgm_ops);
-- CREATE INDEX IF NOT EXISTS idx_estabelecimentos_nome_fantasia_trgm ON estabelecimentos USING gin(nome_fantasia gin_trgm_ops);
-- CREATE INDEX IF NOT EXISTS idx_municipios_descricao_trgm ON municipios USING gin(descricao gin_trgm_ops);
