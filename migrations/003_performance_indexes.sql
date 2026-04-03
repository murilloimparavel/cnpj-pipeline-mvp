-- Migration 003: Performance indexes for CNPJ API
-- Requires pg_trgm extension for trigram-based ILIKE optimization
-- All indexes use CONCURRENTLY to avoid locking the table during creation
-- NOTE: CONCURRENTLY cannot run inside a transaction block

-- Enable pg_trgm extension
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Trigram GIN indexes for ILIKE queries (AC2)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_empresas_razao_social_trgm
    ON empresas USING gin(razao_social gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estab_nome_fantasia_trgm
    ON estabelecimentos USING gin(nome_fantasia gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_municipios_descricao_trgm
    ON municipios USING gin(descricao gin_trgm_ops);

-- Partial composite index for the most common query pattern (AC3)
-- Covers: cnae + uf + situacao=02 + com_email=true
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estab_active_cnae_uf
    ON estabelecimentos (cnae_fiscal_principal, uf)
    WHERE situacao_cadastral = '02' AND correio_eletronico IS NOT NULL;

-- JOIN optimization: estabelecimentos.cnpj_basico is NOT a PK by itself
-- (PK is composite: cnpj_basico, cnpj_ordem, cnpj_dv)
-- This index speeds up the JOIN empresas ON cnpj_basico
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estab_cnpj_basico
    ON estabelecimentos (cnpj_basico);

-- Already exist from migration 002 — NOT duplicating:
-- idx_empresas_natureza_juridica ON empresas(natureza_juridica)
-- idx_empresas_porte ON empresas(porte)
-- idx_empresas_capital_social ON empresas(capital_social)
