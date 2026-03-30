"""CNPJ Pipeline API - Query Brazilian company data."""
import os
from typing import Optional

from fastapi import FastAPI, HTTPException, Security, Query
from fastapi.security import APIKeyHeader
import psycopg2
import psycopg2.extras

app = FastAPI(title="CNPJ Pipeline API", version="1.0.0")

API_KEY = os.getenv("API_KEY", "cnpj-pipeline-default-key-2026")
DATABASE_URL = os.getenv("DATABASE_URL", "")

api_key_header = APIKeyHeader(name="X-API-Key")


def verify_key(key: str = Security(api_key_header)):
    if key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return key


def get_db():
    return psycopg2.connect(DATABASE_URL)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/v1/estabelecimentos")
def search_estabelecimentos(
    cnae: Optional[str] = Query(None, description="CNAE fiscal principal (ex: 7311400)"),
    uf: Optional[str] = Query(None, description="Estado (ex: SP, RJ)"),
    municipio: Optional[str] = Query(None, description="Código município IBGE"),
    com_email: bool = Query(False, description="Filtrar apenas com email"),
    situacao: str = Query("02", description="Situação cadastral (02=Ativa)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    _: str = Security(verify_key),
):
    """Search establishments by CNAE, UF, municipality, etc."""
    conditions = []
    params = []

    if cnae:
        conditions.append("e.cnae_fiscal_principal = %s")
        params.append(cnae)
    if uf:
        conditions.append("e.uf = %s")
        params.append(uf.upper())
    if municipio:
        conditions.append("e.municipio = %s")
        params.append(municipio)
    if com_email:
        conditions.append("e.correio_eletronico IS NOT NULL AND e.correio_eletronico != ''")
    if situacao:
        conditions.append("e.situacao_cadastral = %s")
        params.append(situacao)

    if not conditions:
        raise HTTPException(400, "At least one filter required (cnae, uf, municipio)")

    where = " AND ".join(conditions)
    params.extend([limit, offset])

    query = f"""
        SELECT
            e.cnpj_basico || '/' || e.cnpj_ordem || '-' || e.cnpj_dv as cnpj,
            emp.razao_social,
            e.nome_fantasia,
            e.correio_eletronico as email,
            e.ddd_1 || e.telefone_1 as telefone_1,
            e.ddd_2 || e.telefone_2 as telefone_2,
            e.tipo_logradouro || ' ' || e.logradouro as endereco,
            e.numero,
            e.complemento,
            e.bairro,
            e.uf,
            m.descricao as cidade,
            e.cep,
            e.cnae_fiscal_principal as cnae,
            e.data_inicio_atividade,
            emp.capital_social
        FROM estabelecimentos e
        LEFT JOIN empresas emp ON emp.cnpj_basico = e.cnpj_basico
        LEFT JOIN municipios m ON m.codigo = e.municipio
        WHERE {where}
        ORDER BY emp.razao_social
        LIMIT %s OFFSET %s
    """

    count_query = f"SELECT COUNT(*) FROM estabelecimentos e WHERE {where}"

    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(count_query, params[:-2])
            total = cur.fetchone()["count"]

            cur.execute(query, params)
            rows = cur.fetchall()

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "data": [dict(r) for r in rows],
        }
    finally:
        conn.close()


@app.get("/api/v1/cnpj/{cnpj}")
def lookup_cnpj(
    cnpj: str,
    _: str = Security(verify_key),
):
    """Lookup a single CNPJ (just digits, no formatting)."""
    cnpj_clean = cnpj.replace(".", "").replace("/", "").replace("-", "")

    if len(cnpj_clean) != 14:
        raise HTTPException(400, "CNPJ must have 14 digits")

    cnpj_basico = cnpj_clean[:8]
    cnpj_ordem = cnpj_clean[8:12]
    cnpj_dv = cnpj_clean[12:14]

    query = """
        SELECT
            e.cnpj_basico || '/' || e.cnpj_ordem || '-' || e.cnpj_dv as cnpj,
            emp.razao_social,
            e.nome_fantasia,
            e.correio_eletronico as email,
            e.ddd_1 || e.telefone_1 as telefone_1,
            e.ddd_2 || e.telefone_2 as telefone_2,
            e.tipo_logradouro || ' ' || e.logradouro as endereco,
            e.numero, e.complemento, e.bairro,
            e.uf,
            m.descricao as cidade,
            e.cep,
            e.cnae_fiscal_principal as cnae,
            c.descricao as cnae_descricao,
            e.situacao_cadastral,
            e.data_inicio_atividade,
            emp.capital_social,
            emp.natureza_juridica,
            emp.porte
        FROM estabelecimentos e
        LEFT JOIN empresas emp ON emp.cnpj_basico = e.cnpj_basico
        LEFT JOIN municipios m ON m.codigo = e.municipio
        LEFT JOIN cnaes c ON c.codigo = e.cnae_fiscal_principal
        WHERE e.cnpj_basico = %s AND e.cnpj_ordem = %s AND e.cnpj_dv = %s
    """

    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, (cnpj_basico, cnpj_ordem, cnpj_dv))
            row = cur.fetchone()

        if not row:
            raise HTTPException(404, "CNPJ not found")

        # Get partners
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT nome_socio, qualificacao_do_socio, data_entrada_sociedade FROM socios WHERE cnpj_basico = %s",
                (cnpj_basico,),
            )
            socios = cur.fetchall()

        result = dict(row)
        result["socios"] = [dict(s) for s in socios]
        return result
    finally:
        conn.close()
