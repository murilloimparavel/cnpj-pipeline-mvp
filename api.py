"""CNPJ Pipeline API - Query Brazilian company data."""
import csv
import io
import os
from datetime import date
from typing import Optional, List, Tuple

from fastapi import FastAPI, HTTPException, Security, Query
from fastapi.responses import StreamingResponse
from fastapi.security import APIKeyHeader
import psycopg2
import psycopg2.extras

app = FastAPI(title="CNPJ Pipeline API", version="2.0.0")

API_KEY = os.getenv("API_KEY", "cnpj-pipeline-default-key-2026")
DATABASE_URL = os.getenv("DATABASE_URL", "")

api_key_header = APIKeyHeader(name="X-API-Key")

EXPORT_MAX_ROWS = 10_000


def verify_key(key: str = Security(api_key_header)):
    if key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return key


def get_db():
    return psycopg2.connect(DATABASE_URL)


def build_filters(
    cnae: Optional[str],
    uf: Optional[str],
    municipio: Optional[str],
    com_email: bool,
    situacao: Optional[str],
    natureza_juridica: Optional[str],
    capital_social_min: Optional[float],
    capital_social_max: Optional[float],
    porte: Optional[str],
    com_telefone: bool,
    cnae_list: Optional[str],
    razao_social: Optional[str],
    nome_fantasia: Optional[str],
    data_inicio_min: Optional[date],
    data_inicio_max: Optional[date],
    bairro: Optional[str],
    cidade: Optional[str],
) -> Tuple[List[str], List, bool, bool]:
    """Build WHERE conditions and params from filters.

    Returns (conditions, params, needs_emp_join, needs_mun_join).
    """
    conditions: List[str] = []
    params: List = []
    needs_emp_join = False
    needs_mun_join = False

    # -- estabelecimentos filters --
    if cnae:
        conditions.append("e.cnae_fiscal_principal = %s")
        params.append(cnae)
    if cnae_list:
        cnae_codes = [c.strip() for c in cnae_list.split(",") if c.strip()]
        if cnae_codes:
            placeholders = ", ".join(["%s"] * len(cnae_codes))
            conditions.append(f"e.cnae_fiscal_principal IN ({placeholders})")
            params.extend(cnae_codes)
    if uf:
        conditions.append("e.uf = %s")
        params.append(uf.upper())
    if municipio:
        conditions.append("e.municipio = %s")
        params.append(municipio)
    if com_email:
        conditions.append("e.correio_eletronico IS NOT NULL AND e.correio_eletronico != ''")
    if com_telefone:
        conditions.append("e.telefone_1 IS NOT NULL AND e.telefone_1 != ''")
    if situacao:
        conditions.append("e.situacao_cadastral = %s")
        params.append(situacao)
    if nome_fantasia:
        conditions.append("e.nome_fantasia ILIKE %s")
        params.append(f"%{nome_fantasia}%")
    if bairro:
        conditions.append("e.bairro = %s")
        params.append(bairro.upper())
    if data_inicio_min:
        conditions.append("e.data_inicio_atividade >= %s")
        params.append(data_inicio_min)
    if data_inicio_max:
        conditions.append("e.data_inicio_atividade <= %s")
        params.append(data_inicio_max)

    # -- empresas filters (require JOIN) --
    if natureza_juridica:
        conditions.append("emp.natureza_juridica = %s")
        params.append(natureza_juridica)
        needs_emp_join = True
    if capital_social_min is not None:
        conditions.append("emp.capital_social >= %s")
        params.append(capital_social_min)
        needs_emp_join = True
    if capital_social_max is not None:
        conditions.append("emp.capital_social <= %s")
        params.append(capital_social_max)
        needs_emp_join = True
    if porte:
        conditions.append("emp.porte = %s")
        params.append(porte)
        needs_emp_join = True
    if razao_social:
        conditions.append("emp.razao_social ILIKE %s")
        params.append(f"%{razao_social}%")
        needs_emp_join = True

    # -- municipios filter (require JOIN) --
    if cidade:
        conditions.append("m.descricao ILIKE %s")
        params.append(f"%{cidade}%")
        needs_mun_join = True

    return conditions, params, needs_emp_join, needs_mun_join


def build_from_clause(needs_emp_join: bool, needs_mun_join: bool) -> str:
    """Build FROM + JOINs clause.

    Uses INNER JOIN when filtering on joined tables (better performance),
    LEFT JOIN otherwise (to include rows without matches).
    """
    sql = "FROM estabelecimentos e"
    if needs_emp_join:
        sql += "\n        JOIN empresas emp ON emp.cnpj_basico = e.cnpj_basico"
    else:
        sql += "\n        LEFT JOIN empresas emp ON emp.cnpj_basico = e.cnpj_basico"
    if needs_mun_join:
        sql += "\n        JOIN municipios m ON m.codigo = e.municipio"
    else:
        sql += "\n        LEFT JOIN municipios m ON m.codigo = e.municipio"
    return sql


SELECT_COLUMNS = """
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
            emp.capital_social,
            emp.natureza_juridica,
            emp.porte"""


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/v1/estabelecimentos")
def search_estabelecimentos(
    cnae: Optional[str] = Query(None, description="CNAE fiscal principal (ex: 7311400)"),
    uf: Optional[str] = Query(None, description="Estado (ex: SP, RJ)"),
    municipio: Optional[str] = Query(None, description="Codigo municipio IBGE"),
    com_email: bool = Query(False, description="Filtrar apenas com email"),
    situacao: str = Query("02", description="Situacao cadastral (02=Ativa)"),
    natureza_juridica: Optional[str] = Query(None, description="Codigo natureza juridica (ex: 2062=LTDA)"),
    capital_social_min: Optional[float] = Query(None, description="Capital social minimo", ge=0),
    capital_social_max: Optional[float] = Query(None, description="Capital social maximo", ge=0),
    porte: Optional[str] = Query(None, description="Porte da empresa (00, 01, 03, 05)"),
    com_telefone: bool = Query(False, description="Filtrar apenas com telefone"),
    cnae_list: Optional[str] = Query(None, description="Lista de CNAEs separados por virgula (ex: 7810800,7820500)"),
    razao_social: Optional[str] = Query(None, description="Busca textual na razao social (ILIKE)"),
    nome_fantasia: Optional[str] = Query(None, description="Busca textual no nome fantasia (ILIKE)"),
    data_inicio_min: Optional[date] = Query(None, description="Data inicio atividade minima (YYYY-MM-DD)"),
    data_inicio_max: Optional[date] = Query(None, description="Data inicio atividade maxima (YYYY-MM-DD)"),
    bairro: Optional[str] = Query(None, description="Bairro (exact match, uppercase)"),
    cidade: Optional[str] = Query(None, description="Nome da cidade (ILIKE search)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    _: str = Security(verify_key),
):
    """Search establishments with advanced filters for prospecting."""
    conditions, params, needs_emp, needs_mun = build_filters(
        cnae=cnae, uf=uf, municipio=municipio, com_email=com_email,
        situacao=situacao, natureza_juridica=natureza_juridica,
        capital_social_min=capital_social_min, capital_social_max=capital_social_max,
        porte=porte, com_telefone=com_telefone, cnae_list=cnae_list,
        razao_social=razao_social, nome_fantasia=nome_fantasia,
        data_inicio_min=data_inicio_min, data_inicio_max=data_inicio_max,
        bairro=bairro, cidade=cidade,
    )

    if not conditions:
        raise HTTPException(400, "At least one filter required")

    where = " AND ".join(conditions)
    from_clause = build_from_clause(needs_emp, needs_mun)

    # Count query
    count_params = list(params)
    count_query = f"SELECT COUNT(*) {from_clause} WHERE {where}"

    # Data query
    data_params = list(params)
    data_params.extend([limit, offset])

    query = f"""
        SELECT {SELECT_COLUMNS}
        {from_clause}
        WHERE {where}
        ORDER BY emp.razao_social
        LIMIT %s OFFSET %s
    """

    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(count_query, count_params)
            total = cur.fetchone()["count"]

            cur.execute(query, data_params)
            rows = cur.fetchall()

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "data": [dict(r) for r in rows],
        }
    finally:
        conn.close()


@app.get("/api/v1/estabelecimentos/count")
def count_estabelecimentos(
    cnae: Optional[str] = Query(None, description="CNAE fiscal principal"),
    uf: Optional[str] = Query(None, description="Estado (ex: SP, RJ)"),
    municipio: Optional[str] = Query(None, description="Codigo municipio IBGE"),
    com_email: bool = Query(False, description="Filtrar apenas com email"),
    situacao: str = Query("02", description="Situacao cadastral (02=Ativa)"),
    natureza_juridica: Optional[str] = Query(None, description="Codigo natureza juridica"),
    capital_social_min: Optional[float] = Query(None, ge=0),
    capital_social_max: Optional[float] = Query(None, ge=0),
    porte: Optional[str] = Query(None, description="Porte da empresa"),
    com_telefone: bool = Query(False, description="Filtrar apenas com telefone"),
    cnae_list: Optional[str] = Query(None, description="Lista de CNAEs separados por virgula"),
    razao_social: Optional[str] = Query(None, description="Busca textual na razao social"),
    nome_fantasia: Optional[str] = Query(None, description="Busca textual no nome fantasia"),
    data_inicio_min: Optional[date] = Query(None),
    data_inicio_max: Optional[date] = Query(None),
    bairro: Optional[str] = Query(None),
    cidade: Optional[str] = Query(None, description="Nome da cidade (ILIKE)"),
    _: str = Security(verify_key),
):
    """Count establishments matching filters (no data returned, faster)."""
    conditions, params, needs_emp, needs_mun = build_filters(
        cnae=cnae, uf=uf, municipio=municipio, com_email=com_email,
        situacao=situacao, natureza_juridica=natureza_juridica,
        capital_social_min=capital_social_min, capital_social_max=capital_social_max,
        porte=porte, com_telefone=com_telefone, cnae_list=cnae_list,
        razao_social=razao_social, nome_fantasia=nome_fantasia,
        data_inicio_min=data_inicio_min, data_inicio_max=data_inicio_max,
        bairro=bairro, cidade=cidade,
    )

    if not conditions:
        raise HTTPException(400, "At least one filter required")

    where = " AND ".join(conditions)
    from_clause = build_from_clause(needs_emp, needs_mun)
    count_query = f"SELECT COUNT(*) {from_clause} WHERE {where}"

    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(count_query, params)
            total = cur.fetchone()["count"]
        return {"total": total}
    finally:
        conn.close()


@app.get("/api/v1/estabelecimentos/export")
def export_estabelecimentos(
    cnae: Optional[str] = Query(None, description="CNAE fiscal principal"),
    uf: Optional[str] = Query(None, description="Estado (ex: SP, RJ)"),
    municipio: Optional[str] = Query(None, description="Codigo municipio IBGE"),
    com_email: bool = Query(False, description="Filtrar apenas com email"),
    situacao: str = Query("02", description="Situacao cadastral (02=Ativa)"),
    natureza_juridica: Optional[str] = Query(None, description="Codigo natureza juridica"),
    capital_social_min: Optional[float] = Query(None, ge=0),
    capital_social_max: Optional[float] = Query(None, ge=0),
    porte: Optional[str] = Query(None, description="Porte da empresa"),
    com_telefone: bool = Query(False, description="Filtrar apenas com telefone"),
    cnae_list: Optional[str] = Query(None, description="Lista de CNAEs separados por virgula"),
    razao_social: Optional[str] = Query(None, description="Busca textual na razao social"),
    nome_fantasia: Optional[str] = Query(None, description="Busca textual no nome fantasia"),
    data_inicio_min: Optional[date] = Query(None),
    data_inicio_max: Optional[date] = Query(None),
    bairro: Optional[str] = Query(None),
    cidade: Optional[str] = Query(None, description="Nome da cidade (ILIKE)"),
    _: str = Security(verify_key),
):
    """Export establishments as CSV (max 10,000 rows)."""
    conditions, params, needs_emp, needs_mun = build_filters(
        cnae=cnae, uf=uf, municipio=municipio, com_email=com_email,
        situacao=situacao, natureza_juridica=natureza_juridica,
        capital_social_min=capital_social_min, capital_social_max=capital_social_max,
        porte=porte, com_telefone=com_telefone, cnae_list=cnae_list,
        razao_social=razao_social, nome_fantasia=nome_fantasia,
        data_inicio_min=data_inicio_min, data_inicio_max=data_inicio_max,
        bairro=bairro, cidade=cidade,
    )

    if not conditions:
        raise HTTPException(400, "At least one filter required")

    where = " AND ".join(conditions)
    from_clause = build_from_clause(needs_emp, needs_mun)
    params.append(EXPORT_MAX_ROWS)

    query = f"""
        SELECT {SELECT_COLUMNS}
        {from_clause}
        WHERE {where}
        ORDER BY emp.razao_social
        LIMIT %s
    """

    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        def generate_csv():
            output = io.StringIO()
            if rows:
                writer = csv.DictWriter(output, fieldnames=rows[0].keys())
                writer.writeheader()
                yield output.getvalue()
                output.seek(0)
                output.truncate(0)
                for row in rows:
                    writer.writerow(
                        {k: (str(v) if v is not None else "") for k, v in dict(row).items()}
                    )
                    yield output.getvalue()
                    output.seek(0)
                    output.truncate(0)
            else:
                yield ""

        return StreamingResponse(
            generate_csv(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=estabelecimentos.csv"},
        )
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
