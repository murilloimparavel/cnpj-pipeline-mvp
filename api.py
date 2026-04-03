"""CNPJ Pipeline API - Query Brazilian company data."""
import csv
import hmac
import io
import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from typing import Optional, List, Tuple

from fastapi import FastAPI, HTTPException, Request, Security, Query, Depends
from fastapi.responses import StreamingResponse
from fastapi.security import APIKeyHeader
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="CNPJ Pipeline API", version="3.0.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

API_KEY = os.environ["API_KEY"]
DATABASE_URL = os.getenv("DATABASE_URL", "")

api_key_header = APIKeyHeader(name="X-API-Key")

EXPORT_MAX_ROWS = 10_000
CNAE_LIST_MAX = 20

# ---------------------------------------------------------------------------
# Connection Pool (AC1)
# ---------------------------------------------------------------------------
pool: ThreadedConnectionPool = None


@app.on_event("startup")
def _init_pool():
    global pool
    pool = ThreadedConnectionPool(
        minconn=2,
        maxconn=10,
        dsn=DATABASE_URL,
        options="-c statement_timeout=30000",
    )
    logger.info("Connection pool initialised (min=2, max=10, statement_timeout=30s)")


@contextmanager
def get_conn():
    """Get a connection from the pool with automatic commit/rollback."""
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


# ---------------------------------------------------------------------------
# Security (AC6)
# ---------------------------------------------------------------------------
def verify_key(key: str = Security(api_key_header)):
    if not hmac.compare_digest(key, API_KEY):
        raise HTTPException(status_code=403, detail="Invalid API key")
    return key


# ---------------------------------------------------------------------------
# DRY Filter Params (AC8)
# ---------------------------------------------------------------------------
@dataclass
class FilterParams:
    cnae: Optional[str] = Query(None, description="CNAE fiscal principal (ex: 7311400)")
    uf: Optional[str] = Query(None, description="Estado (ex: SP, RJ)")
    municipio: Optional[str] = Query(None, description="Codigo municipio IBGE")
    com_email: bool = Query(False, description="Filtrar apenas com email")
    situacao: str = Query("02", description="Situacao cadastral (02=Ativa)")
    natureza_juridica: Optional[str] = Query(None, description="Codigo natureza juridica (ex: 2062=LTDA)")
    capital_social_min: Optional[float] = Query(None, description="Capital social minimo", ge=0)
    capital_social_max: Optional[float] = Query(None, description="Capital social maximo", ge=0)
    porte: Optional[str] = Query(None, description="Porte da empresa (00, 01, 03, 05)")
    com_telefone: bool = Query(False, description="Filtrar apenas com telefone")
    cnae_list: Optional[str] = Query(None, description="Lista de CNAEs separados por virgula (ex: 7810800,7820500)")
    razao_social: Optional[str] = Query(None, description="Busca textual na razao social (ILIKE)")
    nome_fantasia: Optional[str] = Query(None, description="Busca textual no nome fantasia (ILIKE)")
    data_inicio_min: Optional[date] = Query(None, description="Data inicio atividade minima (YYYY-MM-DD)")
    data_inicio_max: Optional[date] = Query(None, description="Data inicio atividade maxima (YYYY-MM-DD)")
    bairro: Optional[str] = Query(None, description="Bairro (exact match, uppercase)")
    cidade: Optional[str] = Query(None, description="Nome da cidade (ILIKE search)")


# ---------------------------------------------------------------------------
# Query builders
# ---------------------------------------------------------------------------
def build_filters(f: FilterParams) -> Tuple[List[str], List, bool, bool]:
    """Build WHERE conditions and params from filters.

    Returns (conditions, params, needs_emp_join, needs_mun_join).
    """
    conditions: List[str] = []
    params: List = []
    needs_emp_join = False
    needs_mun_join = False

    # -- estabelecimentos filters --
    if f.cnae:
        conditions.append("e.cnae_fiscal_principal = %s")
        params.append(f.cnae)
    if f.cnae_list:
        cnae_codes = [c.strip() for c in f.cnae_list.split(",") if c.strip()]
        if len(cnae_codes) > CNAE_LIST_MAX:
            raise HTTPException(400, f"cnae_list max {CNAE_LIST_MAX} items, got {len(cnae_codes)}")
        if cnae_codes:
            placeholders = ", ".join(["%s"] * len(cnae_codes))
            conditions.append(f"e.cnae_fiscal_principal IN ({placeholders})")
            params.extend(cnae_codes)
    if f.uf:
        conditions.append("e.uf = %s")
        params.append(f.uf.upper())
    if f.municipio:
        conditions.append("e.municipio = %s")
        params.append(f.municipio)
    if f.com_email:
        conditions.append("e.correio_eletronico IS NOT NULL AND e.correio_eletronico != ''")
    if f.com_telefone:
        conditions.append("e.telefone_1 IS NOT NULL AND e.telefone_1 != ''")
    if f.situacao:
        conditions.append("e.situacao_cadastral = %s")
        params.append(f.situacao)
    if f.nome_fantasia:
        conditions.append("e.nome_fantasia ILIKE %s")
        params.append(f"%{f.nome_fantasia}%")
    if f.bairro:
        conditions.append("e.bairro = %s")
        params.append(f.bairro.upper())
    if f.data_inicio_min:
        conditions.append("e.data_inicio_atividade >= %s")
        params.append(f.data_inicio_min)
    if f.data_inicio_max:
        conditions.append("e.data_inicio_atividade <= %s")
        params.append(f.data_inicio_max)

    # -- empresas filters (require JOIN) --
    if f.natureza_juridica:
        conditions.append("emp.natureza_juridica = %s")
        params.append(f.natureza_juridica)
        needs_emp_join = True
    if f.capital_social_min is not None:
        conditions.append("emp.capital_social >= %s")
        params.append(f.capital_social_min)
        needs_emp_join = True
    if f.capital_social_max is not None:
        conditions.append("emp.capital_social <= %s")
        params.append(f.capital_social_max)
        needs_emp_join = True
    if f.porte:
        conditions.append("emp.porte = %s")
        params.append(f.porte)
        needs_emp_join = True
    if f.razao_social:
        conditions.append("emp.razao_social ILIKE %s")
        params.append(f"%{f.razao_social}%")
        needs_emp_join = True

    # -- municipios filter (require JOIN) --
    if f.cidade:
        conditions.append("m.descricao ILIKE %s")
        params.append(f"%{f.cidade}%")
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


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
@limiter.limit("30/minute")
def health(request: Request):
    return {"status": "ok"}


@app.get("/api/v1/estabelecimentos")
@limiter.limit("30/minute")
def search_estabelecimentos(
    request: Request,
    filters: FilterParams = Depends(),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    _: str = Security(verify_key),
):
    """Search establishments with advanced filters for prospecting."""
    conditions, params, needs_emp, needs_mun = build_filters(filters)

    if not conditions:
        raise HTTPException(400, "At least one filter required")

    where = " AND ".join(conditions)
    from_clause = build_from_clause(needs_emp, needs_mun)

    # AC5: Use COUNT(*) OVER() window function to eliminate double scan
    # Only compute total on first page (offset=0)
    if offset == 0:
        data_params = list(params)
        data_params.extend([limit, offset])
        query = f"""
            SELECT {SELECT_COLUMNS},
                   COUNT(*) OVER() AS _total_count
            {from_clause}
            WHERE {where}
            ORDER BY emp.razao_social
            LIMIT %s OFFSET %s
        """
    else:
        data_params = list(params)
        data_params.extend([limit, offset])
        query = f"""
            SELECT {SELECT_COLUMNS}
            {from_clause}
            WHERE {where}
            ORDER BY emp.razao_social
            LIMIT %s OFFSET %s
        """

    t0 = time.monotonic()
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, data_params)
            rows = cur.fetchall()

    elapsed = time.monotonic() - t0
    if elapsed > 1.0:
        logger.warning(
            "Slow query (%.2fs): search_estabelecimentos filters=%s",
            elapsed,
            {k: v for k, v in filters.__dict__.items() if v},
        )

    if offset == 0 and rows:
        total = rows[0]["_total_count"]
        data = [{k: v for k, v in dict(r).items() if k != "_total_count"} for r in rows]
    elif offset == 0:
        total = 0
        data = []
    else:
        total = -1  # not computed for offset > 0
        data = [dict(r) for r in rows]

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "data": data,
    }


@app.get("/api/v1/estabelecimentos/count")
@limiter.limit("30/minute")
def count_estabelecimentos(
    request: Request,
    filters: FilterParams = Depends(),
    _: str = Security(verify_key),
):
    """Count establishments matching filters (no data returned, faster)."""
    conditions, params, needs_emp, needs_mun = build_filters(filters)

    if not conditions:
        raise HTTPException(400, "At least one filter required")

    where = " AND ".join(conditions)
    from_clause = build_from_clause(needs_emp, needs_mun)
    count_query = f"SELECT COUNT(*) {from_clause} WHERE {where}"

    t0 = time.monotonic()
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(count_query, params)
            total = cur.fetchone()["count"]

    elapsed = time.monotonic() - t0
    if elapsed > 1.0:
        logger.warning("Slow query (%.2fs): count_estabelecimentos", elapsed)

    return {"total": total}


@app.get("/api/v1/estabelecimentos/export")
@limiter.limit("30/minute")
def export_estabelecimentos(
    request: Request,
    filters: FilterParams = Depends(),
    _: str = Security(verify_key),
):
    """Export establishments as CSV (max 10,000 rows) using server-side cursor."""
    conditions, params, needs_emp, needs_mun = build_filters(filters)

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

    # AC4: Server-side cursor with streaming
    def stream_csv():
        conn = pool.getconn()
        try:
            with conn.cursor(name="csv_export") as cur:
                cur.itersize = 2000
                cur.execute(query, params)
                # Fetch first batch to populate cur.description
                first_batch = cur.fetchmany(2000)
                if not first_batch:
                    yield ""
                    return
                # header from cursor description
                col_names = [desc[0] for desc in cur.description]
                buf = io.StringIO()
                writer = csv.writer(buf)
                writer.writerow(col_names)
                yield buf.getvalue()
                # yield first batch
                buf = io.StringIO()
                writer = csv.writer(buf)
                writer.writerows(first_batch)
                yield buf.getvalue()
                # remaining data in chunks
                while True:
                    rows = cur.fetchmany(2000)
                    if not rows:
                        break
                    buf = io.StringIO()
                    writer = csv.writer(buf)
                    writer.writerows(rows)
                    yield buf.getvalue()
            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception("Error during CSV export streaming")
            raise
        finally:
            pool.putconn(conn)

    return StreamingResponse(
        stream_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=estabelecimentos.csv"},
    )


@app.get("/api/v1/cnpj/{cnpj}")
@limiter.limit("30/minute")
def lookup_cnpj(
    request: Request,
    cnpj: str,
    _: str = Security(verify_key),
):
    """Lookup a single CNPJ (just digits, no formatting)."""
    cnpj_clean = cnpj.replace(".", "").replace("/", "").replace("-", "")

    if len(cnpj_clean) != 14:
        raise HTTPException(400, "CNPJ must have 14 digits")

    # SEC-06: Validate CNPJ contains only digits
    if not cnpj_clean.isdigit():
        raise HTTPException(400, "CNPJ must contain only digits")

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

    t0 = time.monotonic()
    with get_conn() as conn:
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

    elapsed = time.monotonic() - t0
    if elapsed > 1.0:
        logger.warning("Slow query (%.2fs): lookup_cnpj cnpj=%s", elapsed, cnpj_basico)

    result = dict(row)
    result["socios"] = [dict(s) for s in socios]
    return result
