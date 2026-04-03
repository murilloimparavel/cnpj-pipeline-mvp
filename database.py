"""PostgreSQL database operations with Polars for fast bulk loading."""

import io
import logging
import time
from typing import List, Set
from urllib.parse import urlparse

import polars as pl
import psycopg2
from psycopg2 import sql

logger = logging.getLogger(__name__)

# SEC-03: Allowlist of valid tables for bulk_upsert
ALLOWED_TABLES = {
    "cnaes", "motivos", "municipios", "naturezas_juridicas",
    "paises", "qualificacoes_socios", "empresas",
    "estabelecimentos", "socios", "dados_simples",
}


class Database:
    """PostgreSQL database handler with temp table upsert."""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self._pk_cache: dict = {}
        self.conn = None

    def _parse_url(self) -> dict:
        """Parse DATABASE_URL into connection parameters."""
        parsed = urlparse(self.database_url)
        return {
            "host": parsed.hostname,
            "port": parsed.port or 5432,
            "database": parsed.path[1:],
            "user": parsed.username,
            "password": parsed.password,
        }

    def connect(self):
        """Establish database connection with retry."""
        if self.conn is not None:
            return

        params = self._parse_url()
        for attempt in range(4):
            try:
                self.conn = psycopg2.connect(**params)
                self.conn.autocommit = False
                return
            except psycopg2.OperationalError:
                if attempt == 3:
                    raise
                time.sleep(2**attempt)

    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_processed_files(self, directory: str) -> Set[str]:
        """Get all processed filenames for a directory."""
        self.connect()
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT filename FROM processed_files WHERE directory = %s",
                    (directory,),
                )
                return {row[0] for row in cur.fetchall()}
        except psycopg2.ProgrammingError:
            # Table may not exist yet on first run
            logger.warning("processed_files table not found, returning empty set")
            return set()
        except Exception:
            # STAB-03: Log and re-raise instead of silently returning empty set
            logger.exception("Error fetching processed files for directory=%s", directory)
            raise

    def mark_processed(self, directory: str, filename: str):
        """Mark a file as processed."""
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO processed_files (directory, filename)
                   VALUES (%s, %s)
                   ON CONFLICT (directory, filename) DO NOTHING""",
                (directory, filename),
            )
            self.conn.commit()

    def clear_processed_files(self, directory: str):
        """Clear all processed file records for a directory (for force re-processing)."""
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM processed_files WHERE directory = %s",
                (directory,),
            )
            self.conn.commit()

    def bulk_upsert(self, df: pl.DataFrame, table_name: str, columns: List[str]):
        """Bulk upsert using temp table + COPY."""
        if df.is_empty():
            return

        # SEC-03: Validate table name against allowlist
        if table_name not in ALLOWED_TABLES:
            raise ValueError(f"Invalid table: {table_name}")

        self.connect()
        temp_table = f"temp_{table_name}_{id(df)}"

        try:
            with self.conn.cursor() as cur:
                # 1. Create temp table — use sql.Identifier for table names
                cur.execute(
                    sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS INCLUDING STORAGE) ON COMMIT DROP").format(
                        sql.Identifier(temp_table),
                        sql.Identifier(table_name),
                    )
                )

                # 2. COPY to temp
                self._copy_to_temp(cur, df, temp_table, columns)

                # 3. Upsert from temp to main
                primary_keys = self._get_primary_keys(cur, table_name)
                self._upsert_from_temp(cur, temp_table, table_name, columns, primary_keys)

                self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error: {table_name}: {e}")
            raise

    def _copy_to_temp(self, cur, df: pl.DataFrame, temp_table: str, columns: List[str]):
        """COPY DataFrame to temp table using Polars CSV."""
        columns_sql = sql.SQL(", ").join([sql.Identifier(col) for col in columns])
        copy_stmt = sql.SQL("COPY {} ({}) FROM STDIN WITH CSV ENCODING 'UTF8'").format(
            sql.Identifier(temp_table),
            columns_sql,
        )
        csv_bytes = df.write_csv(include_header=False).encode("utf-8", errors="replace")
        csv_bytes = csv_bytes.replace(b"\x00", b"")

        cur.copy_expert(copy_stmt.as_string(cur.connection), io.BytesIO(csv_bytes))

    def _get_primary_keys(self, cur, table_name: str) -> List[str]:
        """Get primary key columns for a table with caching."""
        if table_name in self._pk_cache:
            return self._pk_cache[table_name]

        cur.execute(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
            ORDER BY array_position(i.indkey, a.attnum)
            """,
            (table_name,),
        )

        primary_keys = [row[0] for row in cur.fetchall()]
        self._pk_cache[table_name] = primary_keys
        return primary_keys

    def _upsert_from_temp(self, cur, temp_table: str, target_table: str, columns: List[str], primary_keys: List[str]):
        """Upsert from temp to target table."""
        cols = sql.SQL(", ").join([sql.Identifier(c) for c in columns])
        pks = sql.SQL(", ").join([sql.Identifier(pk) for pk in primary_keys])

        update_cols = [c for c in columns if c not in primary_keys]
        if update_cols:
            update_clause = sql.SQL(", ").join([
                sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c))
                for c in update_cols
            ]) + sql.SQL(", data_atualizacao = CURRENT_TIMESTAMP")
            conflict_action = sql.SQL("DO UPDATE SET ") + update_clause
        else:
            conflict_action = sql.SQL("DO NOTHING")

        stmt = sql.SQL(
            "INSERT INTO {target} ({cols}) "
            "SELECT DISTINCT ON ({pks}) {cols} FROM {temp} ORDER BY {pks} "
            "ON CONFLICT ({pks}) {action}"
        ).format(
            target=sql.Identifier(target_table),
            cols=cols,
            pks=pks,
            temp=sql.Identifier(temp_table),
            action=conflict_action,
        )
        cur.execute(stmt)
