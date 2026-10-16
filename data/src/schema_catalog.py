"""Compare application structure without reading or changing application records.

The reference is generated from schema.sql on a disposable database. Generated
TimescaleDB IDs and operational timestamps are excluded; policy settings and
logical object names remain part of the comparison.
"""

from __future__ import annotations

import json

import psycopg2.extensions
from psycopg2 import sql

from schema_normalize import comparison_rows

Catalog = dict[str, list[str]]

# Installer bookkeeping is checked by the installer, outside the application
# contract. Extension-owned functions are managed by the pinned extension.
RELATIONS = """
    SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relkind NOT IN ('i', 'I')
      AND c.relname NOT IN ('schema_migrations', 'schema_installation')
      AND NOT EXISTS (SELECT 1 FROM pg_depend d
          WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid
            AND d.deptype = 'e')
"""

QUERIES: dict[str, str] = {
    "relations": """
        SELECT relname, relkind, relpersistence, relrowsecurity, relforcerowsecurity
        FROM pg_class WHERE oid IN ({RELATIONS})
    """,
    "columns": """
        SELECT c.relname, a.attname, a.attnum, format_type(a.atttypid, a.atttypmod),
               a.attnotnull, a.attidentity, a.attgenerated,
               pg_get_expr(d.adbin, d.adrelid), coll.collname
        FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
        LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        LEFT JOIN pg_collation coll ON coll.oid = a.attcollation
        WHERE c.oid IN ({RELATIONS}) AND c.relkind IN ('r','p','v','m')
          AND a.attnum > 0 AND NOT a.attisdropped
    """,
    "constraints": """
        SELECT c.relname, k.conname, pg_get_constraintdef(k.oid), k.convalidated
        FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid
        WHERE c.oid IN ({RELATIONS})
    """,
    "indexes": """
        SELECT t.relname, c.relname, pg_get_indexdef(i.indexrelid),
               i.indisvalid, i.indisready, i.indisreplident
        FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_class t ON t.oid = i.indrelid
        WHERE t.oid IN ({RELATIONS})
    """,
    "triggers": """
        SELECT c.relname, t.tgname,
               coalesce(replace(pg_get_triggerdef(t.oid),
                   '(' || quote_literal(h.id::text) || ')',
                   '(' || quote_literal(h.schema_name || '.' || h.table_name) || ')'),
                   pg_get_triggerdef(t.oid)), t.tgenabled
        FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
        LEFT JOIN _timescaledb_catalog.hypertable h
          ON t.tgfoid = to_regprocedure(
            '_timescaledb_functions.continuous_agg_invalidation_trigger()')
          AND t.tgargs = convert_to(h.id::text, 'UTF8') || decode('00', 'hex')
        WHERE c.oid IN ({RELATIONS}) AND NOT t.tgisinternal
    """,
    "functions": """
        SELECT p.proname, pg_get_function_identity_arguments(p.oid),
               pg_get_functiondef(p.oid),
               EXISTS (SELECT 1 FROM aclexplode(
                   coalesce(p.proacl, acldefault('f', p.proowner))) acl
                   WHERE acl.grantee = 0 AND acl.privilege_type = 'EXECUTE')
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public' AND NOT EXISTS (
            SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_proc'::regclass
                AND d.objid = p.oid AND d.deptype = 'e')
    """,
    "types": """
        SELECT t.typname, t.typtype, format_type(t.typbasetype, t.typtypmod),
               t.typnotnull, t.typdefault,
               ARRAY(SELECT e.enumlabel FROM pg_enum e
                     WHERE e.enumtypid = t.oid ORDER BY e.enumsortorder),
               ARRAY(SELECT pg_get_constraintdef(k.oid) FROM pg_constraint k
                     WHERE k.contypid = t.oid ORDER BY k.conname)
        FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = 'public' AND t.typtype IN ('e','d')
    """,
    "views": """
        SELECT c.relname, pg_get_viewdef(c.oid), c.reloptions
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind IN ('v','m')
          AND c.relname NOT IN (
            SELECT view_name FROM timescaledb_information.continuous_aggregates
            WHERE view_schema = 'public')
    """,
    "sequences": """
        SELECT c.relname, format_type(s.seqtypid, NULL), s.seqstart,
               s.seqincrement, s.seqmax, s.seqmin, s.seqcache, s.seqcycle,
               t.relname, a.attname
        FROM pg_sequence s JOIN pg_class c ON c.oid = s.seqrelid
        LEFT JOIN pg_depend d ON d.classid = 'pg_class'::regclass
            AND d.objid = c.oid AND d.deptype IN ('a','i')
        LEFT JOIN pg_class t ON t.oid = d.refobjid
        LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
        WHERE c.oid IN ({RELATIONS})
    """,
    "row_policies": """
        SELECT tablename, policyname, permissive, roles, cmd, qual, with_check
        FROM pg_policies WHERE schemaname = 'public'
    """,
    "hypertables": """
        SELECT hypertable_name, num_dimensions, compression_enabled
        FROM timescaledb_information.hypertables WHERE hypertable_schema = 'public'
    """,
    "dimensions": """
        SELECT hypertable_name, dimension_number, column_name, column_type,
               dimension_type, time_interval::text, integer_interval,
               integer_now_func, num_partitions
        FROM timescaledb_information.dimensions WHERE hypertable_schema = 'public'
    """,
    "compression": """
        SELECT hypertable_name, attname, segmentby_column_index,
               orderby_column_index, orderby_asc, orderby_nullsfirst
        FROM timescaledb_information.compression_settings
        WHERE hypertable_schema = 'public'
    """,
    # Recent TimescaleDB versions support only finalized aggregates and remove
    # that information column. Older engines still expose and check the flag.
    "aggregates": """
        SELECT view_name, hypertable_schema, hypertable_name, materialized_only,
               compression_enabled, view_definition,
               coalesce((to_jsonb(a)->>'finalized')::boolean, true)
        FROM timescaledb_information.continuous_aggregates a
        WHERE view_schema = 'public'
    """,
    "jobs": """
        SELECT coalesce(a.view_name, j.hypertable_name),
               j.proc_schema, j.proc_name, j.schedule_interval::text,
               j.max_runtime::text, j.max_retries, j.retry_period::text,
               j.scheduled, j.fixed_schedule,
               j.config - 'hypertable_id' - 'mat_hypertable_id',
               j.check_schema, j.check_name
        FROM timescaledb_information.jobs j
        LEFT JOIN timescaledb_information.continuous_aggregates a
          ON a.materialization_hypertable_schema = j.hypertable_schema
          AND a.materialization_hypertable_name = j.hypertable_name
        WHERE j.hypertable_schema = 'public' OR a.view_schema = 'public'
    """,
}


def database_versions(conn: psycopg2.extensions.connection) -> dict[str, str]:
    """Return the versions used to interpret catalog definitions."""
    with conn.cursor() as cursor:
        cursor.execute("SHOW server_version_num")
        version = cursor.fetchone()
        if version is None:
            raise RuntimeError("PostgreSQL did not return its version")
        major = str(int(version[0]) // 10000)
        cursor.execute(
            "SELECT extversion FROM pg_extension WHERE extname='timescaledb'"
        )
        row = cursor.fetchone()
    return {"postgres_major": major, "timescaledb": str(row[0]) if row else "absent"}


def read_catalog(conn: psycopg2.extensions.connection) -> Catalog:
    """Read deterministic definitions, excluding stored data and generated IDs."""
    result: Catalog = {}
    with conn.cursor() as cursor:
        cursor.execute("SET search_path = public, pg_catalog")
        cursor.execute("SET TIME ZONE 'UTC'")
        cursor.execute("SET intervalstyle = 'postgres'")
        for section, query in QUERIES.items():
            cursor.execute(sql.SQL(query).format(RELATIONS=sql.SQL(RELATIONS)))
            result[section] = sorted(
                json.dumps(list(row), sort_keys=True, default=str)
                for row in cursor.fetchall()
            )
    return result


def differences(expected: Catalog, actual: Catalog) -> list[str]:
    """Describe changed object identities without logging function bodies or data."""
    changes: list[str] = []
    for section in sorted(expected.keys() | actual.keys()):
        before = comparison_rows(section, expected.get(section, []))
        after = comparison_rows(section, actual.get(section, []))
        if before != after:
            names = sorted(
                {str(json.loads(row)[0]) for row in set(before) ^ set(after)}
            )
            changes.append(f"{section}: {', '.join(names) or 'section differs'}")
    return changes
