#!/usr/bin/env python3
"""
Carga masiva genérica Excel -> PostgreSQL basada en metadata de tabla + config.ini.

Objetivo:
- Reutilizable para cualquier Excel/tabla destino.
- Escalable y mantenible por funciones.
- Validación por tipos reales de PostgreSQL.
"""

from __future__ import annotations

import argparse
import configparser
import difflib
import math
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

SPANISH_MONTHS = {
    "ene": 1,
    "feb": 2,
    "mar": 3,
    "abr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dic": 12,
}

DATE_TYPES = {"date"}
TIMESTAMP_TYPES = {"timestamp without time zone", "timestamp with time zone"}
NUMERIC_TYPES = {
    "smallint",
    "integer",
    "bigint",
    "numeric",
    "decimal",
    "real",
    "double precision",
}
BOOL_TYPES = {"boolean"}


@dataclass
class ColumnMeta:
    name: str
    data_type: str
    is_nullable: bool
    column_default: str | None
    is_identity: bool
    is_generated: bool


@dataclass
class ValidationResult:
    valid_df: pd.DataFrame
    invalid_df: pd.DataFrame


def canonicalize_header(header: str) -> str:
    """Normaliza cabeceras a snake-like base sin símbolos: minúsculas, sin acentos, sin paréntesis."""
    txt = str(header).strip().lower()
    # Elimina contenido entre paréntesis
    txt = re.sub(r"\([^\)]*\)", " ", txt)
    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    # Conserva letras/números, reemplaza bloques no alfanuméricos por separador
    txt = re.sub(r"[^a-z0-9]+", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    # Clave canonical para matching flexible
    return txt.replace(" ", "")


def to_snake_name(header: str) -> str:
    """Convierte un nombre a snake_case (útil para fallback de nombres sin mapear)."""
    txt = str(header).strip().lower()
    txt = re.sub(r"\([^\)]*\)", " ", txt)
    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    txt = re.sub(r"[^a-z0-9]+", "_", txt)
    txt = re.sub(r"_+", "_", txt).strip("_")
    return txt


def load_config(config_path: Path) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    if not config_path.exists():
        raise FileNotFoundError(f"No existe config.ini: {config_path}")
    cfg.read(config_path, encoding="utf-8")
    for section in ["postgres", "target", "input", "output", "run"]:
        if section not in cfg:
            raise KeyError(f"Falta sección [{section}] en {config_path}")
    return cfg


def get_db_params(cfg: configparser.ConfigParser) -> Dict[str, str]:
    return dict(cfg["postgres"])


def pick_excel_file(input_dir: Path, file_name: str | None = None) -> Path:
    if file_name:
        file_path = input_dir / file_name
        if not file_path.exists():
            raise FileNotFoundError(f"No existe el fichero: {file_path}")
        return file_path

    candidates = sorted(
        list(input_dir.glob("*.xlsx")) + list(input_dir.glob("*.xls")),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"No hay ficheros Excel en {input_dir}")
    return candidates[0]


def fetch_available_schemas(conn_params: Dict[str, str]) -> List[str]:
    """Obtiene schemas de usuario visibles en la base de datos."""
    sql = """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
          AND schema_name NOT LIKE 'pg_temp_%'
          AND schema_name NOT LIKE 'pg_toast_temp_%'
        ORDER BY schema_name
    """
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [r[0] for r in cur.fetchall()]


def fetch_tables_in_schema(conn_params: Dict[str, str], schema: str) -> List[str]:
    """Lista tablas base en un schema dado."""
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (schema,))
            return [r[0] for r in cur.fetchall()]


def prompt_choice(title: str, options: List[str]) -> str:
    """Muestra opciones en consola y pide selección por índice."""
    if not options:
        raise ValueError(f"No hay opciones disponibles para: {title}")

    print(f"\n[SELECCION] {title}")
    for i, opt in enumerate(options, 1):
        print(f"  {i}. {opt}")

    while True:
        raw = input(f"Selecciona una opción (1-{len(options)}): ").strip()
        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(options):
                return options[idx - 1]
        print("Selección inválida, intenta de nuevo.")


def get_table_metadata(conn_params: Dict[str, str], schema: str, table: str) -> List[ColumnMeta]:
    sql = """
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_default,
            is_identity,
            is_generated
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
    """
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (schema, table))
            rows = cur.fetchall()

    if not rows:
        raise ValueError(f"No se encontró metadata para {schema}.{table}")

    return [
        ColumnMeta(
            name=r[0],
            data_type=r[1],
            is_nullable=(r[2] == "YES"),
            column_default=r[3],
            is_identity=(r[4] == "YES"),
            is_generated=(r[5] != "NEVER"),
        )
        for r in rows
    ]


def get_insertable_columns(metadata: List[ColumnMeta]) -> List[str]:
    cols = []
    for c in metadata:
        if c.is_identity or c.is_generated:
            continue
        if c.column_default and "nextval(" in c.column_default:
            continue
        cols.append(c.name)
    return cols


def get_config_column_map(cfg: configparser.ConfigParser) -> Dict[str, str]:
    """Lee mapeos manuales desde [column_map]."""
    custom_map: Dict[str, str] = {}
    if "column_map" in cfg:
        for raw_name, target_col in cfg["column_map"].items():
            custom_map[canonicalize_header(raw_name)] = target_col.strip()
    return custom_map


def load_mapping_store(mapping_path: Path) -> configparser.ConfigParser:
    """Carga/crea mapping.ini persistente para reutilizar homologaciones por tabla."""
    cp = configparser.ConfigParser()
    cp.optionxform = str
    if mapping_path.exists():
        cp.read(mapping_path, encoding="utf-8")
    return cp


def get_stored_table_map(mapping_cp: configparser.ConfigParser, table_section: str) -> Dict[str, str]:
    """Obtiene mapeos guardados para una tabla concreta."""
    out: Dict[str, str] = {}
    if table_section in mapping_cp:
        for src, target in mapping_cp[table_section].items():
            out[canonicalize_header(src)] = target.strip()
    return out


def find_best_target_column(raw_key: str, target_columns: List[str], threshold: float) -> tuple[str | None, float]:
    """Busca mejor match por similitud textual; retorna (columna, score)."""
    best_col = None
    best_score = 0.0
    for tgt in target_columns:
        score = difflib.SequenceMatcher(None, raw_key, canonicalize_header(tgt)).ratio()
        if score > best_score:
            best_score = score
            best_col = tgt
    if best_col and best_score >= threshold:
        return best_col, best_score
    return None, best_score


def propose_header_mapping(
    raw_headers: List[str],
    target_columns: List[str],
    cfg_map: Dict[str, str],
    stored_map: Dict[str, str],
    similarity_threshold: float,
) -> pd.DataFrame:
    """Genera propuesta de homologación columna Excel -> columna tabla con método y score."""
    target_key_map = {canonicalize_header(col): col for col in target_columns}
    rows = []

    for raw_col in raw_headers:
        raw_key = canonicalize_header(raw_col)
        mapped = None
        method = ""
        score = None

        if raw_key in cfg_map:
            mapped = cfg_map[raw_key]
            method = "config_map"
            score = 1.0
        elif raw_key in stored_map:
            mapped = stored_map[raw_key]
            method = "mapping_ini"
            score = 1.0
        elif raw_key in target_key_map:
            mapped = target_key_map[raw_key]
            method = "exact"
            score = 1.0
        else:
            mapped, score = find_best_target_column(raw_key, target_columns, similarity_threshold)
            if mapped:
                method = "fuzzy"
            else:
                mapped = to_snake_name(raw_col)
                method = "fallback"
                score = 0.0

        rows.append(
            {
                "excel_columna": raw_col,
                "excel_normalizada": to_snake_name(raw_col),
                "tabla_columna_propuesta": mapped,
                "metodo": method,
                "score": round(float(score), 4) if score is not None else None,
            }
        )

    return pd.DataFrame(rows)


def apply_mapping_to_dataframe(df: pd.DataFrame, mapping_df: pd.DataFrame) -> pd.DataFrame:
    """Aplica homologación aprobada al DataFrame, gestionando duplicados."""
    df = df.copy()
    col_map = dict(zip(mapping_df["excel_columna"], mapping_df["tabla_columna_propuesta"]))

    mapped_cols: List[str] = []
    used = set()
    for raw_col in df.columns:
        mapped = col_map.get(raw_col, to_snake_name(raw_col))
        if mapped in used:
            mapped = f"{mapped}__dup"
        used.add(mapped)
        mapped_cols.append(mapped)

    df.columns = mapped_cols
    return df


def save_mapping_ini(mapping_path: Path, table_section: str, mapping_df: pd.DataFrame) -> None:
    """Guarda homologación propuesta en mapping.ini para reutilización futura."""
    cp = load_mapping_store(mapping_path)
    if table_section not in cp:
        cp[table_section] = {}

    for _, row in mapping_df.iterrows():
        cp[table_section][str(row["excel_columna"])] = str(row["tabla_columna_propuesta"])

    with mapping_path.open("w", encoding="utf-8") as f:
        cp.write(f)


def export_mapping_review(mapping_df: pd.DataFrame, output_dir: Path, schema: str, table: str) -> Path:
    """Exporta homologación a Excel para revisión manual."""
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = output_dir / f"mapping_review_{schema}_{table}_{ts}.xlsx"
    mapping_df.to_excel(out, index=False)
    return out


def confirm_mapping(mapping_df: pd.DataFrame, mapping_path: Path, review_path: Path) -> str:
    """Muestra homologación y espera confirmación del usuario: si/no/recargar."""
    print("\n[HOMOLOGACION] Propuesta columnas Excel -> Tabla")
    print(mapping_df.to_string(index=False))
    print(f"\n[HOMOLOGACION] mapping.ini: {mapping_path}")
    print(f"[HOMOLOGACION] reporte excel: {review_path}")

    while True:
        ans = input("¿El mapeo es correcto? [si/no/recargar]: ").strip().lower()
        if ans in {"si", "s", "yes", "y"}:
            return "yes"
        if ans in {"no", "n"}:
            return "no"
        if ans in {"recargar", "r", "reload"}:
            return "reload"
        print("Respuesta no válida. Usa: si / no / recargar")


def clean_text_values(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    object_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in object_cols:
        df[col] = df[col].astype("string").str.strip()
        df[col] = df[col].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    return df


def parse_excel_serial_dates(series: pd.Series, existing: pd.Series | None = None) -> pd.Series:
    s = series.astype("string").str.strip()
    parsed = existing.copy() if existing is not None else pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")

    missing = parsed.isna() & s.notna() & (s != "")
    if not missing.any():
        return parsed

    numeric_candidate = pd.to_numeric(s[missing], errors="coerce")
    serial_mask = numeric_candidate.notna() & (numeric_candidate >= 1) & (numeric_candidate <= 73050)
    if serial_mask.any():
        serial_vals = numeric_candidate[serial_mask]
        parsed_serial = pd.to_datetime(serial_vals, unit="D", origin="1899-12-30", errors="coerce")
        parsed.loc[parsed_serial.index] = parsed_serial

    return parsed


def parse_periodo_series(series: pd.Series) -> pd.Series:
    s = series.astype("string").str.strip().str.lower()
    parsed = pd.to_datetime(s, errors="coerce", dayfirst=True)

    missing = parsed.isna() & s.notna() & (s != "")
    if missing.any():
        sub = s[missing]
        pattern = re.compile(r"^(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)[\-\/]?(\d{2}|\d{4})$")

        built = []
        idxs = []
        for idx, val in sub.items():
            m = pattern.fullmatch(str(val))
            if not m:
                continue
            mon_txt, year_txt = m.group(1), m.group(2)
            month = SPANISH_MONTHS[mon_txt]
            year = int(year_txt)
            if year < 100:
                year += 2000
            try:
                built.append(pd.Timestamp(year=year, month=month, day=1))
                idxs.append(idx)
            except Exception:
                pass

        if idxs:
            parsed.loc[idxs] = pd.Series(built, index=idxs)

    return parsed


def parse_numeric_series(series: pd.Series) -> pd.Series:
    s = series.astype("string").str.strip()

    both = s.str.contains(r"\.", regex=True, na=False) & s.str.contains(",", regex=True, na=False)
    comma_only = ~both & s.str.contains(",", regex=True, na=False)

    s = s.where(~both, s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False))
    s = s.where(~comma_only, s.str.replace(",", ".", regex=False))

    # Limpia literales tipo np.float64(12.3)
    s = s.str.replace(r"^np\.float64\((.+)\)$", r"\1", regex=True)
    s = s.str.replace(r"^np\.int64\((.+)\)$", r"\1", regex=True)

    return pd.to_numeric(s, errors="coerce")


def parse_bool_series(series: pd.Series) -> pd.Series:
    return (
        series.astype("string")
        .str.strip()
        .str.lower()
        .replace(
            {
                "true": True,
                "false": False,
                "1": True,
                "0": False,
                "si": True,
                "sí": True,
                "no": False,
                "<na>": pd.NA,
                "nan": pd.NA,
                "": pd.NA,
            }
        )
    )


def drop_fully_empty_rows(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    if not cols:
        return df
    mask = df[cols].notna().any(axis=1)
    return df.loc[mask].copy()


def validate_and_transform(
    df_raw: pd.DataFrame,
    metadata: List[ColumnMeta],
    insert_cols: List[str],
    fixed_cols: List[str],
    min_year: int,
    max_year: int,
) -> ValidationResult:
    df = df_raw.copy()

    input_cols = [c for c in insert_cols if c not in fixed_cols]

    for col in input_cols:
        if col not in df.columns:
            df[col] = pd.NA

    # Sólo columnas esperadas del excel
    df = df[input_cols].copy()

    # Añadir columnas fijas para mantener esquema interno
    for col in fixed_cols:
        if col not in df.columns:
            df[col] = pd.NA

    type_map = {c.name: c.data_type for c in metadata}

    errors: List[pd.Series] = []

    for col in input_cols:
        data_type = type_map.get(col, "text")
        original = df[col]

        if data_type in DATE_TYPES or data_type in TIMESTAMP_TYPES:
            if col == "periodo":
                parsed = parse_periodo_series(original)
            else:
                parsed = pd.to_datetime(original, errors="coerce", dayfirst=True)
            parsed = parse_excel_serial_dates(original, parsed)

            bad_parse = original.notna() & (original.astype(str).str.strip() != "") & parsed.isna()
            bad_year = parsed.notna() & ((parsed.dt.year < min_year) | (parsed.dt.year > max_year))
            bad = bad_parse | bad_year
            errors.append(bad.rename(f"error_{col}"))

            if data_type in DATE_TYPES:
                df[col] = parsed.where(~bad, pd.NaT).dt.date
            else:
                df[col] = parsed.where(~bad, pd.NaT)

        elif data_type in NUMERIC_TYPES:
            parsed = parse_numeric_series(original)
            bad = original.notna() & (original.astype(str).str.strip() != "") & parsed.isna()
            errors.append(bad.rename(f"error_{col}"))
            df[col] = parsed

        elif data_type in BOOL_TYPES:
            normalized = parse_bool_series(original)
            bad = normalized.notna() & ~normalized.isin([True, False])
            errors.append(bad.rename(f"error_{col}"))
            df[col] = normalized.where(normalized.isin([True, False]), pd.NA)

        else:
            # Texto / tipos no tratados de forma especial
            pass

    # Validación cruzada opcional si existen columnas típicas
    if "fecha_inicio_proyecto" in df.columns and "fecha_fin_proyecto" in df.columns:
        start = pd.to_datetime(df["fecha_inicio_proyecto"], errors="coerce")
        end = pd.to_datetime(df["fecha_fin_proyecto"], errors="coerce")
        bad_date_order = start.notna() & end.notna() & (start > end)
        errors.append(bad_date_order.rename("error_rango_fechas_proyecto"))

    error_df = pd.concat(errors, axis=1) if errors else pd.DataFrame(index=df.index)
    row_has_error = error_df.any(axis=1) if not error_df.empty else pd.Series(False, index=df.index)

    invalid_df = df_raw.loc[row_has_error].copy()
    if not error_df.empty:
        invalid_df["errores"] = error_df.loc[row_has_error].apply(
            lambda row: ", ".join([c.replace("error_", "") for c, v in row.items() if bool(v)]),
            axis=1,
        )

    valid_df = df.loc[~row_has_error].copy()
    return ValidationResult(valid_df=valid_df, invalid_df=invalid_df)


def parse_fixed_value(token: str) -> Any:
    val = str(token).strip()
    low = val.lower()

    if low == "__now_ts__":
        return datetime.now()
    if low == "__today__":
        return datetime.now().date()
    if low == "__true__":
        return True
    if low == "__false__":
        return False
    if low == "__null__":
        return None

    return val


def apply_fixed_values(df: pd.DataFrame, cfg: configparser.ConfigParser, insert_cols: List[str]) -> pd.DataFrame:
    df = df.copy()
    if "fixed_values" not in cfg:
        return df

    for col, token in cfg["fixed_values"].items():
        if col in insert_cols:
            df[col] = parse_fixed_value(token)
    return df


def to_db_value(value):
    if pd.isna(value):
        return None

    if hasattr(value, "item") and callable(getattr(value, "item")):
        try:
            value = value.item()
        except Exception:
            pass

    if isinstance(value, str):
        txt = value.strip()
        m_float = re.fullmatch(r"np\.float64\(([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)\)", txt)
        m_int = re.fullmatch(r"np\.int64\(([-+]?\d+)\)", txt)
        if m_float:
            return float(m_float.group(1))
        if m_int:
            return int(m_int.group(1))
        return txt

    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    return value


def insert_valid_rows(
    df: pd.DataFrame,
    conn_params: Dict[str, str],
    schema: str,
    table: str,
    insert_cols: List[str],
    batch_size: int,
    progress_every: int,
) -> int:
    if df.empty:
        return 0

    rows = [tuple(to_db_value(v) for v in row) for row in df[insert_cols].itertuples(index=False, name=None)]
    total = len(rows)

    sql = f"""
        INSERT INTO {schema}.{table} ({', '.join(insert_cols)})
        VALUES %s
    """

    inserted = 0
    next_progress_mark = progress_every

    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            for start in range(0, total, batch_size):
                end = min(start + batch_size, total)
                execute_values(cur, sql, rows[start:end], page_size=batch_size)
                inserted = end

                while progress_every > 0 and inserted >= next_progress_mark:
                    print(f"[PROGRESO] Insertados {next_progress_mark} de {total} registros")
                    next_progress_mark += progress_every
        conn.commit()

    if progress_every > 0 and inserted % progress_every != 0:
        print(f"[PROGRESO] Insertados {inserted} de {total} registros")

    return inserted


def export_invalid(invalid_df: pd.DataFrame, output_dir: Path) -> Path | None:
    if invalid_df.empty:
        return None
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = output_dir / f"registros_invalidos_{ts}.xlsx"
    invalid_df.to_excel(out, index=False)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Carga masiva genérica Excel -> PostgreSQL")
    parser.add_argument("--config-path", help="Ruta a config.ini (por defecto, junto al script)")
    parser.add_argument(
        "--auto-approve-mapping",
        action="store_true",
        help="Aprueba mapeo automáticamente sin interacción",
    )
    parser.add_argument(
        "--only-mapping",
        action="store_true",
        help="Genera/valida homologación y sale sin insertar datos",
    )
    parser.add_argument(
        "--interactive-target",
        action="store_true",
        help="Permite seleccionar schema/tabla en consola a partir de lo disponible en BD",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    config_path = Path(args.config_path) if args.config_path else (script_dir / "config.ini")

    cfg = load_config(config_path)

    conn_params = get_db_params(cfg)
    schema = cfg["target"].get("schema")
    table = cfg["target"].get("table")

    if args.interactive_target:
        schemas = fetch_available_schemas(conn_params)
        schema = prompt_choice("Schema destino", schemas)
        tables = fetch_tables_in_schema(conn_params, schema)
        table = prompt_choice(f"Tabla destino en schema '{schema}'", tables)
        print(f"[SELECCION] Usando destino: {schema}.{table}")

    input_dir = Path(cfg["input"].get("input_dir"))
    file_name = cfg["input"].get("file_name", fallback="").strip() or None
    sheet_name = cfg["input"].get("sheet_name", fallback="bbdd")

    output_dir = Path(cfg["output"].get("output_dir", "./salidas"))
    mapping_file = cfg["output"].get("mapping_file", fallback="mapping.ini")
    mapping_path = (script_dir / mapping_file).resolve() if not Path(mapping_file).is_absolute() else Path(mapping_file)

    batch_size = cfg["run"].getint("batch_size", fallback=1000)
    progress_every = cfg["run"].getint("progress_every", fallback=10000)
    min_year = cfg["run"].getint("min_year", fallback=1900)
    max_year = cfg["run"].getint("max_year", fallback=2100)
    similarity_threshold = cfg["run"].getfloat("similarity_threshold", fallback=0.78)

    if not input_dir.exists() or not input_dir.is_dir():
        raise NotADirectoryError(f"Carpeta de entrada inválida: {input_dir}")

    metadata = get_table_metadata(conn_params, schema, table)
    insert_cols = get_insertable_columns(metadata)

    fixed_cols = list(cfg["fixed_values"].keys()) if "fixed_values" in cfg else []
    target_columns = [c for c in insert_cols if c not in fixed_cols]

    cfg_map = get_config_column_map(cfg)
    table_section = f"{schema}.{table}"

    excel_file = pick_excel_file(input_dir, file_name)
    print(f"[INFO] Leyendo Excel: {excel_file} (hoja: {sheet_name})")

    df_raw = pd.read_excel(excel_file, sheet_name=sheet_name)

    # 1) Proponer homologación
    stored_map = get_stored_table_map(load_mapping_store(mapping_path), table_section)
    mapping_df = propose_header_mapping(
        raw_headers=[str(c) for c in df_raw.columns],
        target_columns=target_columns,
        cfg_map=cfg_map,
        stored_map=stored_map,
        similarity_threshold=similarity_threshold,
    )

    # 2) Exportar homologación y guardar mapping.ini para revisión
    review_path = export_mapping_review(mapping_df, output_dir, schema, table)
    save_mapping_ini(mapping_path, table_section, mapping_df)

    # 3) Confirmar homologación antes de cargar
    if args.auto_approve_mapping:
        decision = "yes"
        print("[HOMOLOGACION] Auto-aprobada por --auto-approve-mapping")
    else:
        decision = confirm_mapping(mapping_df, mapping_path, review_path)
        while decision == "reload":
            print("\n[HOMOLOGACION] Recargando mapping.ini actualizado...")
            stored_map = get_stored_table_map(load_mapping_store(mapping_path), table_section)
            mapping_df = propose_header_mapping(
                raw_headers=[str(c) for c in df_raw.columns],
                target_columns=target_columns,
                cfg_map=cfg_map,
                stored_map=stored_map,
                similarity_threshold=similarity_threshold,
            )
            review_path = export_mapping_review(mapping_df, output_dir, schema, table)
            save_mapping_ini(mapping_path, table_section, mapping_df)
            decision = confirm_mapping(mapping_df, mapping_path, review_path)

    if decision == "no":
        print("[INFO] Carga detenida por usuario. Ajusta mapping.ini y ejecuta de nuevo.")
        return

    if args.only_mapping:
        print("[INFO] Modo --only-mapping: homologación confirmada. No se insertaron datos.")
        return

    # 4) Aplicar homologación aprobada y continuar proceso
    df_raw = apply_mapping_to_dataframe(df_raw, mapping_df)
    df_raw = clean_text_values(df_raw)

    excel_input_cols = [c for c in insert_cols if c not in fixed_cols]
    df_raw = drop_fully_empty_rows(df_raw, [c for c in excel_input_cols if c in df_raw.columns])

    result = validate_and_transform(
        df_raw=df_raw,
        metadata=metadata,
        insert_cols=insert_cols,
        fixed_cols=fixed_cols,
        min_year=min_year,
        max_year=max_year,
    )

    if not result.invalid_df.empty and "errores" in result.invalid_df.columns:
        print("[DIAGNOSTICO] Top errores de validación:")
        exploded = result.invalid_df["errores"].astype("string").str.split(", ").explode()
        counts = exploded.value_counts(dropna=True).head(10)
        for err, cnt in counts.items():
            print(f"  - {err}: {cnt}")

    valid_df = apply_fixed_values(result.valid_df, cfg, insert_cols)

    invalid_path = export_invalid(result.invalid_df, output_dir)
    inserted = insert_valid_rows(
        df=valid_df,
        conn_params=conn_params,
        schema=schema,
        table=table,
        insert_cols=insert_cols,
        batch_size=batch_size,
        progress_every=progress_every,
    )

    print("[RESUMEN]")
    print(f"- Filas leídas: {len(df_raw)}")
    print(f"- Filas válidas insertadas: {inserted}")
    print(f"- Filas inválidas: {len(result.invalid_df)}")
    if invalid_path:
        print(f"- Reporte inválidos: {invalid_path}")


if __name__ == "__main__":
    main()
