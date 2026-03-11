#!/usr/bin/env python3
"""
Proceso de carga para la tabla:
proyecto_dashboard_defensa.converge_proyectos_financieros

Flujo:
1) Lee el último Excel de una carpeta (o un archivo concreto)
2) Limpia datos básicos
3) Valida tipos (fechas, numéricos, booleanos)
4) Separa registros inválidos para exportar informe
5) Inserta registros válidos en PostgreSQL usando credenciales desde .ini
"""

from __future__ import annotations

import argparse
import configparser
import math
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

SCHEMA = "proyecto_dashboard_defensa"
TABLE = "converge_proyectos_financieros"

# Reglas de negocio para fechas
MIN_YEAR = 1900
MAX_YEAR = 2100

DATE_COLS = [
    "periodo",
    "fecha_inicio_proyecto",
    "fecha_fin_proyecto",
]

NUMERIC_COLS = [
    "contratacion",
    "ventas",
    "margen_bruto",
    "costes_directos",
    "c_dir_corporativos",
    "c_dir_auxiliares",
    "c_elaboracion_ofertas",
    "disponibilidad",
    "actividades_id",
    "desviacion_tasa",
    "margen_directo",
    "indirectos",
    "costes_indirectos_mercado",
    "costes_indirectos_produccion",
    "costes_indirectos_comerciales",
    "margen_contribucion",
    "estructura",
    "indemnizaciones",
    "funciones_corporativas",
    "resto_estructura",
    "ebit",
    "amortizacion",
    "facturacion",
    "cobros",
    "cartera_com_final",
    "mg_cartera_com_final",
    "cartera_fin_final",
    "deuda",
    "dpf",
    "alo",
    "existencias",
]

# Usuario fijo para trazabilidad de carga masiva
MASSIVE_IMPORT_USER = "Massive Import"

# Mapeo de cabeceras Excel -> nombres de columna en base de datos
# Clave: nombre de cabecera canonicalizado (sin acentos, minúsculas, sin símbolos)
HEADER_ALIASES = {
    "periodo": "periodo",
    "operacioncdg": "operacion_cdg",
    "id": "id_externo",
    "proyectoindicador": "proyecto_indicador",
    "proyectoindicador2": "proyecto_indicador2",
    "empresa": "empresa",
    "empresa5": "empresa5",
    "mercadoglobalhom": "mercado_global_hom",
    "mercadoindrahomgln": "mercado_indra_homg_ln",
    "mercadoindrahomgnorg": "mercado_indra_homg_norg",
    "mercadoindragroup": "mercado_indra_group",
    "nivelorg2": "nivel_org_2",
    "unidaddeempresaano": "unidad_empresa_ano",
    "unidaddeempresaano8": "unidad_empresa_ano8",
    "clientecontractual": "cliente_contractual",
    "metacliente": "metacliente",
    "interlocutor": "interlocutor",
    "intergruporev": "intergrupo_rev",
    "intergruporevfyc": "intergrupo_rev_fyc",
    "intersegmento": "intersegmento",
    "segmentolineadenegocio": "segmento_linea_negocio",
    "geografiahom": "geografia_hom",
    "pais": "pais",
    "regiondefensa": "region_defensa",
    "aftermarket": "aftermarket",
    "clasedeproyecto": "clase_proyecto",
    "tipoproyectoservicionivel1": "tipo_proyecto_servicio_nivel_1",
    "tipoproyectoservicionivel2": "tipo_proyecto_servicio_nivel_2",
    "tipoproyectoservicionivel3": "tipo_proyecto_servicio_nivel_3",
    "solucionnivel2": "solucion_nivel_2",
    "solucionnivel3": "solucion_nivel_3",
    "gerentevertical": "gerente_vertical",
    "gestorvertical": "gestor_vertical",
    "fechainicioproyecto": "fecha_inicio_proyecto",
    "fechafinproyecto": "fecha_fin_proyecto",
    "grandesprogdef": "grandes_prog_def",
    "metodoreconocimientoingreso7": "metodo_reconocimiento_ingreso7",
    "indicadoragrupacion": "indicador_agrupacion",
    "mercadoindra": "mercado_indra",
    "auditable": "auditable",
    "mision": "mision",
    "programa": "programa",
    "valores": "valores",
    "contratacion": "contratacion",
    "ventas": "ventas",
    "margenbruto": "margen_bruto",
    "costesdirectos": "costes_directos",
    "cdircorporativos": "c_dir_corporativos",
    "cdirauxiliares": "c_dir_auxiliares",
    "celaboracionofertas": "c_elaboracion_ofertas",
    "disponibilidad": "disponibilidad",
    "actividadesid": "actividades_id",
    "desviaciontasa": "desviacion_tasa",
    "margendirecto": "margen_directo",
    "indirectos": "indirectos",
    "costesindirectosdemercado": "costes_indirectos_mercado",
    "costesindirectosdeproduccion": "costes_indirectos_produccion",
    "costesindirectoscomerciales": "costes_indirectos_comerciales",
    "margencontribucion": "margen_contribucion",
    "estructura": "estructura",
    "indemnizaciones": "indemnizaciones",
    "funcionescorporativas": "funciones_corporativas",
    "restoestructura": "resto_estructura",
    "ebit": "ebit",
    "amortizacion": "amortizacion",
    "facturacion": "facturacion",
    "cobros": "cobros",
    "carteracomfinal": "cartera_com_final",
    "mgcarteracomfinal": "mg_cartera_com_final",
    "carterafinfinal": "cartera_fin_final",
    "deuda": "deuda",
    "dpf": "dpf",
    "alo": "alo",
    "existencias": "existencias",
}

# Columnas que se deben insertar (excluye id serial)
INSERT_COLS = [
    "fecha_carga",
    "periodo",
    "operacion_cdg",
    "id_externo",
    "proyecto_indicador",
    "proyecto_indicador2",
    "empresa",
    "empresa5",
    "mercado_global_hom",
    "mercado_indra_homg_ln",
    "mercado_indra_homg_norg",
    "mercado_indra_group",
    "nivel_org_2",
    "unidad_empresa_ano",
    "unidad_empresa_ano8",
    "cliente_contractual",
    "metacliente",
    "interlocutor",
    "intergrupo_rev",
    "intergrupo_rev_fyc",
    "intersegmento",
    "segmento_linea_negocio",
    "geografia_hom",
    "pais",
    "region_defensa",
    "aftermarket",
    "clase_proyecto",
    "tipo_proyecto_servicio_nivel_1",
    "tipo_proyecto_servicio_nivel_2",
    "tipo_proyecto_servicio_nivel_3",
    "solucion_nivel_2",
    "solucion_nivel_3",
    "gerente_vertical",
    "gestor_vertical",
    "fecha_inicio_proyecto",
    "fecha_fin_proyecto",
    "grandes_prog_def",
    "metodo_reconocimiento_ingreso7",
    "indicador_agrupacion",
    "mercado_indra",
    "auditable",
    "mision",
    "programa",
    "valores",
    "contratacion",
    "ventas",
    "margen_bruto",
    "costes_directos",
    "c_dir_corporativos",
    "c_dir_auxiliares",
    "c_elaboracion_ofertas",
    "disponibilidad",
    "actividades_id",
    "desviacion_tasa",
    "margen_directo",
    "indirectos",
    "costes_indirectos_mercado",
    "costes_indirectos_produccion",
    "costes_indirectos_comerciales",
    "margen_contribucion",
    "estructura",
    "indemnizaciones",
    "funciones_corporativas",
    "resto_estructura",
    "ebit",
    "amortizacion",
    "facturacion",
    "cobros",
    "cartera_com_final",
    "mg_cartera_com_final",
    "cartera_fin_final",
    "deuda",
    "dpf",
    "alo",
    "existencias",
    "fecha_creacion",
    "fecha_ult_modificacion",
    "creador",
    "ult_modificador",
    "deleted_row",
]

# Columnas con valores fijos (no vienen del Excel)
FIXED_COLS = [
    "fecha_carga",
    "fecha_creacion",
    "fecha_ult_modificacion",
    "creador",
    "ult_modificador",
    "deleted_row",
]

# Columnas esperadas desde el Excel
EXCEL_COLS = [col for col in INSERT_COLS if col not in FIXED_COLS]

# No se validan booleanos desde Excel porque deleted_row se fija en el proceso
BOOL_COLS: List[str] = []


@dataclass
class ValidationResult:
    valid_df: pd.DataFrame
    invalid_df: pd.DataFrame


def read_db_config(ini_path: Path, section: str = "postgres") -> Dict[str, str]:
    """Lee parámetros de conexión desde un .ini y devuelve un diccionario apto para psycopg2."""
    cfg = configparser.ConfigParser()
    if not ini_path.exists():
        raise FileNotFoundError(f"No existe el fichero .ini: {ini_path}")
    cfg.read(ini_path)
    if section not in cfg:
        raise KeyError(f"No existe la sección [{section}] en {ini_path}")
    return dict(cfg[section])


def pick_excel_file(input_dir: Path, file_name: str | None = None) -> Path:
    """Selecciona el Excel a procesar; por defecto, el más reciente de la carpeta."""
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


def canonicalize_header(header: str) -> str:
    """Normaliza una cabecera para hacer match robusto con aliases de Excel."""
    txt = str(header).strip().lower()
    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    txt = re.sub(r"[^a-z0-9]+", "", txt)
    return txt


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza cabeceras y las mapea al naming de base de datos."""
    df = df.copy()

    mapped_cols = []
    used = set()
    for raw_col in df.columns:
        key = canonicalize_header(raw_col)
        mapped = HEADER_ALIASES.get(key, key)

        # Evita nombres duplicados tras mapear; conserva el original canonicalizado como fallback
        if mapped in used:
            mapped = f"{mapped}__dup"
        used.add(mapped)
        mapped_cols.append(mapped)

    df.columns = mapped_cols
    return df


def clean_text_values(df: pd.DataFrame) -> pd.DataFrame:
    """Limpieza básica de texto: trim y vacíos a nulo."""
    df = df.copy()
    object_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in object_cols:
        df[col] = df[col].astype("string").str.strip()
        df[col] = df[col].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    return df


def drop_non_data_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina filas completamente vacías y filas de resumen al final del Excel."""
    df = df.copy()

    # Quita filas donde todas las columnas de entrada estén vacías
    non_empty_mask = df[EXCEL_COLS].notna().any(axis=1)
    df = df.loc[non_empty_mask].copy()

    # Quita filas de resumen típicas: 'col/columna', 'datos', 'nulos'
    if {"periodo", "operacion_cdg", "id_externo"}.issubset(df.columns):
        p = df["periodo"].astype("string").str.strip().str.lower()
        o = df["operacion_cdg"].astype("string").str.strip().str.lower()
        i = df["id_externo"].astype("string").str.strip().str.lower()

        summary_mask = p.isin(["col", "columna", "column"]) & (
            o.str.contains("dato", na=False) | o.str.contains("cantidad", na=False)
        ) & (
            i.str.contains("nulo", na=False) | i.str.contains("vacio", na=False) | i.str.contains("vacío", na=False)
        )

        df = df.loc[~summary_mask].copy()

    return df


def validate_and_transform(df_raw: pd.DataFrame) -> ValidationResult:
    """Valida campos por tipo y separa registros válidos e inválidos con detalle de errores."""
    df = df_raw.copy()

    # Garantiza presencia de columnas esperadas desde Excel (faltantes se añaden como nulas)
    for col in EXCEL_COLS:
        if col not in df.columns:
            df[col] = pd.NA

    # Ignorar columnas extra del Excel y trabajar solo con columnas de entrada esperadas
    df = df[EXCEL_COLS].copy()

    # Completa columnas fijas para mantener el mismo esquema interno de INSERT_COLS
    for col in FIXED_COLS:
        if col not in df.columns:
            df[col] = pd.NA

    errors: List[pd.Series] = []

    # Validación/transformación de fechas
    for col in DATE_COLS:
        original = df[col]
        parsed = pd.to_datetime(original, errors="coerce", dayfirst=True)

        # Error de parseo: formato inválido (ej. mes 54, día 99, texto no fecha)
        bad_parse = original.notna() & (original.astype(str).str.strip() != "") & parsed.isna()

        # Error de rango de año: evita fechas técnicamente parseables pero no válidas para negocio (ej. año 0123)
        bad_year_range = parsed.notna() & ((parsed.dt.year < MIN_YEAR) | (parsed.dt.year > MAX_YEAR))

        bad = bad_parse | bad_year_range
        errors.append(bad.rename(f"error_{col}"))

        # Si no pasa validación, se deja nulo para no insertar valor incorrecto
        parsed = parsed.where(~bad, pd.NaT)
        df[col] = parsed.dt.date

    # Validación/transformación de numéricos
    for col in NUMERIC_COLS:
        original = df[col]
        if original.dtype == "object":
            original = original.astype("string").str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
        parsed = pd.to_numeric(original, errors="coerce")
        bad = pd.Series(False, index=df.index)
        src = df[col]
        bad = src.notna() & (src.astype(str).str.strip() != "") & parsed.isna()
        errors.append(bad.rename(f"error_{col}"))
        df[col] = parsed

    # Booleanos
    for col in BOOL_COLS:
        normalized = (
            df[col]
            .astype("string")
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
        bad = normalized.notna() & ~normalized.isin([True, False])
        errors.append(bad.rename(f"error_{col}"))
        df[col] = normalized.where(normalized.isin([True, False]), pd.NA)

    # Validación cruzada: fecha_inicio_proyecto no puede ser posterior a fecha_fin_proyecto
    if "fecha_inicio_proyecto" in df.columns and "fecha_fin_proyecto" in df.columns:
        start = pd.to_datetime(df["fecha_inicio_proyecto"], errors="coerce")
        end = pd.to_datetime(df["fecha_fin_proyecto"], errors="coerce")
        bad_date_order = start.notna() & end.notna() & (start > end)
        errors.append(bad_date_order.rename("error_rango_fechas_proyecto"))

    # deleted_row: default false
    if "deleted_row" in df.columns:
        df["deleted_row"] = df["deleted_row"].fillna(False).astype(bool)

    # Matriz de errores
    error_df = pd.concat(errors, axis=1)
    row_has_error = error_df.any(axis=1)

    invalid_df = df_raw.loc[row_has_error].copy()
    invalid_df["errores"] = error_df.loc[row_has_error].apply(
        lambda row: ", ".join([c.replace("error_", "") for c, v in row.items() if bool(v)]),
        axis=1,
    )

    valid_df = df.loc[~row_has_error].copy()
    return ValidationResult(valid_df=valid_df, invalid_df=invalid_df)


def apply_fixed_audit_values(df: pd.DataFrame) -> pd.DataFrame:
    """Asigna valores fijos para columnas que no llegan desde Excel."""
    df = df.copy()
    now_ts = datetime.now()
    today = now_ts.date()
    df["fecha_carga"] = now_ts
    df["fecha_creacion"] = today
    df["fecha_ult_modificacion"] = today
    df["creador"] = MASSIVE_IMPORT_USER
    df["ult_modificador"] = MASSIVE_IMPORT_USER
    df["deleted_row"] = False
    return df


def to_db_value(value):
    """Convierte valores de pandas/numpy a tipos nativos seguros para psycopg2."""
    if pd.isna(value):
        return None

    # Convierte escalares numpy (np.float64, np.int64, etc.) a tipos Python puros
    if hasattr(value, "item") and callable(getattr(value, "item")):
        try:
            value = value.item()
        except Exception:
            pass

    # Limpia textos con patrón literal de numpy (ej: 'np.float64(12.3)')
    if isinstance(value, str):
        txt = value.strip()
        m_float = re.fullmatch(r"np\.float64\(([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)\)", txt)
        m_int = re.fullmatch(r"np\.int64\(([-+]?\d+)\)", txt)
        if m_float:
            return float(m_float.group(1))
        if m_int:
            return int(m_int.group(1))
        return txt

    # Evita NaN/Inf en float nativo
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None

    # Timestamp pandas a datetime nativo
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    return value


def insert_valid_rows(
    df: pd.DataFrame,
    conn_params: Dict[str, str],
    batch_size: int = 1000,
    progress_every: int = 10_000,
) -> int:
    """Inserta en bloque los registros válidos en PostgreSQL y devuelve cuántos se insertaron."""
    if df.empty:
        return 0

    rows = [tuple(to_db_value(v) for v in row) for row in df[INSERT_COLS].itertuples(index=False, name=None)]
    total = len(rows)

    sql = f"""
        INSERT INTO {SCHEMA}.{TABLE} ({', '.join(INSERT_COLS)})
        VALUES %s
    """

    inserted = 0
    next_progress_mark = progress_every

    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            for start in range(0, total, batch_size):
                end = min(start + batch_size, total)
                chunk = rows[start:end]
                execute_values(cur, sql, chunk, page_size=batch_size)
                inserted = end

                # Reporte de avance cada N registros
                while inserted >= next_progress_mark:
                    print(f"[PROGRESO] Insertados {next_progress_mark} de {total} registros")
                    next_progress_mark += progress_every

        conn.commit()

    if inserted % progress_every != 0:
        print(f"[PROGRESO] Insertados {inserted} de {total} registros")

    return inserted


def export_invalid(invalid_df: pd.DataFrame, output_dir: Path) -> Path | None:
    """Exporta los registros inválidos a Excel para su revisión."""
    if invalid_df.empty:
        return None
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = output_dir / f"registros_invalidos_{ts}.xlsx"
    invalid_df.to_excel(out, index=False)
    return out


def main() -> None:
    """Punto de entrada CLI del proceso de ingesta y validación."""
    parser = argparse.ArgumentParser(description="Carga y validación de Excel a PostgreSQL")
    parser.add_argument("--input-dir", required=True, help="Carpeta donde está el Excel")
    parser.add_argument("--file-name", required=False, help="Nombre del Excel (opcional)")
    parser.add_argument("--sheet-name", default="bbdd", help="Nombre de hoja Excel a procesar")
    parser.add_argument("--ini-path", required=True, help="Ruta al fichero .ini de conexión")
    parser.add_argument("--ini-section", default="postgres", help="Sección del .ini")
    parser.add_argument("--output-dir", default="./salidas", help="Carpeta para exportar inválidos")
    parser.add_argument("--batch-size", type=int, default=1000, help="Tamaño de lote para inserción en BD")
    parser.add_argument(
        "--progress-every",
        type=int,
        default=10000,
        help="Mostrar progreso cada N registros insertados",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    ini_path = Path(args.ini_path)
    output_dir = Path(args.output_dir)

    if not input_dir.exists() or not input_dir.is_dir():
        raise NotADirectoryError(f"Carpeta de entrada inválida: {input_dir}")

    excel_file = pick_excel_file(input_dir, args.file_name)
    conn_params = read_db_config(ini_path, args.ini_section)

    print(f"[INFO] Leyendo Excel: {excel_file} (hoja: {args.sheet_name})")
    df_raw = pd.read_excel(excel_file, sheet_name=args.sheet_name)
    df_raw = normalize_headers(df_raw)
    df_raw = clean_text_values(df_raw)
    df_raw = drop_non_data_rows(df_raw)

    result = validate_and_transform(df_raw)

    # Las columnas de auditoría no vienen en Excel: se rellenan con valores fijos para inserción
    valid_df = apply_fixed_audit_values(result.valid_df)

    invalid_path = export_invalid(result.invalid_df, output_dir)
    inserted = insert_valid_rows(
        valid_df,
        conn_params,
        batch_size=args.batch_size,
        progress_every=args.progress_every,
    )

    print("[RESUMEN]")
    print(f"- Filas leídas: {len(df_raw)}")
    print(f"- Filas válidas insertadas: {inserted}")
    print(f"- Filas inválidas: {len(result.invalid_df)}")
    if invalid_path:
        print(f"- Reporte inválidos: {invalid_path}")


if __name__ == "__main__":
    main()
