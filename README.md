# cvg

## Nueva versión genérica (escalable): `cvg_massive_excels.py`

Se añadió una versión nueva y reutilizable para cualquier tabla destino de PostgreSQL, usando `config.ini` en la misma carpeta del script.

### Características

- Lee metadata real de la tabla (`information_schema.columns`) y valida por tipo.
- Homologa columnas Excel -> tabla y **muestra la propuesta en consola**.
- Exporta propuesta de homologación a Excel y la guarda en `mapping.ini` reutilizable por tabla.
- Pide confirmación interactiva: `si / no / recargar` antes de insertar.
- Permite mapeos manuales extra en sección `[column_map]` de `config.ini`.
- Soporta valores fijos configurables para columnas no presentes en Excel (`[fixed_values]`).
- Exporta inválidos y realiza carga masiva por lotes con progreso.

### Archivos nuevos

- `cvg_massive_excels.py`: versión nueva genérica.
- `config.example.ini`: ejemplo de configuración.

### Ejecución (nueva versión)

1. Copia `config.example.ini` a `config.ini` y rellena tus datos.
2. Ejecuta:

```bash
python3 cvg_massive_excels.py
```

En modo interactivo, el script mostrará la homologación propuesta y esperará:
- `si` -> continúa y carga.
- `no` -> detiene la carga.
- `recargar` -> vuelve a leer `mapping.ini` (útil tras editarlo).

Opcional, especificando ruta de config:

```bash
python3 cvg_massive_excels.py --config-path "/ruta/config.ini"
```

Opcional sin interacción (aprobación automática):

```bash
python3 cvg_massive_excels.py --auto-approve-mapping
```

---

Proceso en Python para cargar datos desde Excel a la tabla:

`proyecto_dashboard_defensa.converge_proyectos_financieros`

## Qué hace

1. Toma un Excel de una carpeta (último por fecha o por nombre).
2. Carga el archivo en un DataFrame de pandas.
3. Limpia datos básicos (cabeceras, espacios, vacíos).
   - Incluye mapeo automático de nombres de columna del Excel (con/sin acentos, espacios o símbolos)
     al naming de base de datos (por ejemplo: `Operación CdG` -> `operacion_cdg`, `ID` -> `id_externo`).
4. Valida tipos según la tabla:
   - solo sobre columnas que vienen en Excel (`EXCEL_COLS`)
   - fechas (`date`) con control de parseo y rango de año (1900-2100)
   - validación cruzada: `fecha_inicio_proyecto` <= `fecha_fin_proyecto`
   - numéricos (`numeric`)
5. Separa datos inválidos para reporte.
6. Inserta los válidos en PostgreSQL.
7. Asigna valores fijos en columnas no presentes en Excel:
   - `fecha_carga` = timestamp actual (`datetime.now()`)
   - `fecha_creacion` = fecha actual
   - `fecha_ult_modificacion` = fecha actual
   - `creador` = `Massive Import`
   - `ult_modificador` = `Massive Import`
   - `deleted_row` = `False`

## Estructura

- `carga_converge.py`: script principal de ingesta/validación/carga.

## Requisitos

- Python 3.9+
- Dependencias:
  - `pandas`
  - `openpyxl`
  - `psycopg2-binary`

Instalación rápida:

```bash
pip install pandas openpyxl psycopg2-binary
```

## Configuración de BD (.ini)

Ejemplo de fichero `db.ini` (también disponible como `db.example.ini` en el repo):

```ini
[postgres]
host=localhost
port=5432
dbname=mi_base
user=mi_usuario
password=mi_password
```

## Uso

```bash
python3 carga_converge.py \
  --input-dir "/ruta/carpeta_excels" \
  --ini-path "/ruta/db.ini" \
  --ini-section "postgres" \
  --sheet-name "bbdd" \
  --output-dir "./salidas"
```

Opcionalmente, para un fichero concreto:

```bash
--file-name "archivo.xlsx"
```

Opciones útiles:

```bash
--batch-size 1000
--progress-every 10000
```

- `--batch-size`: tamaño del lote de inserción en PostgreSQL.
- `--progress-every`: muestra avance de inserción cada N registros (por ejemplo, 10.000 de X).
- `--sheet-name`: hoja de Excel a procesar (por defecto `bbdd`).

## Salidas

- Inserción de registros válidos en PostgreSQL.
- Exportación de inválidos a Excel con columna `errores` en la carpeta de salida.
