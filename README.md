# cvg

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

## Salidas

- Inserción de registros válidos en PostgreSQL.
- Exportación de inválidos a Excel con columna `errores` en la carpeta de salida.
