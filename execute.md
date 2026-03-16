# Guía de ejecución - `cvg_massive_excels.py`

## 1) Preparación inicial

1. Crear/activar entorno virtual e instalar dependencias:

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/Mac
source .venv/bin/activate

pip install -r requirements.txt
```

2. Crear `config.ini` a partir de `config.example.ini`.

3. Revisar especialmente en `config.ini`:
- `[postgres]` credenciales
- `[target]` destino estándar
- `[target_defensa]` destino defensa
- `[input]` carpeta/archivo/hoja Excel
- `[output]` carpeta de salidas y `mapping_file`
- `[fixed_values]` valores automáticos

---

## 2) Ejecución normal (recomendada)

```bash
python cvg_massive_excels.py
```

Flujo:
1. Pregunta si el import es para Defensa.
2. Pregunta si es primer insert o reintento (elige `input_dir` o `retry_input_dir`).
3. Resuelve hoja a cargar:
   - si existe `sheet_name`, la usa;
   - si hay una sola hoja, la usa automáticamente;
   - si hay varias y no coincide, pregunta cuál usar.
4. Propone homologación de columnas Excel -> tabla.
5. Exporta homologación a Excel y guarda/actualiza `mapping.ini`.
6. Pide confirmación (`si/no/recargar`).
7. Si confirmas, valida e inserta en BD.

---

## 3) Solo homologación (sin insertar)

```bash
python cvg_massive_excels.py --only-mapping
```

Útil para revisar el mapeo antes de cargar datos.

---

## 4) Aprobación automática del mapping (sin prompt)

```bash
python cvg_massive_excels.py --auto-approve-mapping
```

Útil para ejecución automática cuando el `mapping.ini` ya está consolidado.

---

## 5) Selección manual de schema/tabla desde consola

```bash
python cvg_massive_excels.py --interactive-target
```

Ignora `[target]` y `[target_defensa]`, y deja elegir:
- schema disponible
- tabla disponible dentro del schema

---

## 6) Especificar ruta de config.ini

```bash
python cvg_massive_excels.py --config-path "C:/ruta/config.ini"
```

Combinable con otras opciones (`--only-mapping`, `--interactive-target`, etc.).

---

## 7) Modos combinados útiles

### Revisar mapping de un destino elegido en BD (sin insertar)

```bash
python cvg_massive_excels.py --interactive-target --only-mapping
```

### Carga automática sin confirmación manual

```bash
python cvg_massive_excels.py --auto-approve-mapping
```

---

## 8) Archivos generados

- `salidas/mapping_review_<schema>_<table>_<timestamp>.xlsx`
- `salidas/registros_invalidos_<timestamp>.xlsx` (si existen inválidos)
- `mapping.ini` (persistente por tabla)
- `inputs_retry/registros_invalidos_<timestamp>.xlsx` (copia para corrección y recarga puntual)
- `retry_index.json` (relación entre parciales y reintentos para cierre automático)
- Excel original marcado como procesado cuando hubo inserción:
  - `..._OK` si no hubo inválidos
  - `..._PARTIAL_ERROR` si hubo inválidos
  - `processed_mode=move` -> mueve a `excels_done/`
  - `processed_mode=rename` -> renombra en origen
- Si el **reintento** termina 100% OK:
  - se elimina el excel de `inputs_retry`
  - se elimina el reporte de inválidos previo en `salidas`
  - el archivo original `..._PARTIAL_ERROR` pasa a `..._OK`

---

## 9) Respuestas de confirmación de mapeo

Cuando el script pregunta:

`¿El mapeo es correcto? [si/no/recargar]:`

- `si` -> continúa
- `no` -> detiene la carga
- `recargar` -> vuelve a leer `mapping.ini` y reproponer

---

## 11) Manejo de errores (mensajes guiados)

El script captura errores comunes y muestra acción sugerida:

- Archivo no encontrado -> revisar rutas de `config.ini`.
- Carpeta de entrada inválida -> crear carpeta o corregir `input_dir`.
- Hoja inexistente -> indica hojas disponibles y corregir `sheet_name`.
- Tabla/schema no encontrados -> revisar `[target]`/`[target_defensa]`, credenciales y permisos.
- Error de conexión PostgreSQL -> revisar host/puerto/dbname/usuario/password/VPN.

Siempre que sea posible, corrige `config.ini` y vuelve a ejecutar.
