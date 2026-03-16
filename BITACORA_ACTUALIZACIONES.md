# Bitácora de actualizaciones (cvg)

## Contexto funcional
Proyecto de carga masiva Excel -> PostgreSQL con dos scripts históricos:
- `carga_converge.py`: versión específica original (Defensa).
- `cvg_massive_excels.py`: versión genérica y reutilizable por metadata de tabla.

El objetivo de las últimas iteraciones fue estabilizar flujo de carga inicial/reintentos, limpieza de archivos y trazabilidad de errores para operación diaria.

---

## Último estado acordado con negocio/usuario
- Mantener un flujo **limpio y recursivo**:
  - Carga inicial inserta válidos y deriva inválidos a reintento.
  - Reintento no ensucia `excels_done`; consume y elimina su input de retry.
- Mejorar UX de consola con respuestas cortas (`s/n`, `s/n/r`).
- Asegurar cierre de handles de Excel para evitar bloqueo de archivos.
- Garantizar valores para columnas faltantes con `DEFAULT` de tabla cuando aplique.
- Registrar errores por fila también en el Excel fuente (columna `errores`).
- Añadir modo de operación más automatizable (non-interactive) y logging a archivo.

---

## Cambios aplicados recientemente (esta sesión)

### 1) Prompts abreviados y más claros
**Archivo:** `cvg_massive_excels.py`
- `ask_yes_no(...)` ahora usa:
  - prompt: `[s/n] (s=si, n=no)`
  - validación de entrada acorde.
- `confirm_mapping(...)` ahora usa:
  - prompt: `[s/n/r] (s=si, n=no, r=recargar)`
  - validación de entrada acorde.

### 2) Comportamiento en reintentos (retry)
**Archivo:** `cvg_massive_excels.py`
- Si la ejecución es modo reintento (`retry_input_dir`) y hubo inserciones (`inserted > 0`):
  - el Excel consumido de `inputs_retry` se **elimina**.
  - **no** se mueve a `excels_done`.
- Se mantiene cierre automático del ciclo cuando el retry queda 100% OK:
  - limpieza de artefactos asociados (según `retry_index.json`).
  - promoción de original `..._PARTIAL_ERROR` a `..._OK` si existe vínculo en índice.

### 3) Columna `errores` en Excel fuente
**Archivo:** `cvg_massive_excels.py`
- Se añadió anotación en el Excel original cargado:
  - crea/reutiliza columna `errores` en hoja procesada.
  - escribe por fila todos los errores de validación concatenados.
- Si el archivo no es `.xlsx` (ej. `.xls`), se informa y omite anotación.

### 4) Evitar insertar columna de control `errores`
**Archivo:** `cvg_massive_excels.py`
- Al leer Excel, se eliminan columnas de control/no insertables (actualmente `errores`) para evitar que interfieran en homologación/inserción, especialmente en retries.

### 5) Columnas faltantes en Excel vs tabla destino
**Archivo:** `cvg_massive_excels.py`
Se incorporó flujo explícito para columnas insertables que existen en tabla pero no vienen en Excel:
- Detección de faltantes (`collect_missing_input_columns`).
- Parseo de defaults SQL literales (`parse_column_default_literal`) para casos típicos:
  - booleanos (`true/false`), números, texto literal con comillas.
- Prompt de confirmación al usuario:
  - muestra por columna si irá con DEFAULT literal o NULL.
  - `[s/n]` para continuar o detener y editar Excel.
- Aplicación del plan antes de insertar (`apply_missing_columns_plan`):
  - si hay default literal interpretable -> usa ese valor.
  - si no -> envía `NULL`.

> Motivo principal: evitar que columnas como `ischecked` y `nps_pregunta` queden en `NULL` cuando en tabla tienen default (`false`/`true`).

---

## Consideraciones operativas
1. Si una carga parcial fue hecha con versión antigua, puede faltar correlación en `retry_index.json`; en ese caso la promoción automática a `_OK` puede requerir ajuste manual o crear entrada de índice.
2. Con `processed_mode=move`, el archivo se mueve a `excels_done` sin cambiar necesariamente nombre; el control de estado puede depender del nombre heredado/manual y del índice.
3. Se validó sintaxis Python tras cambios con `python3 -m py_compile cvg_massive_excels.py`.

---

## Cambios adicionales (modo PRO)

### 6) Logging a archivo
**Archivo:** `cvg_massive_excels.py`
- Nuevo flag: `--log-file <ruta>`
- Duplica `stdout/stderr` a archivo (formato simple con marca de inicio de ejecución).

### 7) Modo no interactivo completo
**Archivo:** `cvg_massive_excels.py`
- Nuevo flag: `--non-interactive`
  - auto-aprueba homologación.
  - auto-acepta columnas faltantes (DEFAULT/NULL).
  - evita prompt de defensa cuando aplica (usa `target` por defecto).
- Nuevo flag: `--target-section {target,target_defensa}` para fijar destino sin prompt.
- Nuevo flag: `--load-mode {initial,retry}` para fijar carpeta de entrada sin prompt.
- Nuevo flag: `--yes-missing-columns` para aceptar sólo la parte de faltantes sin activar todo non-interactive.

### 8) Pruebas mínimas integradas (smoke tests)
**Archivo:** `cvg_massive_excels.py`
- Nuevo flag: `--run-tests`
- Ejecuta asserts de funciones críticas:
  - parseo de defaults SQL literales
  - parseo de periodos (`ene-24`, `dic/2025`)
  - parseo numérico (miles/decimales y literales `np.float64(...)`)
  - parseo booleano básico

---

## Próximos ajustes sugeridos (opcionales)
- Ajustar `mark_excel_as_processed` en modo `move` para mantener sufijo de estado (`_PARTIAL_ERROR`/`_OK`) también en nombre del archivo movido.
- Añadir changelog resumido en `README.md` para visibilidad rápida.
