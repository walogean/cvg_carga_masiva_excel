# Bitácora de actualizaciones (cvg)

## Contexto funcional
Proyecto de carga masiva Excel -> PostgreSQL con dos scripts históricos:
- `carga_converge.py`: versión específica original (Defensa).
- `cvg_massive_excels.py`: versión genérica y reutilizable por metadata de tabla.

El objetivo de las últimas iteraciones fue estabilizar flujo de carga inicial/reintentos, limpieza de archivos, trazabilidad de errores y **flexibilizar la lectura de Excel para distintos formatos reales**.

---

## Último estado acordado con negocio/usuario
- Mantener un flujo limpio y recursivo
- UX simplificada
- Logging y automatización
- Soporte de encabezados dinámicos
- Persistencia inteligente de configuraciones

---

## Cambios aplicados recientemente (esta sesión)

### 16) Persistencia de header en mapping.ini (NUEVO 🔥)

Se añade capacidad de guardar el header detectado por tabla directamente en `mapping.ini`:

```ini
[schema.table.__meta__]
header_row = 3
```

#### Comportamiento:
- Se guarda automáticamente después de detectar o seleccionar header.
- Se reutiliza en futuras ejecuciones.
- No rompe ejecución si no existe.

#### Beneficio:
- Evita redetectar siempre
- Hace el proceso más estable y predecible

---

### 17) Validación rápida del header persistido

Antes de usar el header guardado:
- Se valida con scoring contra columnas reales.
- Si el score es bajo → se ignora y se detecta nuevamente.

#### Regla:
- Score mínimo aceptable: `>= 10`

---

### 18) Early stop en detección de header (optimización ⚡)

Se introduce corte anticipado en detección automática:

```python
EARLY_STOP_SCORE = 200
```

#### Comportamiento:
- Si una fila supera ese score:
  - se detiene el escaneo
  - se selecciona inmediatamente

#### Beneficio:
- Mejora rendimiento significativamente
- Reduce ruido en Excel grandes

---

### 19) Limpieza de consola (UX)

Se reduce el output en consola:

ANTES:
- múltiples líneas por fila analizada

AHORA:
- solo se muestra:
```
[HEADER] Detección automática de fila de encabezados
[HEADER] Fila detectada como header: X
```

#### Nota:
- El detalle completo se mantiene en logs

---

### 20) Corrección crítica mapping.ini (error Unnamed)

#### Problema:
Columnas tipo `Unnamed:*` generaban duplicados en `configparser`:

```
option 'Unnamed' already exists
```

#### Solución:
Se filtran antes de guardar:

```python
if re.fullmatch(r"unnamed[:\s_0-9\-]*", raw.strip().lower()):
    continue
```

#### Resultado:
- Eliminado error crítico
- mapping.ini estable

---

### 21) Mejora robustez general del header handling

Nuevo flujo de decisión:

1. Header manual (config / usuario)
2. Header guardado en mapping.ini
3. Detección automática
4. Prompt (solo si ambigüedad)

#### Principio clave:
> El sistema intenta resolver solo antes de preguntar

---

## Cambios previos (se mantienen vigentes)

### 6) Prompts abreviados y más claros
- `[s/n]`
- `[s/n/r]`

---

### 7) Comportamiento en reintentos (retry)
- Eliminación automática de archivos retry
- Limpieza de artefactos
- Promoción de estado

---

### 8) Columna errores en Excel fuente
- Escritura alineada al header real

---

### 9) Evitar insertar columna errores
- Eliminación previa a insert

---

### 10) Columnas faltantes vs tabla destino
- Uso de DEFAULT
- Confirmación controlada

---

### 11) Logging a archivo
- `--log-file`
- duplicación stdout/stderr

---

### 12) Modo no interactivo completo
- ejecución automatizable

---

### 13) Pruebas mínimas integradas
- validación básica

---

### 14) Acciones previas por tabla
- `TRUNCATE` u otras

---

### 15) Selección de tabla Defensa
- uso de selector dinámico

---

## Consideraciones operativas

1. Detección automática puede fallar en layouts muy atípicos
2. mapping.ini ahora influye en comportamiento (persistencia)
3. early stop acelera pero depende de scoring correcto
4. sistema es ahora híbrido: automático + validado
5. mantener backup de mapping.ini recomendado

---

## Estado actual del sistema

✔ Robusto  
✔ Flexible  
✔ Optimizado  
✔ Menos dependiente del usuario  
✔ Más inteligente en decisiones  

---

## Comandos de uso

### Carga inicial

```bash
python3 cvg_massive_excels.py \
  --non-interactive \
  --target-section target_defensa \
  --yes-missing-columns \
  --log-file run.log
```

### Reintento

```bash
python3 cvg_massive_excels.py \
  --non-interactive \
  --target-section target_defensa \
  --load-mode retry \
  --yes-missing-columns \
  --log-file run_retry.log
```

---

## Nota final

Esta versión introduce inteligencia operativa en el tratamiento de headers y elimina uno de los principales puntos de fricción en cargas masivas reales.

Fecha actualización: 2026-03-18 10:00:07