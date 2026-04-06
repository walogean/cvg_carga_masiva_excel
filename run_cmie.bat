@echo off
setlocal

REM Ir a la carpeta donde está este .bat
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo No existe entorno virtual en .venv
  echo Ejecuta primero setup_env.bat
  pause
  exit /b 1
)

call ".venv\Scripts\activate"

if not exist "config.ini" (
  echo No existe config.ini en esta carpeta.
  echo Crea uno copiando config.example.ini
  pause
  exit /b 1
)

echo ¿Modo solo homologacion (sin insertar)? [S/N]
set /p ONLYMAP=

if /I "%ONLYMAP%"=="S" (
  python cvg_massive_excels.py --only-mapping
) else (
  python cvg_massive_excels.py
)

echo.
pause