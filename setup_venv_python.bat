@echo off
setlocal

REM Ir a la carpeta donde está este .bat
cd /d "%~dp0"

echo [1/6] Creando entorno virtual (.venv)...
python -m venv .venv
if errorlevel 1 (
  echo ERROR creando el entorno virtual.
  pause
  exit /b 1
)

echo [2/6] Activando entorno...
call ".venv\Scripts\activate"

echo [3/6] Actualizando pip...
python -m pip install --upgrade pip

echo [4/6] Instalando dependencias base...
pip install -r requirements.txt
if errorlevel 1 (
  echo ERROR instalando requirements.txt
  pause
  exit /b 1
)

echo [5/6] Instalar dependencias de desarrollo (Spyder)? [S/N]
set /p INSTALL_DEV=
if /I "%INSTALL_DEV%"=="S" (
  pip install -r requirements-dev.txt
  if errorlevel 1 (
    echo ERROR instalando requirements-dev.txt
    pause
    exit /b 1
  )
)

echo [6/6] Listo.
echo Interprete para Spyder:
echo %cd%\.venv\Scripts\python.exe
echo.
echo En Spyder: Tools ^> Preferences ^> Python interpreter ^> Use the following interpreter
echo.
pause