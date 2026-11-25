@echo off
rem ------------------------------------------------------------------
rem Loudnorm PRO v2 - Windows-Installer bauen
rem   1. Einzeldatei-EXE (src\LoudnormPro.App\obj\installer-app)
rem   2. Inno Setup -> publish\installer\LoudnormPRO-Setup-<version>.exe
rem Parameter "nopause": ohne Pause am Ende (fuer den Build-Helfer)
rem ------------------------------------------------------------------
setlocal
cd /d "%~dp0.."

set "VERSION="
for /f "tokens=3 delims=<>" %%v in ('findstr /c:"<Version>" Directory.Build.props') do set "VERSION=%%v"
if "%VERSION%"=="" set "VERSION=2.0.0"

set "ISCC=%ProgramFiles(x86)%\Inno Setup 6\ISCC.exe"
if not exist "%ISCC%" set "ISCC=%ProgramFiles%\Inno Setup 6\ISCC.exe"
if not exist "%ISCC%" (
  echo Inno Setup 6 wurde nicht gefunden: https://jrsoftware.org/isdl.php
  set "RC=1"
  goto :end
)

echo [1/2] Publish win-x64 (Version %VERSION%) ...
dotnet publish src\LoudnormPro.App\LoudnormPro.App.csproj -c Release -r win-x64 --self-contained true ^
  -p:PublishSingleFile=true -p:WindowsInstallerBuild=true -p:DebugType=none -o src\LoudnormPro.App\obj\installer-app
if errorlevel 1 (
  set "RC=1"
  goto :end
)

echo [2/2] Installer ...
"%ISCC%" /Q /DAppVersion=%VERSION% installer\LoudnormPro.iss
if errorlevel 1 (
  set "RC=1"
  goto :end
)
set "RC=0"
echo.
echo Fertig: publish\installer\LoudnormPRO-Setup-%VERSION%.exe

:end
if /i not "%~1"=="nopause" pause
exit /b %RC%
