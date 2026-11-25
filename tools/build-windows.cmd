@echo off
rem ------------------------------------------------------------------
rem Loudnorm PRO v2 - Windows-Build (portabler Ordner, ohne installierte .NET-Runtime lauffaehig)
rem Ergebnis: publish\windows\LoudnormPRO.exe + portable.mode + DLLs + Languages\
rem ------------------------------------------------------------------
cd /d "%~dp0.."
dotnet publish src\LoudnormPro.App\LoudnormPro.App.csproj -c Release -r win-x64 --self-contained true ^
  -p:PublishSingleFile=true -p:DebugType=none -o publish\windows
if errorlevel 1 (
  echo.
  echo BUILD FEHLGESCHLAGEN
  pause
  exit /b 1
)
echo.
echo Fertig: publish\windows\LoudnormPRO.exe
pause
