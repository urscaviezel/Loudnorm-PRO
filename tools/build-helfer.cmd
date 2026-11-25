@echo off
setlocal
title Loudnorm PRO v2 - Build-Helfer
cd /d "%~dp0.."
if not exist tools\req mkdir tools\req
if not exist tools\out mkdir tools\out
echo ============================================================
echo  Loudnorm PRO v2 - Build-Helfer
echo  Fuehrt NUR feste Befehle aus: build, test, run, publish, installer.
echo  Fenster offen lassen. Beenden mit Ctrl+C oder Fenster schliessen.
echo ============================================================
(
  echo === dotnet --info ===
  dotnet --info
  echo === where ===
  where dotnet
  where ffmpeg
  where ffprobe
) > tools\out\env.log 2>&1
echo [%time%] Umgebung geprueft (tools\out\env.log)
:loop
if exist tools\req\build.req (
  del /q tools\req\build.req
  echo [%time%] Build ...
  dotnet build LoudnormPro.sln -c Debug -v minimal -clp:NoSummary > tools\out\build.log 2>&1
  call :done build
)
if exist tools\req\test.req (
  del /q tools\req\test.req
  echo [%time%] Tests ...
  dotnet test tests\LoudnormPro.Core.Tests\LoudnormPro.Core.Tests.csproj -c Debug -v minimal > tools\out\test.log 2>&1
  call :done test
)
if exist tools\req\run.req (
  del /q tools\req\run.req
  echo [%time%] Starte App ...
  start "Loudnorm PRO v2" /D "%cd%" dotnet run --project src\LoudnormPro.App\LoudnormPro.App.csproj -c Debug --no-build
  >tools\out\run.exit echo started
)
if exist tools\req\publish.req (
  del /q tools\req\publish.req
  echo [%time%] Publish win-x64 ...
  dotnet publish src\LoudnormPro.App\LoudnormPro.App.csproj -c Release -r win-x64 --self-contained true -p:PublishSingleFile=true -p:DebugType=none -o publish\windows > tools\out\publish.log 2>&1
  call :done publish
)
if exist tools\req\installer.req (
  del /q tools\req\installer.req
  echo [%time%] Installer ...
  call tools\build-installer.cmd nopause > tools\out\installer.log 2>&1
  call :done installer
)
ping -n 3 127.0.0.1 >nul
goto loop

:done
>tools\out\%1.exit echo %errorlevel%
echo [%time%] %1 fertig (Exitcode %errorlevel%)
exit /b
