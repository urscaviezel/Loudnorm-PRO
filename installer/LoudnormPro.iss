; ------------------------------------------------------------------
; Loudnorm PRO 2 - Windows-Installer (Inno Setup 6)
;
; - Sprache des Installers = Systemsprache (DE / FR), sonst Englisch
; - Deinstallation fragt, ob die Einstellungen behalten werden sollen
;
; Bauen: tools\build-installer.cmd
; ------------------------------------------------------------------

#ifndef AppVersion
  #define AppVersion "2.0.0"
#endif
#define AppName "Loudnorm PRO"
#define AppExe "LoudnormPRO.exe"
#define AppPublisher "Loudnorm PRO"
#define AppUrl "https://github.com/urscaviezel/Loudnorm-PRO"
#define SourceDir "..\src\LoudnormPro.App\obj\installer-app"

[Setup]
AppId={{6A1D2B10-7C4E-4F5B-9A2E-3C5D8E1F2000}
AppName={#AppName}
AppVersion={#AppVersion}
AppVerName={#AppName} {#AppVersion}
AppPublisher={#AppPublisher}
AppPublisherURL={#AppUrl}
AppSupportURL={#AppUrl}/issues
AppUpdatesURL={#AppUrl}/releases
DefaultDirName={autopf}\{#AppName} 2
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes
LicenseFile=..\LICENSE
OutputDir=..\publish\installer
OutputBaseFilename=LoudnormPRO-Setup-{#AppVersion}
SetupIconFile=..\src\LoudnormPro.App\Assets\loudnorm.ico
UninstallDisplayIcon={app}\{#AppExe}
UninstallDisplayName={#AppName} {#AppVersion}
Compression=lzma2/ultra64
SolidCompression=yes
WizardStyle=modern
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
PrivilegesRequired=admin
PrivilegesRequiredOverridesAllowed=dialog
CloseApplications=yes
RestartApplications=no
; Sprache automatisch aus der Windows-Oberflaechensprache; Dialog nur, wenn keine passt
LanguageDetectionMethod=uilanguage
ShowLanguageDialog=auto
VersionInfoVersion={#AppVersion}
VersionInfoProductName={#AppName}

[Languages]
; Erste Sprache = Fallback (Englisch)
Name: "english"; MessagesFile: "compiler:Default.isl"
Name: "german";  MessagesFile: "compiler:Languages\German.isl"
Name: "french";  MessagesFile: "compiler:Languages\French.isl"

[CustomMessages]
english.AppLanguage=en
german.AppLanguage=de
french.AppLanguage=fr
english.KeepSettings=Do you want to keep your Loudnorm PRO settings?%n%nSettings, profiles, custom language files and the resume history are stored in:%n%1%n%nYes = keep (recommended for reinstalling or updating)%nNo = delete everything
german.KeepSettings=Möchtest du deine Loudnorm-PRO-Einstellungen behalten?%n%nEinstellungen, Profile, eigene Sprachdateien und der Resume-Verlauf liegen in:%n%1%n%nJa = behalten (empfohlen bei Neuinstallation oder Update)%nNein = alles löschen
french.KeepSettings=Voulez-vous conserver vos réglages Loudnorm PRO ?%n%nLes réglages, profils, fichiers de langue personnalisés et l’historique de reprise se trouvent dans :%n%1%n%nOui = conserver (recommandé pour une réinstallation ou une mise à jour)%nNon = tout supprimer
english.FfmpegHint=FFmpeg was not found on this computer.%n%nLoudnorm PRO needs ffmpeg.exe and ffprobe.exe, e.g. in C:\ffmpeg\bin or in the PATH.%nDownload: https://ffmpeg.org/download.html
german.FfmpegHint=FFmpeg wurde auf diesem Computer nicht gefunden.%n%nLoudnorm PRO benötigt ffmpeg.exe und ffprobe.exe, z. B. in C:\ffmpeg\bin oder im PATH.%nDownload: https://ffmpeg.org/download.html
french.FfmpegHint=FFmpeg est introuvable sur cet ordinateur.%n%nLoudnorm PRO a besoin de ffmpeg.exe et ffprobe.exe, par ex. dans C:\ffmpeg\bin ou dans le PATH.%nTéléchargement : https://ffmpeg.org/download.html

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
Source: "{#SourceDir}\{#AppExe}"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#SourceDir}\*.dll"; DestDir: "{app}"; Flags: ignoreversion skipifsourcedoesntexist
Source: "{#SourceDir}\Languages\*.json"; DestDir: "{app}\Languages"; Flags: ignoreversion

[Icons]
Name: "{autoprograms}\{#AppName}"; Filename: "{app}\{#AppExe}"
Name: "{autodesktop}\{#AppName}"; Filename: "{app}\{#AppExe}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#AppExe}"; Description: "{cm:LaunchProgram,{#AppName}}"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
Type: files; Name: "{app}\setup-language.txt"
Type: dirifempty; Name: "{app}\Languages"
Type: dirifempty; Name: "{app}"

[Code]
function SettingsDir(): String;
begin
  Result := ExpandConstant('{localappdata}\Loudnorm PRO');
end;

function FfmpegAvailable(): Boolean;
var
  PathEnv: String;
begin
  Result := FileExists('C:\ffmpeg\bin\ffmpeg.exe') or FileExists('C:\ffmpeg\ffmpeg.exe');
  if not Result then
  begin
    PathEnv := GetEnv('PATH');
    Result := FileSearch('ffmpeg.exe', PathEnv) <> '';
  end;
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep <> ssPostInstall then Exit;
  if not SaveStringToFile(ExpandConstant('{app}\setup-language.txt'), CustomMessage('AppLanguage'), False) then
    RaiseException(SysErrorMessage(DLLGetLastError));
  if (not WizardSilent()) and (not FfmpegAvailable()) then
    MsgBox(CustomMessage('FfmpegHint'), mbInformation, MB_OK);
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
begin
  if (CurUninstallStep = usPostUninstall) and DirExists(SettingsDir()) then
  begin
    { Ohne Rueckfrage (still) bleiben die Einstellungen immer erhalten }
    if UninstallSilent() then
      Exit;
    if MsgBox(FmtMessage(CustomMessage('KeepSettings'), [SettingsDir()]), mbConfirmation, MB_YESNO or MB_DEFBUTTON1) = IDNO then
      DelTree(SettingsDir(), True, True, True);
  end;
end;
