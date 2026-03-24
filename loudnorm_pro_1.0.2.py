import csv
import ctypes
from ctypes import wintypes
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import webbrowser
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from tkinterdnd2 import TkinterDnD, DND_FILES
    DND_AVAILABLE = True
except Exception:
    TkinterDnD = None
    DND_FILES = None
    DND_AVAILABLE = False



def get_app_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def get_data_dir() -> str:
    base_dir = get_app_dir()
    portable_marker = os.path.join(base_dir, "portable.mode")
    if os.path.exists(portable_marker):
        os.makedirs(base_dir, exist_ok=True)
        return base_dir

    local_appdata = os.environ.get("LOCALAPPDATA")
    if local_appdata:
        data_dir = os.path.join(local_appdata, "Loudnorm PRO")
    else:
        data_dir = base_dir

    os.makedirs(data_dir, exist_ok=True)
    return data_dir


DEFAULT_INPUT = ""
DEFAULT_OUTPUT = ""

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".mov", ".m4v", ".ts"}
CRASH_LOG = os.path.join(get_data_dir(), "loudnorm_gui_crash.log")
MAX_JOB_ROWS = 8
JOB_FRAME_BASE_HEIGHT = 52
JOB_ROW_HEIGHT = 74
VISIBLE_JOB_ROWS = 3
PREVIEW_BASE_HEIGHT = 77
PREVIEW_ROW_HEIGHT = 26
PREVIEW_MIN_ROWS = 4
PREVIEW_MAX_ROWS = 8
NO_WINDOW = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
LOUDNORM_SUFFIX_RE = re.compile(r"(?:_loudnorm(?:_nvenc)?)$", re.IGNORECASE)
RESUME_STATE_FILE = os.path.join(get_data_dir(), "loudnorm_resume_state.csv")
SETTINGS_FILE = "loudnorm_settings.json"

APP_VERSION = "1.0.2"
BUILD_DATE = "2026-03-24 18:10"
PROJECT_LICENSE_NOTICE = "Released under GNU GPL v3"
GITHUB_REPO = "urscaviezel/Loudnorm-PRO"
GITHUB_RELEASES_URL = f"https://github.com/{GITHUB_REPO}/releases"
GITHUB_API_LATEST = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
UPDATE_TIMEOUT_SECONDS = 12

PREFERRED_LANGUAGE_CHOICES = {
    "de": [("de", "Deutsch"), ("en", "Englisch"), ("orig", "Original"), ("first", "Erste Spur")],
    "en": [("de", "German"), ("en", "English"), ("orig", "Original"), ("first", "First track")],
}
PREFERRED_LANGUAGE_ALIASES = {
    "de": {"de", "deu", "ger"},
    "en": {"en", "eng"},
}
UI_TEXT_DE = {
    "Loudnorm PRO": "Loudnorm PRO",
    "Quelle": "Quelle",
    "Ordner": "Ordner",
    "Einzeldateien": "Einzeldateien",
    "Eingabeordner": "Eingabeordner",
    "Durchsuchen": "Durchsuchen",
    "Dateien": "Dateien",
    "Entfernen": "Entfernen",
    "Leeren": "Leeren",
    "Verarbeitung": "Verarbeitung",
    "Ausgabeordner": "Ausgabeordner",
    "Audio": "Audio",
    "Normalisierung": "Normalisierung",
    "Video": "Video",
    "Parallel-Jobs": "Parallel-Jobs",
    "Start": "Start",
    "Abbrechen": "Abbrechen",
    "Schliessen": "Schliessen",
    "Tools": "Tools",
    "Fortschritt": "Fortschritt",
    "Audio-Track Vorschau": "Audio-Track Vorschau",
    "Aktive Jobs": "Aktive Jobs",
    "Log": "Log",
    "Vorschau ausblenden": "Vorschau ausblenden",
    "Vorschau anzeigen": "Vorschau anzeigen",
    "Build-Info": "Build-Info",
    "Update": "Update",
    "Version kopieren": "Version kopieren",
    "GitHub öffnen": "GitHub öffnen",
    "Sprache": "Sprache",
    "Keine aktiven Jobs": "Keine aktiven Jobs",
    "Originaldateien überschreiben": "Originaldateien überschreiben",
    "Temporärer Arbeitsordner": "Temporärer Arbeitsordner",
    "Theme": "Theme",
    "Hell": "Hell",
    "Dunkel": "Dunkel",
}
UI_TEXT_EN = {
    "Loudnorm PRO": "Loudnorm PRO",
    "Quelle": "Source",
    "Ordner": "Folder",
    "Einzeldateien": "Files",
    "Eingabeordner": "Input folder",
    "Durchsuchen": "Browse",
    "Dateien": "Files",
    "Entfernen": "Remove",
    "Leeren": "Clear",
    "Verarbeitung": "Processing",
    "Ausgabeordner": "Output folder",
    "Audio": "Audio",
    "Audio-Bitrate": "Audio bitrate",
    "Normalisierung": "Normalization",
    "Video": "Video",
    "Video-Preset": "Video preset",
    "Video-Bitrate": "Video bitrate",
    "Parallel-Jobs": "Parallel jobs",
    "Start": "Start",
    "Abbrechen": "Cancel",
    "Schliessen": "Close",
    "Tools": "Tools",
    "Fortschritt": "Progress",
    "Audio-Track Vorschau": "Audio track preview",
    "Aktive Jobs": "Active jobs",
    "Log": "Log",
    "Vorschau ausblenden": "Hide preview",
    "Vorschau anzeigen": "Show preview",
    "Build-Info": "Build info",
    "Update": "Update",
    "Version kopieren": "Copy version",
    "GitHub öffnen": "Open GitHub",
    "Bevorzugte Sprache": "Preferred language",
    "Bevorzugte Sprache nach vorne setzen + als Default markieren": "Move preferred language first + mark as default",
    "Sprache": "Language",
    "Keine aktiven Jobs": "No active jobs",
    "Originaldateien überschreiben": "Overwrite original files",
    "Ursprünglichen Timestamp beibehalten": "Preserve original timestamp",
    "Temporärer Arbeitsordner": "Temporary work folder",
    "Unterbrochene Jobs fortsetzen": "Resume interrupted jobs",
    "Bereit": "Ready",
    "Theme": "Theme",
    "Hell": "Light",
    "Dunkel": "Dark",
}


I18N_MSGS = {
    "de": {
        "not_found": "nicht gefunden",
        "ready": "Bereit",
        "checking_settings": "Pruefe Einstellungen...",
        "done": "Fertig",
        "no_files_found": "Keine Dateien gefunden",
        "no_file_selected_preview": "Keine Datei fuer Audio-Vorschau ausgewaehlt",
        "choose_file_or_folder": "Waehle eine Datei oder einen Ordner mit Videodateien aus.",
        "loading_preview": "Lade Audio-Track Vorschau...",
        "no_audio_preview": "Keine Audiospuren gefunden oder Vorschau nicht verfuegbar.",
        "ffprobe_preview_unavailable": "ffprobe nicht gefunden. Audio-Vorschau ist nicht verfuegbar.",
        "preview_prefix": "Vorschau:",
        "preview_normalized_suffix": "Spur(en) werden normalisiert.",
        "no_active_jobs": "Keine aktiven Jobs",
        "resume_jobs": "Unterbrochene Jobs fortsetzen",
        "resume_hint": "Hinweis: Schneller nach Abbruch. Bereits abgeschlossene Dateien aus dem Ausgabeordner werden beim Neustart übersprungen.",
        "dragdrop_enabled": "Drag & Drop aktiv",
        "dragdrop_disabled": "Drag & Drop aus",
        "drop_here": "Dateien oder Ordner hier hineinziehen",
        "use_buttons": "Buttons zum Hinzufügen verwenden",
        "cancel_requested": "Abbruch angefordert...",
        "processing_aborted": "Verarbeitung abgebrochen.",
        "processing_completed": "Verarbeitung abgeschlossen.",
        "source_folder": "Ordner",
        "source_files": "Einzeldateien",
        "source_label": "Quelle",
        "audio_mode_label": "AudioMod",
        "analysis_label": "Analyse",
        "parallel_label": "Parallel",
        "resume_on": "aktiv",
        "resume_off": "aus",
        "total_files": "Gesamtdateien",
        "resume_skipped": "Resume-Übersprungen",
        "done_title": "Fertig",
        "input_folder_not_found": "Eingabeordner nicht gefunden.",
        "ffmpeg_missing_title": "Fehler",
        "ffmpeg_missing_message": "ffmpeg.exe wurde nicht gefunden.\n\nGesucht in:\n- EXE/Script-Ordner\n- .\\ffmpeg\\bin\\\n- C:\\ffmpeg\\bin\\",
        "ffprobe_missing_message": "ffprobe.exe wurde nicht gefunden.\n\nGesucht in:\n- EXE/Script-Ordner\n- .\\ffmpeg\\bin\\\n- C:\\ffmpeg\\bin\\",
        "build_info_title": "Build-Info",
        "license_notice": "Lizenz-Hinweis:",
        "third_party": "Drittkomponenten:",
        "ffmpeg_license_note": "- FFmpeg (eigene Lizenzbedingungen beachten)",
        "parallel_auto_copy_nvenc": "Auto aktiv: {jobs} Job(s) | Recommendation COPY={copy}, NVENC={nvenc}",
        "parallel_auto_analysis_encode": "Auto aktiv: Analyse={analysis}, Encode={encode} | COPY={copy}, NVENC={nvenc}",
        "parallel_manual_copy_nvenc": "Manuell: {jobs} Job(s) | Recommendation COPY={copy}, NVENC={nvenc}",
        "files_added": "{count} Datei(en) hinzugefügt.",
        "files_added_from_folder": "{count} Datei(en) aus Ordner hinzugefügt.",
        "select_video_files_title": "Videodateien auswählen",
        "select_folder_title": "Ordner auswählen",
        "waiting": "Wartet...",
        "job_done": "Abgeschlossen",
        "job_skipped": "Uebersprungen",
        "overwrite_hint": "Bei Erfolg wird die Originaldatei durch eine temporäre Ausgabedatei ersetzt.",
                "overwrite_done": "Originaldatei ersetzt",
        "preserve_timestamp_label": "Ursprünglichen Timestamp beibehalten",
                "replace_failed": "Ersetzen fehlgeschlagen: {error}",
                                                                "pass1_analyze_track": "Pass 1: Analyse Spur {current}/{total}",
        "pass1_error": "Pass 1 Fehler",
        "pass1_missing_stats": "Keine loudnorm Analysewerte fuer Spur {track} gefunden.",
        "pass1_phase_track": "Pass 1 Spur {track}",
        "pass1_json_incomplete": "Pass-1 JSON fuer Spur {track} unvollstaendig.",
        "analysis_finished": "Analyse fertig",
        "pass2_normalization": "Pass 2: Normalisierung",
        "eta_short": "ETA {eta}",
        "update_available_title": "Update verfuegbar",
        "update_not_available_title": "Kein Update",
        "update_error_title": "Update-Fehler",
        "update_busy_title": "Update gesperrt",
        "update_checking": "Suche nach Updates...",
        "update_busy_message": "Bitte erst warten, bis keine Verarbeitung mehr laeuft.",
        "update_available_message": "Version {version} ist verfuegbar.\n\nMoechtest du das Update jetzt herunterladen und installieren?",
        "update_not_available_message": "Du verwendest bereits die aktuelle Version ({version}).",
        "update_download_started": "Update wird heruntergeladen...",
        "update_download_finished": "Update heruntergeladen. Die App wird jetzt neu gestartet.",
        "update_download_failed": "Update konnte nicht heruntergeladen werden:\n{error}",
        "update_check_failed": "Update-Pruefung fehlgeschlagen:\n{error}",
        "update_source_mode_message": "Neue Version {version} gefunden.\n\nZum Aktualisieren wird die Release-Seite im Browser geoeffnet, weil die App nicht als EXE laeuft.",
        "update_no_exe_asset": "Es wurde kein EXE-Asset in der neuesten Release gefunden.",
    },
    "en": {
        "not_found": "not found",
        "ready": "Ready",
        "checking_settings": "Checking settings...",
        "done": "Done",
        "no_files_found": "No files found",
        "no_file_selected_preview": "No file selected for audio preview",
        "choose_file_or_folder": "Choose a file or folder with video files.",
        "loading_preview": "Loading audio track preview...",
        "no_audio_preview": "No audio streams found or preview unavailable.",
        "ffprobe_preview_unavailable": "ffprobe not found. Audio preview unavailable.",
        "preview_prefix": "Preview:",
        "preview_normalized_suffix": "track(s) will be normalized.",
        "no_active_jobs": "No active jobs",
        "resume_jobs": "Resume interrupted jobs",
        "resume_hint": "Hint: Faster after interruption. Already completed files in the output folder are skipped on restart.",
        "dragdrop_enabled": "Drag & Drop enabled",
        "dragdrop_disabled": "Drag & Drop off",
        "drop_here": "Drop files or folders here",
        "use_buttons": "Use the buttons to add files",
        "cancel_requested": "Cancellation requested...",
        "processing_aborted": "Processing aborted.",
        "processing_completed": "Processing completed.",
        "source_folder": "Folder",
        "source_files": "Files",
        "source_label": "Source",
        "audio_mode_label": "AudioMode",
        "analysis_label": "Analysis",
        "parallel_label": "Parallel",
        "resume_on": "on",
        "resume_off": "off",
        "total_files": "Total files",
        "resume_skipped": "Resume-skipped",
        "done_title": "Done",
        "input_folder_not_found": "Input folder not found.",
        "ffmpeg_missing_title": "Error",
        "ffmpeg_missing_message": "ffmpeg.exe was not found.\n\nSearched in:\n- EXE/Script folder\n- .\\ffmpeg\\bin\\\n- C:\\ffmpeg\\bin\\",
        "ffprobe_missing_message": "ffprobe.exe was not found.\n\nSearched in:\n- EXE/Script folder\n- .\\ffmpeg\\bin\\\n- C:\\ffmpeg\\bin\\",
        "build_info_title": "Build info",
        "license_notice": "License notice:",
        "third_party": "Third-party components:",
        "ffmpeg_license_note": "- FFmpeg (check its own license terms)",
        "parallel_auto_copy_nvenc": "Auto active: {jobs} job(s) | Recommendation COPY={copy}, NVENC={nvenc}",
        "parallel_auto_analysis_encode": "Auto active: Analysis={analysis}, Encode={encode} | COPY={copy}, NVENC={nvenc}",
        "parallel_manual_copy_nvenc": "Manual: {jobs} job(s) | Recommendation COPY={copy}, NVENC={nvenc}",
        "files_added": "{count} file(s) added.",
        "files_added_from_folder": "{count} file(s) added from folder.",
        "select_video_files_title": "Select video files",
        "select_folder_title": "Select folder",
        "waiting": "Waiting...",
        "job_done": "Completed",
        "job_skipped": "Skipped",
        "overwrite_hint": "On success, the original file will be replaced by a temporary output file.",
                "overwrite_done": "Original file replaced",
        "preserve_timestamp_label": "Keep original timestamp",
                "replace_failed": "Replace failed: {error}",
                                                                "pass1_analyze_track": "Pass 1: Analyze track {current}/{total}",
        "pass1_error": "Pass 1 error",
        "pass1_missing_stats": "No loudnorm analysis values found for track {track}.",
        "pass1_phase_track": "Pass 1 track {track}",
        "pass1_json_incomplete": "Pass 1 JSON for track {track} is incomplete.",
        "analysis_finished": "Analysis complete",
        "pass2_normalization": "Pass 2: Normalization",
        "eta_short": "ETA {eta}",
        "update_available_title": "Update available",
        "update_not_available_title": "No update",
        "update_error_title": "Update error",
        "update_busy_title": "Update locked",
        "update_checking": "Checking for updates...",
        "update_busy_message": "Please wait until no processing is running.",
        "update_available_message": "Version {version} is available.\n\nDo you want to download and install the update now?",
        "update_not_available_message": "You are already using the latest version ({version}).",
        "update_download_started": "Downloading update...",
        "update_download_finished": "Update downloaded. The app will now restart.",
        "update_download_failed": "The update could not be downloaded:\n{error}",
        "update_check_failed": "Update check failed:\n{error}",
        "update_source_mode_message": "A new version {version} was found.\n\nThe releases page will be opened in your browser because the app is not running as an EXE.",
        "update_no_exe_asset": "No EXE asset was found in the latest release.",
    },
}


def write_crash_log(text: str) -> None:
    try:
        stamp = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(CRASH_LOG, "a", encoding="utf-8") as f:
            f.write(f"[{stamp}] {text}\n")
    except Exception:
        pass


def resource_path(relative_path: str) -> str:
    try:
        base_path = getattr(sys, "_MEIPASS", get_app_dir())
    except Exception:
        base_path = get_app_dir()
    return os.path.join(base_path, relative_path)


def set_windows_appusermodel_id() -> None:
    if os.name != "nt":
        return
    try:
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("urscaviezel.LoudnormPRO")
    except Exception:
        pass


def resolve_app_icon_path() -> str | None:
    candidates = [
        "loudnorm_pro_icon_optimized.ico",
        "loudnorm_pro_icon.ico",
    ]
    for name in candidates:
        path = resource_path(name)
        if os.path.exists(path):
            return path
    return None


def resolve_app_icon_photo_paths() -> list[str]:
    candidates = [
        "loudnorm_pro_icon_16.png",
        "loudnorm_pro_icon_32.png",
        "loudnorm_pro_icon_48.png",
        "loudnorm_pro_icon_optimized_256.png",
    ]
    result = []
    for name in candidates:
        path = resource_path(name)
        if os.path.exists(path):
            result.append(path)
    return result


def get_app_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def resolve_tool_path(filename: str):
    app_dir = get_app_dir()
    candidates = [
        os.path.join(app_dir, filename),
        os.path.join(app_dir, "ffmpeg", "bin", filename),
        os.path.join(r"C:\ffmpeg\bin", filename),
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return None


def find_loudnorm_json(text: str):
    if not text:
        return None
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        block = text[start:end + 1]
        try:
            return json.loads(block)
        except Exception:
            return None
    return None


def normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def canonical_output_stem(name: str) -> str:
    return LOUDNORM_SUFFIX_RE.sub("", name)


def sanitize_windows_config_path(value: str) -> str:
    value = (value or "").strip().strip('"')
    if not value:
        return ""

    repaired = []
    for ch in value:
        if ch == "\t":
            repaired.append("\\t")
        elif ch == "\n":
            repaired.append("\\n")
        elif ch == "\r":
            repaired.append("\\r")
        elif ch == "\f":
            repaired.append("\\f")
        elif ch == "\v":
            repaired.append("\\v")
        else:
            repaired.append(ch)

    value = "".join(repaired)
    return value.replace("\\", "/")



def normalize_stream_language_code(value: str) -> str:
    value = (value or "").strip().lower()
    if value in {"de", "deu", "ger"}:
        return "de"
    if value in {"en", "eng"}:
        return "en"
    return value


def stream_matches_language(info: dict, preferred_key: str) -> bool:
    if preferred_key == "first":
        return False

    language = normalize_stream_language_code(info.get("language") or "")
    title = (info.get("title") or "").strip().lower()

    if preferred_key == "orig":
        return bool(language)

    aliases = PREFERRED_LANGUAGE_ALIASES.get(preferred_key, {preferred_key})
    if language in aliases or language == preferred_key:
        return True

    title_keywords = {
        "de": ("deutsch", "german"),
        "en": ("english", "englisch"),
    }.get(preferred_key, tuple())

    return any(keyword in title for keyword in title_keywords)



def get_media_duration_seconds(ffprobe_path: str, input_file: str) -> float:
    try:
        cp = subprocess.run(
            [
                ffprobe_path,
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                input_file,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=NO_WINDOW,
        )
        lines = (cp.stdout or "").strip().splitlines()
        if lines:
            value = float(lines[0].strip())
            return value if value > 0 else 1.0
    except Exception:
        pass
    return 1.0


def get_audio_stream_count(ffprobe_path: str, input_file: str) -> int:
    try:
        cp = subprocess.run(
            [
                ffprobe_path,
                "-v", "error",
                "-select_streams", "a",
                "-show_entries", "stream=index",
                "-of", "json",
                input_file,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=NO_WINDOW,
        )
        data = json.loads(cp.stdout or "{}")
        streams = data.get("streams", [])
        return len(streams)
    except Exception:
        return 0


def get_audio_stream_info(ffprobe_path: str, input_file: str):
    try:
        cp = subprocess.run(
            [
                ffprobe_path,
                "-v", "error",
                "-select_streams", "a",
                "-show_entries",
                "stream=index,codec_name,channels,channel_layout,sample_rate:stream_tags=language,title:stream_disposition=default,forced",
                "-of", "json",
                input_file,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=NO_WINDOW,
        )

        data = json.loads(cp.stdout or "{}")
        streams = data.get("streams", [])
        result = []

        for s in streams:
            tags = s.get("tags", {}) or {}
            disposition = s.get("disposition", {}) or {}

            result.append(
                {
                    "index": s.get("index"),
                    "language": tags.get("language", ""),
                    "title": tags.get("title", ""),
                    "default": int(disposition.get("default", 0) or 0),
                    "forced": int(disposition.get("forced", 0) or 0),
                    "codec": s.get("codec_name", ""),
                    "channels": s.get("channels"),
                    "channel_layout": s.get("channel_layout", ""),
                    "sample_rate": s.get("sample_rate", ""),
                }
            )

        return result
    except Exception:
        return []


def parse_loudnorm_stats(json_obj):
    if not json_obj:
        return None

    stats = {
        "input_i": str(json_obj.get("input_i", "")).strip(),
        "input_tp": str(json_obj.get("input_tp", "")).strip(),
        "input_lra": str(json_obj.get("input_lra", "")).strip(),
        "input_thresh": str(json_obj.get("input_thresh", "")).strip(),
        "target_offset": str(json_obj.get("target_offset", "")).strip(),
    }

    if not all(stats.values()):
        return None
    return stats


def format_eta(seconds: float) -> str:
    if seconds <= 0:
        return "ETA: 0 min"
    seconds = int(round(seconds))
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    if hours > 0:
        return f"ETA: {hours} h {minutes:02d} min"
    if minutes > 0:
        return f"ETA: {minutes} min"
    return f"ETA: {secs} s"



def format_eta_short(seconds: float) -> str:
    try:
        seconds = max(0, int(seconds))
    except Exception:
        return "--"
    if seconds < 60:
        return f"{seconds}s"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {sec:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"



def parse_dnd_files(data: str):
    parts = []
    buf = ""
    in_brace = False

    for ch in data:
        if ch == "{":
            in_brace = True
            if buf.strip():
                parts.extend(buf.strip().split())
                buf = ""
            continue
        if ch == "}":
            in_brace = False
            if buf:
                parts.append(buf)
                buf = ""
            continue
        if ch == " " and not in_brace:
            if buf:
                parts.append(buf)
                buf = ""
            continue
        buf += ch

    if buf:
        parts.append(buf)

    cleaned = []
    for p in parts:
        p = p.strip().strip('"').strip()
        if p:
            cleaned.append(p)
    return cleaned


def is_video_file(path: str) -> bool:
    return Path(path).suffix.lower() in VIDEO_EXTS


def collect_videos_from_folder(folder: str):
    files = []
    for root_dir, _, file_names in os.walk(folder):
        for name in file_names:
            p = str(Path(root_dir) / name)
            if is_video_file(p) and not re.search(r"_loudnorm($|_)", Path(p).stem, re.IGNORECASE):
                files.append(p)
    return files


def find_first_video_in_folder(folder: str):
    try:
        for root_dir, _, file_names in os.walk(folder):
            for name in sorted(file_names, key=str.lower):
                p = str(Path(root_dir) / name)
                if is_video_file(p) and not re.search(r"_loudnorm($|_)", Path(p).stem, re.IGNORECASE):
                    return p
    except Exception:
        pass
    return None




def _filetime_from_timestamp(timestamp: float) -> int:
    return int((float(timestamp) + 11644473600.0) * 10000000)


def get_path_timestamps(path: str) -> dict:
    st = os.stat(path)
    data = {
        "created": float(getattr(st, "st_ctime", st.st_mtime)),
        "accessed": float(st.st_atime),
        "modified": float(st.st_mtime),
    }
    if os.name == "nt":
        try:
            GetFileAttributesExW = ctypes.windll.kernel32.GetFileAttributesExW
            GetFileAttributesExW.argtypes = [wintypes.LPCWSTR, wintypes.INT, wintypes.LPVOID]
            data_buf = (ctypes.c_byte * 36)()
            if GetFileAttributesExW(path, 0, ctypes.byref(data_buf)):
                ft_create = ctypes.c_ulonglong.from_buffer(data_buf, 4).value
                if ft_create:
                    data["created"] = (ft_create / 10000000.0) - 11644473600.0
        except Exception:
            pass
    return data


def set_path_timestamps(path: str, created: float | None = None, accessed: float | None = None, modified: float | None = None) -> None:
    if accessed is not None or modified is not None:
        current = os.stat(path)
        os.utime(path, (
            float(accessed if accessed is not None else current.st_atime),
            float(modified if modified is not None else current.st_mtime),
        ))

    if created is not None and os.name == "nt":
        GENERIC_WRITE = 0x40000000
        OPEN_EXISTING = 3
        FILE_ATTRIBUTE_NORMAL = 0x80
        handle = ctypes.windll.kernel32.CreateFileW(
            wintypes.LPCWSTR(path),
            GENERIC_WRITE,
            0,
            None,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            None,
        )
        if handle == wintypes.HANDLE(-1).value:
            raise ctypes.WinError()

        try:
            c_time = ctypes.c_ulonglong(_filetime_from_timestamp(created))
            a_time = ctypes.c_ulonglong(_filetime_from_timestamp(accessed if accessed is not None else os.stat(path).st_atime))
            m_time = ctypes.c_ulonglong(_filetime_from_timestamp(modified if modified is not None else os.stat(path).st_mtime))
            if not ctypes.windll.kernel32.SetFileTime(
                handle,
                ctypes.byref(c_time),
                ctypes.byref(a_time),
                ctypes.byref(m_time),
            ):
                raise ctypes.WinError()
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)

class LoudnormApp:
    def __init__(self, root):
        self.root = root
        self.root.title(f"Loudnorm PRO GUI v{APP_VERSION}")
        self.root.geometry("1040x680")
        self.root.minsize(760, 540)
        self.root.configure(bg="#202020")
        self._app_icon_photos = []
        try:
            icon_photo_paths = resolve_app_icon_photo_paths()
            for icon_photo_path in icon_photo_paths:
                self._app_icon_photos.append(tk.PhotoImage(file=icon_photo_path))
            if self._app_icon_photos:
                self.root.iconphoto(True, *self._app_icon_photos)
        except Exception:
            self._app_icon_photos = []
        try:
            icon_path = resolve_app_icon_path()
            if icon_path:
                self.root.iconbitmap(default=icon_path)
        except Exception:
            pass

        self.cancel_requested = False
        self.worker_thread = None
        self.ui_queue = queue.Queue()

        self.processes = []
        self.processes_lock = threading.Lock()

        self.job_row_locks = [threading.Lock() for _ in range(MAX_JOB_ROWS)]
        self.active_job_map = {}
        self.active_job_map_lock = threading.Lock()

        self.run_started_ts = None
        self.completed_times = []
        self.completed_times_for_eta = []

        self.ffmpeg_path = resolve_tool_path("ffmpeg.exe")
        self.ffprobe_path = resolve_tool_path("ffprobe.exe")

        self.file_list = []
        self.preview_file_path = None
        self.preview_audio_info = []
        self.audio_preview_request_id = 0
        self.audio_preview_after_id = None
        self.preview_visible_var = tk.BooleanVar(value=False)
        self.audio_preview_file_var = tk.StringVar(value="Keine Datei fuer Audio-Vorschau ausgewaehlt")
        self.audio_preview_status_var = tk.StringVar(value="Waehle eine Datei oder einen Ordner mit Videodateien aus.")

        self.source_mode_var = tk.StringVar(value="folder")
        self.input_var = tk.StringVar(value=DEFAULT_INPUT)
        self.output_var = tk.StringVar(value=DEFAULT_OUTPUT)
        self.temp_work_dir_var = tk.StringVar(value="")
        self.audio_var = tk.StringVar(value="AAC")
        self.audio_bitrate_var = tk.StringVar(value="384k")
        self.audio_track_mode_var = tk.StringVar(value="Auto (bevorzugte Sprache)")
        self.audio_track_mode_hint_var = tk.StringVar(value="Hinweis: Schnellste Option. Wenn eine Spur in der bevorzugten Sprache erkannt wird, wird diese normalisiert, sonst die erste Audiospur.")
        self.prefer_german_first_var = tk.BooleanVar(value=True)
        self.preferred_language_var = tk.StringVar(value="Deutsch")
        self.prefer_german_first_hint_var = tk.StringVar(value="Optional: Nur im Modus 'Nur bevorzugte Sprache' wird die erste passende Spur nach vorne einsortiert und als Default markiert.")
        self.resume_jobs_var = tk.BooleanVar(value=True)
        self.overwrite_original_var = tk.BooleanVar(value=False)
        self.preserve_timestamp_var = tk.BooleanVar(value=True)
        self.overwrite_hint_var = tk.StringVar(value="")
        self.overwrite_warning_var = tk.StringVar(value="")
        self.resume_jobs_hint_var = tk.StringVar(value="Hinweis: Schneller nach Abbruch. Bereits abgeschlossene Dateien aus dem Ausgabeordner werden beim Neustart übersprungen.")
        self.video_var = tk.StringVar(value="COPY")
        self.video_preset_var = tk.StringVar(value="p5 balanced")
        self.video_bitrate_var = tk.StringVar(value="CQ 19 (quality)")
        self.jobs_var = tk.StringVar(value="Auto")
        self.parallel_hint_var = tk.StringVar(value="Auto: --")
        self.language_var = tk.StringVar(value="Deutsch")
        self.theme_var = tk.StringVar(value="Dunkel")
        self.update_check_in_progress = False
        self.startup_update_prompt_shown = False

        self.load_settings()
        self._build_ui()
        self._bind_events()

        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.after(100, self.process_ui_queue)


    def get_settings_path(self) -> str:
        return os.path.join(get_data_dir(), SETTINGS_FILE)

    def save_settings(self) -> None:
        try:
            temp_work_dir_value = sanitize_windows_config_path(self.temp_work_dir_var.get())

            if not temp_work_dir_value:
                try:
                    settings_path = self.get_settings_path()
                    if os.path.exists(settings_path):
                        with open(settings_path, "r", encoding="utf-8") as f:
                            existing_data = json.load(f)
                        existing_temp = sanitize_windows_config_path(existing_data.get("temp_work_dir", ""))
                        if existing_temp:
                            temp_work_dir_value = existing_temp
                except Exception:
                    pass

            data = {
                "source_mode": self.source_mode_var.get(),
                "input": self.input_var.get(),
                "output": self.output_var.get(),
                "temp_work_dir": temp_work_dir_value,
                "audio": self.audio_var.get(),
                "audio_bitrate": self.audio_bitrate_var.get(),
                "audio_track_mode": self.get_audio_track_mode_key(),
                "preferred_language": self.get_preferred_language_key(),
                "prefer_first": bool(self.prefer_german_first_var.get()),
                "resume_jobs": bool(self.resume_jobs_var.get()),
                "video": self.video_var.get(),
                "video_preset": self.video_preset_var.get(),
                "video_bitrate": self.video_bitrate_var.get(),
                "jobs": self.jobs_var.get(),
                "language": self.language_var.get(),
                "theme": self.theme_var.get(),
                "preview_visible": bool(self.preview_visible_var.get()),
            }
            with open(self.get_settings_path(), "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception:
            pass

    def load_settings(self) -> None:
        try:
            path = self.get_settings_path()
            if not os.path.exists(path):
                return
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            source_mode = data.get("source_mode")
            if source_mode in {"folder", "files"}:
                self.source_mode_var.set(source_mode)

            val = data.get("input")
            if isinstance(val, str):
                self.input_var.set(val)
            val = data.get("output")
            if isinstance(val, str):
                self.output_var.set(val)
            val = data.get("temp_work_dir")
            if isinstance(val, str):
                self.temp_work_dir_var.set(sanitize_windows_config_path(val))

            if data.get("audio") in {"AAC", "E-AC3"}:
                self.audio_var.set(data["audio"])
            val = data.get("audio_bitrate")
            if isinstance(val, str):
                self.audio_bitrate_var.set(val)

            mode_key = data.get("audio_track_mode")
            if mode_key in {"auto", "all", "preferred_only"}:
                self.audio_track_mode_var.set(self.get_audio_track_mode_display(mode_key))

            pref_key = data.get("preferred_language")
            if pref_key in {"de", "en", "orig", "first"}:
                self.preferred_language_var.set(self.get_preferred_language_display(pref_key))

            if "prefer_first" in data:
                self.prefer_german_first_var.set(bool(data["prefer_first"]))
            if "resume_jobs" in data:
                self.resume_jobs_var.set(bool(data["resume_jobs"]))

            if data.get("video") in {"COPY", "HEVC NVENC"}:
                self.video_var.set(data["video"])
            val = data.get("video_preset")
            if isinstance(val, str):
                self.video_preset_var.set(val)
            val = data.get("video_bitrate")
            if isinstance(val, str):
                self.video_bitrate_var.set(val)

            jobs = str(data.get("jobs", "")).strip()
            if jobs in {"Auto", "1", "2", "3", "4", "5", "6", "7", "8"}:
                self.jobs_var.set(jobs)

            language = data.get("language")
            if language in {"Deutsch", "English"}:
                self.language_var.set(language)

            theme = data.get("theme")
            if theme in {"Dunkel", "Hell", "Dark", "Light"}:
                self.theme_var.set("Dunkel" if theme == "Dark" else "Hell" if theme == "Light" else theme)

            if "preview_visible" in data:
                self.preview_visible_var.set(bool(data["preview_visible"]))
        except Exception:
            pass

    def bind_settings_persistence(self) -> None:
        def _save(*_args):
            self.save_settings()

        vars_to_watch = [
            self.source_mode_var,
            self.input_var,
            self.output_var,
            self.temp_work_dir_var,
            self.audio_var,
            self.audio_bitrate_var,
            self.audio_track_mode_var,
            self.preferred_language_var,
            self.prefer_german_first_var,
            self.resume_jobs_var,
            self.video_var,
            self.video_preset_var,
            self.video_bitrate_var,
            self.jobs_var,
            self.language_var,
            self.theme_var,
            self.preview_visible_var,
        ]
        for var in vars_to_watch:
            try:
                var.trace_add("write", _save)
            except Exception:
                pass

    def _build_ui(self):
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("TProgressbar", troughcolor="#2d2d2d", background="#78ff78", lightcolor="#78ff78", darkcolor="#78ff78", bordercolor="#2d2d2d")

        self.root.grid_rowconfigure(0, weight=1)
        self.root.grid_columnconfigure(0, weight=1)

        self.main = tk.Frame(self.root, bg="#202020")
        self.main.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
        self.main.grid_rowconfigure(1, weight=1)
        self.main.grid_columnconfigure(0, weight=0)
        self.main.grid_columnconfigure(1, weight=1)

        header = tk.Frame(self.main, bg="#202020")
        header.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        header.grid_columnconfigure(0, weight=1)

        self.header_title = tk.Label(
            header,
            text="Loudnorm PRO",
            bg="#202020",
            fg="#50dcff",
            font=("Segoe UI Semibold", 17),
            anchor="w",
        )
        self.header_title.grid(row=0, column=0, sticky="w")

        self.lbl_language = tk.Label(
            header,
            text="Sprache",
            bg="#202020",
            fg="#9fdcff",
            font=("Segoe UI", 9),
        )
        self.lbl_language.grid(row=0, column=1, sticky="e", padx=(0, 6))

        self.language_combo = ttk.Combobox(
            header,
            textvariable=self.language_var,
            values=["Deutsch", "English"],
            state="readonly",
            width=10,
            font=("Segoe UI", 9),
        )
        self.language_combo.grid(row=0, column=2, sticky="e", padx=(0, 8))

        self.btn_theme = tk.Button(
            header,
            text="Dunkel",
            command=self.toggle_theme,
            bg="#373737",
            fg="white",
            activebackground="#4a4a4a",
            relief="flat",
            font=("Segoe UI", 9),
        )
        self.btn_theme.grid(row=0, column=3, sticky="e", padx=(0, 8), ipady=2, ipadx=8)

        self.btn_build_info = tk.Button(
            header,
            text="Build-Info",
            command=self.show_build_info,
            bg="#373737",
            fg="white",
            activebackground="#4a4a4a",
            relief="flat",
            font=("Segoe UI", 9),
        )
        self.btn_build_info.grid(row=0, column=4, sticky="e", ipady=2, ipadx=4)

        self.btn_update = tk.Button(
            header,
            text="Update",
            command=self.check_for_updates_manual,
            bg="#375a7a",
            fg="white",
            activebackground="#4b6f92",
            relief="flat",
            font=("Segoe UI", 9),
        )
        self.btn_update.grid(row=0, column=5, sticky="e", padx=(6, 0), ipady=2, ipadx=8)

        self.left_shell = tk.Frame(self.main, bg="#202020", width=340)
        self.left_shell.grid(row=1, column=0, sticky="nsew", padx=(0, 8))
        self.left_shell.grid_propagate(False)
        self.left_shell.grid_rowconfigure(1, weight=1)
        self.left_shell.grid_columnconfigure(0, weight=1)

        self.left_source_holder = tk.Frame(self.left_shell, bg="#202020")
        self.left_source_holder.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        self.left_source_holder.grid_columnconfigure(0, weight=1)

        self.left_settings_frame = tk.LabelFrame(
            self.left_shell,
            text="Einstellungen",
            bg="#202020",
            fg="#9fdcff",
            font=("Segoe UI Semibold", 10),
            bd=1,
        )
        self.left_settings_frame.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(0, 6))
        self.left_settings_frame.grid_rowconfigure(0, weight=1)
        self.left_settings_frame.grid_columnconfigure(0, weight=1)

        self.left_canvas = tk.Canvas(self.left_settings_frame, bg="#202020", highlightthickness=0, bd=0)
        self.left_canvas.grid(row=0, column=0, sticky="nsew", padx=(6, 0), pady=(4, 6))
        self.left_scrollbar = tk.Scrollbar(self.left_settings_frame, orient="vertical", command=self.left_canvas.yview)
        self.left_scrollbar.grid(row=0, column=1, sticky="ns", padx=(0, 6), pady=(4, 6))
        self.left_canvas.configure(yscrollcommand=self.left_scrollbar.set)

        self.left_tools_holder = tk.Frame(self.left_shell, bg="#202020")
        self.left_tools_holder.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(6, 0))
        self.left_tools_holder.grid_columnconfigure(0, weight=1)

        self.left_actions_holder = tk.Frame(self.left_shell, bg="#202020")
        self.left_actions_holder.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(6, 0))
        self.left_actions_holder.grid_columnconfigure(0, weight=1)

        self.left_col = tk.Frame(self.left_canvas, bg="#202020", width=320)
        self.left_col.grid_columnconfigure(0, weight=1)
        self.left_canvas_window = self.left_canvas.create_window((0, 0), window=self.left_col, anchor="nw")

        self.left_col.bind(
            "<Configure>",
            lambda e: self.left_canvas.configure(scrollregion=self.left_canvas.bbox("all"))
        )
        self.left_canvas.bind(
            "<Configure>",
            lambda e: self.left_canvas.itemconfigure(self.left_canvas_window, width=e.width)
        )
        self.root.after(200, self._bind_left_panel_mousewheel_targets)

        self.right_col = tk.Frame(self.main, bg="#202020")
        self.right_col.grid(row=1, column=1, sticky="nsew")
        self.right_col.grid_rowconfigure(2, weight=1)
        self.right_col.grid_columnconfigure(0, weight=1)

        self._build_left_column()
        self._build_right_column()

        self.update_parallel_ui()
        self.update_job_rows_visibility()
        self.update_tool_labels()
        self.update_audio_track_mode_hint()
        self.on_source_mode_changed()
        self.update_overwrite_ui()
        self.apply_language()
        self.apply_theme()
        self.root.after(1800, self.check_for_updates_silent)

        self.bind_settings_persistence()
        
        if DND_AVAILABLE:
            self._enable_drag_drop()



    def toggle_theme(self):
        current = self.theme_var.get()
        self.theme_var.set("Hell" if current in {"Dunkel", "Dark"} else "Dunkel")
        self.apply_theme()
        self.save_settings()

    def apply_theme(self):
        dark = self.theme_var.get() in {"Dunkel", "Dark"}
        bg = "#202020" if dark else "#f2f2f2"
        panel = "#202020" if dark else "#ffffff"
        entry_bg = "#2d2d2d" if dark else "#ffffff"
        entry_fg = "white" if dark else "#111111"
        accent = "#9fdcff" if dark else "#0b5cab"
        title_fg = "#50dcff" if dark else "#0b5cab"
        warn = "#ffd278" if dark else "#8a5a00"
        soft = "#9fdcff" if dark else "#3f6ea5"

        try:
            self.root.configure(bg=bg)
            self.main.configure(bg=bg)
            self.header_title.configure(bg=bg, fg=title_fg)
            self.lbl_language.configure(bg=bg, fg=accent)
            self.left_shell.configure(bg=bg)
            self.left_source_holder.configure(bg=bg)
            self.left_tools_holder.configure(bg=bg)
            self.left_actions_holder.configure(bg=bg)
            self.left_col.configure(bg=bg)
            self.left_canvas.configure(bg=bg)
            self.right_col.configure(bg=bg)
            self.preview_header.configure(bg=bg)
            self.progress_frame.configure(bg=bg, fg=accent)
            self.preview_frame.configure(bg=bg, fg=accent)
            self.jobs_frame.configure(bg=bg, fg=accent)
            self.log_frame.configure(bg=bg, fg=accent)
            self.source_frame.configure(bg=bg, fg=accent)
            self.left_settings_frame.configure(bg=bg, fg=accent)
            self.tools_frame.configure(bg=bg, fg=accent)
            self.btn_theme.configure(text=("Dunkel" if dark else "Hell"))
        except Exception:
            pass

        try:
            style = ttk.Style()
            style.configure("TProgressbar",
                            troughcolor="#2d2d2d" if dark else "#d8d8d8",
                            background="#78ff78",
                            lightcolor="#78ff78",
                            darkcolor="#78ff78",
                            bordercolor="#2d2d2d" if dark else "#d8d8d8")
            style.configure("Treeview", background=panel, fieldbackground=panel, foreground=entry_fg)
            style.configure("Treeview.Heading", background=entry_bg, foreground=entry_fg)
        except Exception:
            pass

        def walk(widget):
            for child in widget.winfo_children():
                try:
                    cls = child.winfo_class()
                    if cls in {"Frame", "Labelframe", "LabelFrame"}:
                        child.configure(bg=bg)
                        try:
                            child.configure(fg=accent)
                        except Exception:
                            pass
                    elif cls == "Label":
                        fg = child.cget("fg")
                        if fg in ("#ffd278", "#ffb070"):
                            child.configure(bg=bg, fg=warn)
                        elif fg in ("#9fdcff", "#50dcff"):
                            child.configure(bg=bg, fg=accent if fg == "#9fdcff" else title_fg)
                        else:
                            child.configure(bg=bg, fg=entry_fg)
                    elif cls in {"Button", "Checkbutton", "Radiobutton"}:
                        child.configure(bg=entry_bg if cls == "Button" else bg,
                                        fg=entry_fg,
                                        activebackground=entry_bg if cls == "Button" else bg,
                                        activeforeground=entry_fg,
                                        selectcolor=entry_bg)
                    elif cls == "Entry":
                        child.configure(bg=entry_bg, fg=entry_fg, insertbackground=entry_fg)
                    elif cls == "Text":
                        child.configure(bg="#141414" if dark else "#ffffff", fg=entry_fg, insertbackground=entry_fg)
                    elif cls == "Listbox":
                        child.configure(bg="#141414" if dark else "#ffffff", fg=entry_fg)
                    elif cls == "Canvas":
                        child.configure(bg=bg)
                except Exception:
                    pass
                walk(child)
        try:
            walk(self.main)
        except Exception:
            pass

    def get_lang_code(self):
        return "en" if self.language_var.get() == "English" else "de"

    def tr(self, de_text: str) -> str:
        return UI_TEXT_EN.get(de_text, de_text) if self.get_lang_code() == "en" else UI_TEXT_DE.get(de_text, de_text)

    def msg(self, key: str, **kwargs) -> str:
        value = I18N_MSGS[self.get_lang_code()].get(key, key)
        return value.format(**kwargs) if kwargs else value

    def not_found_text(self) -> str:
        return self.msg("not_found")


    def get_preferred_language_options(self):
        return [label for _key, label in PREFERRED_LANGUAGE_CHOICES[self.get_lang_code()]]

    def get_preferred_language_key(self, value: str | None = None):
        value = (value if value is not None else self.preferred_language_var.get()).strip()
        for key, label in PREFERRED_LANGUAGE_CHOICES["de"] + PREFERRED_LANGUAGE_CHOICES["en"]:
            if value == label:
                return key
        return "de"

    def get_preferred_language_display(self, key: str):
        for lang_key, label in PREFERRED_LANGUAGE_CHOICES[self.get_lang_code()]:
            if lang_key == key:
                return label
        return PREFERRED_LANGUAGE_CHOICES[self.get_lang_code()][0][1]



    def get_audio_track_mode_options(self):
        lang = self.get_lang_code()
        if lang == "en":
            return [
                "Auto (preferred language)",
                "All tracks",
                "Preferred language only",
            ]
        return [
            "Auto (bevorzugte Sprache)",
            "Alle Spuren",
            "Nur bevorzugte Sprache",
        ]

    def get_audio_track_mode_key(self, value: str | None = None):
        value = (value if value is not None else self.audio_track_mode_var.get()).strip()
        mapping = {
            "Auto (Deutsch bevorzugt)": "auto",
            "Auto (Prefer German)": "auto",
            "Auto (bevorzugte Sprache)": "auto",
            "Auto (preferred language)": "auto",
            "Alle Spuren": "all",
            "All tracks": "all",
            "Nur bevorzugte Sprache": "preferred_only",
            "Preferred language only": "preferred_only",
            "Nur bevorzugte Sprache": "preferred_only",
            "Preferred language only": "preferred_only",
        }
        return mapping.get(value, "auto")

    def get_audio_track_mode_display(self, key: str):
        lang = self.get_lang_code()
        labels = {
            "de": {
                "auto": "Auto (bevorzugte Sprache)",
                "all": "Alle Spuren",
                "preferred_only": "Nur bevorzugte Sprache",
            },
            "en": {
                "auto": "Auto (preferred language)",
                "all": "All tracks",
                "preferred_only": "Preferred language only",
            },
        }
        return labels[lang].get(key, labels[lang]["auto"])

    def update_audio_preview_headers(self):

        if not hasattr(self, "audio_preview_tree"):
            return
        lang = self.get_lang_code()
        headers = {
            "de": {"out": "Out", "lang": "Sprache", "title": "Titel", "format": "Format", "flags": "Flags", "action": "Aktion"},
            "en": {"out": "Out", "lang": "Language", "title": "Title", "format": "Format", "flags": "Flags", "action": "Action"},
        }[lang]
        for key, label in headers.items():
            self.audio_preview_tree.heading(key, text=label)

    def apply_language(self):
        prev_pref_key = self.get_preferred_language_key()
        prev_mode_key = self.get_audio_track_mode_key()
        self._translate_widget_tree(self.root)
        self.update_language_dependent_vars()
        try:
            self.audio_track_mode_combo.configure(values=self.get_audio_track_mode_options())
            self.audio_track_mode_var.set(self.get_audio_track_mode_display(prev_mode_key))
        except Exception:
            pass
        try:
            self.preferred_language_combo.configure(values=self.get_preferred_language_options())
            self.preferred_language_var.set(self.get_preferred_language_display(prev_pref_key))
        except Exception:
            pass
        try:
            self.chk_prefer_german_first.config(text=("Bevorzugte Sprache nach vorne setzen + als Default markieren" if self.get_lang_code() == "de" else "Move preferred language first + mark as default"))
        except Exception:
            pass
        try:
            self.left_settings_frame.config(text=("Einstellungen" if self.get_lang_code() == "de" else "Settings"))
            self.chk_overwrite_original.config(text=self.tr("Originaldateien überschreiben"))
            self.chk_preserve_timestamp.config(text=self.tr("Ursprünglichen Timestamp beibehalten"))
            self.update_overwrite_ui()
        except Exception:
            pass
        try:
            self.update_audio_track_mode_hint()
        except Exception:
            pass
        try:
            self.update_audio_preview_headers()
        except Exception:
            pass
        try:
            self.refresh_active_job_rows()
        except Exception:
            pass
        try:
            self.refresh_file_listbox()
        except Exception:
            pass
        try:
            self.update_tool_labels()
        except Exception:
            pass
        try:
            self.btn_update.config(text=self.tr("Update"))
            self.btn_theme.config(text=self.tr(self.theme_var.get()))
        except Exception:
            pass
    def on_language_changed(self):
        self.apply_language()
        try:
            self.schedule_audio_preview_refresh()
        except Exception:
            pass

    def on_preferred_language_changed(self):
        self.update_audio_track_mode_hint()
        try:
            self.schedule_audio_preview_refresh()
        except Exception:
            pass

    def _translate_widget_tree(self, parent):
        stack = [parent]
        while stack:
            widget = stack.pop()
            try:
                children = list(widget.winfo_children())
            except Exception:
                children = []
            stack.extend(children)

            try:
                text = widget.cget("text")
            except Exception:
                continue

            de_source = None
            for de, en in UI_TEXT_EN.items():
                if text == de or text == en:
                    de_source = de
                    break
            if de_source is None:
                continue
            try:
                widget.config(text=self.tr(de_source))
            except Exception:
                pass

    def update_language_dependent_vars(self):
        lang = self.get_lang_code()
        self.root.title("Loudnorm PRO GUI")
        if hasattr(self, "audio_track_mode_combo"):
            current_key = self.get_audio_track_mode_key()
            options = self.get_audio_track_mode_options()
            self.audio_track_mode_combo.configure(values=options)
            self.audio_track_mode_var.set(self.get_audio_track_mode_display(current_key))

        self.dnd_hint_var.set(self.msg("dragdrop_enabled") if DND_AVAILABLE else self.msg("dragdrop_disabled"))
        self.drop_hint_var.set(self.msg("drop_here") if DND_AVAILABLE else self.msg("use_buttons"))

        file_prefix_old = "Datei:" if lang == "en" else "File:"
        file_prefix_new = "File:" if lang == "en" else "Datei:"
        self.audio_preview_file_var.set(self.audio_preview_file_var.get().replace(file_prefix_old, file_prefix_new))

        current_status = self.audio_preview_status_var.get()
        replacements = {
            "No file selected for audio preview": self.msg("no_file_selected_preview"),
            "Keine Datei fuer Audio-Vorschau ausgewaehlt": self.msg("no_file_selected_preview"),
            "Choose a file or folder with video files.": self.msg("choose_file_or_folder"),
            "Waehle eine Datei oder einen Ordner mit Videodateien aus.": self.msg("choose_file_or_folder"),
            "Loading audio track preview...": self.msg("loading_preview"),
            "Lade Audio-Track Vorschau...": self.msg("loading_preview"),
            "No audio streams found or preview unavailable.": self.msg("no_audio_preview"),
            "Keine Audiospuren gefunden oder Vorschau nicht verfuegbar.": self.msg("no_audio_preview"),
            "ffprobe not found. Audio preview unavailable.": self.msg("ffprobe_preview_unavailable"),
            "ffprobe nicht gefunden. Audio-Vorschau ist nicht verfuegbar.": self.msg("ffprobe_preview_unavailable"),
            "Preview:": self.msg("preview_prefix"),
            "Vorschau:": self.msg("preview_prefix"),
            "track(s) will be normalized.": self.msg("preview_normalized_suffix"),
            "Spur(en) werden normalisiert.": self.msg("preview_normalized_suffix"),
        }
        for old, new in replacements.items():
            current_status = current_status.replace(old, new)
        self.audio_preview_status_var.set(current_status)

        if self.no_active_jobs_label is not None:
            self.no_active_jobs_label.config(text=self.msg("no_active_jobs"))

        self.resume_jobs_hint_var.set(self.msg("resume_hint"))
        try:
            self.chk_resume_jobs.config(text=self.msg("resume_jobs"))
        except Exception:
            pass

        try:
            current_progress = self.lbl_progress.cget("text")
            progress_map = {
                "Bereit": self.msg("ready"),
                "Ready": self.msg("ready"),
                "Pruefe Einstellungen...": self.msg("checking_settings"),
                "Checking settings...": self.msg("checking_settings"),
                "Fertig": self.msg("done"),
                "Done": self.msg("done"),
                "Keine Dateien gefunden": self.msg("no_files_found"),
                "No files found": self.msg("no_files_found"),
            }
            if current_progress in progress_map:
                self.lbl_progress.config(text=progress_map[current_progress])
        except Exception:
            pass


    def _parse_version_tuple(self, value: str):
        cleaned = re.sub(r"[^0-9.]", "", (value or "").strip())
        if not cleaned:
            return (0,)
        parts = [int(p) for p in cleaned.split(".") if p != ""]
        return tuple(parts) if parts else (0,)

    def _fetch_latest_release(self):
        req = urllib.request.Request(
            GITHUB_API_LATEST,
            headers={
                "User-Agent": f"Loudnorm-PRO/{APP_VERSION}",
                "Accept": "application/vnd.github+json",
            },
        )
        with urllib.request.urlopen(req, timeout=UPDATE_TIMEOUT_SECONDS) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
        return {
            "tag": data.get("tag_name", ""),
            "name": data.get("name", ""),
            "body": data.get("body", ""),
            "assets": data.get("assets", []),
            "html_url": data.get("html_url", GITHUB_RELEASES_URL),
        }

    def _find_exe_asset(self, release_info):
        for asset in release_info.get("assets", []):
            name = (asset.get("name") or "").lower()
            if name.endswith(".exe") and asset.get("browser_download_url"):
                return asset
        return None

    def check_for_updates_silent(self):
        if self.startup_update_prompt_shown:
            return
        self.check_for_updates(manual=False)

    def check_for_updates_manual(self):
        self.check_for_updates(manual=True)

    def check_for_updates(self, manual: bool = False):
        if self.update_check_in_progress:
            return
        self.update_check_in_progress = True
        if manual:
            self.log(self.msg("update_checking"))
        threading.Thread(target=self._check_for_updates_worker, args=(manual,), daemon=True).start()

    def _check_for_updates_worker(self, manual: bool):
        try:
            latest = self._fetch_latest_release()
            latest_version = latest.get("tag") or latest.get("name") or "0"
            is_newer = self._parse_version_tuple(latest_version) > self._parse_version_tuple(APP_VERSION)
            self.ui(self._handle_update_check_result, manual, latest, is_newer, None)
        except Exception as e:
            self.ui(self._handle_update_check_result, manual, None, False, str(e))

    def _handle_update_check_result(self, manual: bool, latest, is_newer: bool, error: str | None):
        self.update_check_in_progress = False
        if error:
            if manual:
                messagebox.showerror(self.msg("update_error_title"), self.msg("update_check_failed", error=error))
            return

        latest_version = latest.get("tag") or latest.get("name") or "?"
        if not is_newer:
            if manual:
                messagebox.showinfo(self.msg("update_not_available_title"), self.msg("update_not_available_message", version=latest_version))
            return

        self.startup_update_prompt_shown = True
        if not getattr(sys, "frozen", False):
            if messagebox.askyesno(self.msg("update_available_title"), self.msg("update_source_mode_message", version=latest_version)):
                webbrowser.open(latest.get("html_url", GITHUB_RELEASES_URL))
            return

        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showwarning(self.msg("update_busy_title"), self.msg("update_busy_message"))
            return

        if messagebox.askyesno(self.msg("update_available_title"), self.msg("update_available_message", version=latest_version)):
            self._start_update_download(latest)

    def _start_update_download(self, latest):
        asset = self._find_exe_asset(latest)
        if not asset:
            messagebox.showerror(self.msg("update_error_title"), self.msg("update_no_exe_asset"))
            return
        self.log(self.msg("update_download_started"))
        threading.Thread(target=self._download_and_install_update_worker, args=(asset,), daemon=True).start()

    def _download_and_install_update_worker(self, asset):
        try:
            download_url = asset.get("browser_download_url")
            temp_dir = tempfile.gettempdir()
            download_path = os.path.join(temp_dir, f"loudnorm_update_{int(time.time())}.exe")
            req = urllib.request.Request(
                download_url,
                headers={"User-Agent": f"Loudnorm-PRO/{APP_VERSION}"},
            )
            with urllib.request.urlopen(req, timeout=UPDATE_TIMEOUT_SECONDS) as resp, open(download_path, "wb") as f:
                while True:
                    chunk = resp.read(1024 * 256)
                    if not chunk:
                        break
                    f.write(chunk)
            self.ui(self._finish_update_install, download_path)
        except Exception as e:
            self.ui(messagebox.showerror, self.msg("update_error_title"), self.msg("update_download_failed", error=str(e)))

    def _finish_update_install(self, download_path: str):
        if not getattr(sys, "frozen", False):
            return
        current_exe = sys.executable
        updater_bat = os.path.join(tempfile.gettempdir(), f"loudnorm_updater_{int(time.time())}.bat")
        bat = f"""@echo off
set SRC={download_path}
set DST={current_exe}
:retry
copy /Y "%SRC%" "%DST%" >nul 2>&1
if errorlevel 1 (
  timeout /t 1 /nobreak >nul
  goto retry
)
start "" "%DST%"
del "%SRC%" >nul 2>&1
del "%~f0"
"""
        with open(updater_bat, "w", encoding="utf-8") as f:
            f.write(bat)
        self.log(self.msg("update_download_finished"))
        subprocess.Popen(["cmd", "/c", updater_bat], creationflags=NO_WINDOW)
        self.root.after(300, self.root.destroy)

    def copy_version_to_clipboard(self):
        version_text = APP_VERSION
        try:
            self.root.clipboard_clear()
            self.root.clipboard_append(version_text)
            self.root.update_idletasks()
        except Exception:
            pass

    def open_repo_url(self):
        try:
            webbrowser.open(f"https://github.com/{GITHUB_REPO}")
        except Exception:
            pass

    def show_build_info(self):
        lang = self.get_lang_code()

        dlg = tk.Toplevel(self.root)
        dlg.title(self.msg("build_info_title"))
        dlg.configure(bg="#202020")
        dlg.transient(self.root)
        dlg.resizable(False, False)
        try:
            dlg.iconbitmap(resource_path("loudnorm_pro_icon.ico"))
        except Exception:
            pass

        container = tk.Frame(dlg, bg="#202020", padx=14, pady=14)
        container.pack(fill="both", expand=True)

        header = tk.Label(
            container,
            text="Loudnorm PRO GUI",
            bg="#202020",
            fg="#50dcff",
            font=("Segoe UI Semibold", 14),
            anchor="w",
        )
        header.grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 10))

        info_lines = [
            ("Version", APP_VERSION),
            ("Build", BUILD_DATE),
            ("Python", sys.version.split()[0]),
            ("FFmpeg", self.ffmpeg_path or self.not_found_text()),
            ("FFprobe", self.ffprobe_path or self.not_found_text()),
            ("Repo", f"https://github.com/{GITHUB_REPO}"),
            ("Data", get_data_dir()),
        ]

        row = 1
        for label, value in info_lines:
            tk.Label(
                container,
                text=f"{label}:",
                bg="#202020",
                fg="#9fdcff",
                font=("Segoe UI", 9, "bold"),
                anchor="nw",
            ).grid(row=row, column=0, sticky="nw", padx=(0, 10), pady=2)
            if label == "Repo":
                link = tk.Label(
                    container,
                    text=value,
                    bg="#202020",
                    fg="#6db7ff",
                    cursor="hand2",
                    font=("Segoe UI", 9, "underline"),
                    anchor="nw",
                    justify="left",
                    wraplength=460,
                )
                link.grid(row=row, column=1, sticky="nw", pady=2)
                link.bind("<Button-1>", lambda e: self.open_repo_url())
            else:
                tk.Label(
                    container,
                    text=value,
                    bg="#202020",
                    fg="white",
                    font=("Segoe UI", 9),
                    anchor="nw",
                    justify="left",
                    wraplength=460,
                ).grid(row=row, column=1, sticky="nw", pady=2)
            row += 1

        tk.Frame(container, bg="#404040", height=1).grid(row=row, column=0, columnspan=2, sticky="ew", pady=(10, 10))
        row += 1

        tk.Label(
            container,
            text=self.msg("license_notice"),
            bg="#202020",
            fg="#9fdcff",
            font=("Segoe UI", 9, "bold"),
            anchor="w",
        ).grid(row=row, column=0, columnspan=2, sticky="w")
        row += 1

        tk.Label(
            container,
            text=PROJECT_LICENSE_NOTICE,
            bg="#202020",
            fg="white",
            font=("Segoe UI", 9),
            anchor="w",
            justify="left",
            wraplength=540,
        ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(2, 10))
        row += 1

        tk.Label(
            container,
            text=self.msg("third_party"),
            bg="#202020",
            fg="#9fdcff",
            font=("Segoe UI", 9, "bold"),
            anchor="w",
        ).grid(row=row, column=0, columnspan=2, sticky="w")
        row += 1

        third_party_text = f"{self.msg('ffmpeg_license_note')}\n- tkinter / tkinterdnd2"
        tk.Label(
            container,
            text=third_party_text,
            bg="#202020",
            fg="white",
            font=("Segoe UI", 9),
            anchor="w",
            justify="left",
            wraplength=540,
        ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(2, 12))
        row += 1

        buttons = tk.Frame(container, bg="#202020")
        buttons.grid(row=row, column=0, columnspan=2, sticky="e")
        buttons.grid_columnconfigure(0, weight=1)

        btn_copy = tk.Button(
            buttons,
            text=self.tr("Version kopieren"),
            command=self.copy_version_to_clipboard,
            bg="#375a7a",
            fg="white",
            activebackground="#4b6f92",
            relief="flat",
            font=("Segoe UI", 9),
            padx=10,
        )
        btn_copy.grid(row=0, column=0, padx=(0, 8), ipady=2)

        btn_repo = tk.Button(
            buttons,
            text=self.tr("GitHub öffnen"),
            command=self.open_repo_url,
            bg="#375a7a",
            fg="white",
            activebackground="#4b6f92",
            relief="flat",
            font=("Segoe UI", 9),
            padx=10,
        )
        btn_repo.grid(row=0, column=1, padx=(0, 8), ipady=2)

        btn_ok = tk.Button(
            buttons,
            text="OK",
            command=dlg.destroy,
            bg="#2f2f2f",
            fg="white",
            activebackground="#454545",
            relief="flat",
            font=("Segoe UI", 9),
            padx=14,
        )
        btn_ok.grid(row=0, column=2, ipady=2)

        container.grid_columnconfigure(1, weight=1)
        dlg.update_idletasks()

        root_x = self.root.winfo_rootx()
        root_y = self.root.winfo_rooty()
        root_w = self.root.winfo_width()
        root_h = self.root.winfo_height()
        dlg_w = dlg.winfo_reqwidth()
        dlg_h = dlg.winfo_reqheight()
        pos_x = root_x + max(20, (root_w - dlg_w) // 2)
        pos_y = root_y + max(20, (root_h - dlg_h) // 2)
        dlg.geometry(f"+{pos_x}+{pos_y}")
        dlg.grab_set()
        dlg.focus_set()


    def _scroll_left_canvas_from_event(self, event):
        try:
            if hasattr(event, "delta") and event.delta:
                steps = int(-1 * (event.delta / 120))
                if steps == 0:
                    steps = -1 if event.delta > 0 else 1
            elif getattr(event, "num", None) == 4:
                steps = -1
            elif getattr(event, "num", None) == 5:
                steps = 1
            else:
                steps = 0

            if steps:
                self.left_canvas.yview_scroll(steps, "units")
        except Exception:
            pass
        return "break"

    def _block_combobox_mousewheel(self, event):
        # Scroll the left settings pane, but do not let the combobox change its value.
        return self._scroll_left_canvas_from_event(event)

    def _on_left_panel_mousewheel(self, event):
        return self._scroll_left_canvas_from_event(event)

    def _bind_left_panel_mousewheel_to_widget(self, widget):
        try:
            if hasattr(self, "jobs_frame"):
                parent = widget
                while parent is not None:
                    if parent == self.jobs_frame:
                        return
                    parent = getattr(parent, "master", None)
            widget_class = str(widget.winfo_class())
        except Exception:
            widget_class = ""

        redirect_classes = {
            "TCombobox", "Combobox", "TSpinbox", "Spinbox",
            "Entry", "Text", "Listbox"
        }

        handler = self._block_combobox_mousewheel if widget_class in redirect_classes else self._on_left_panel_mousewheel

        try:
            widget.bind("<MouseWheel>", handler)
            widget.bind("<Button-4>", handler)
            widget.bind("<Button-5>", handler)
        except Exception:
            pass

        try:
            for child in widget.winfo_children():
                self._bind_left_panel_mousewheel_to_widget(child)
        except Exception:
            pass

    def _bind_left_panel_mousewheel_targets(self):
        try:
            self._bind_left_panel_mousewheel_to_widget(self.left_col)
        except Exception:
            pass

    def replace_original_file(self, temp_output: str, final_output: str, preserve_timestamp: bool = True):
        temp_output = os.path.normpath(temp_output)
        final_output = os.path.normpath(final_output)

        if not os.path.exists(temp_output):
            raise FileNotFoundError(temp_output)
        if os.path.normcase(temp_output) == os.path.normcase(final_output):
            raise OSError("Temporary output path must differ from final output path.")

        target_dir = os.path.dirname(final_output) or "."
        os.makedirs(target_dir, exist_ok=True)

        original_times = None
        if os.path.exists(final_output):
            try:
                original_times = get_path_timestamps(final_output)
            except Exception:
                original_times = None

        temp_drive = os.path.splitdrive(temp_output)[0].lower()
        final_drive = os.path.splitdrive(final_output)[0].lower()

        if temp_drive == final_drive:
            os.replace(temp_output, final_output)
        else:
            swap_name = f".__replace_tmp__{os.getpid()}_{threading.get_ident()}_{int(time.time() * 1000)}_{os.path.basename(final_output)}"
            swap_path = os.path.join(target_dir, swap_name)

            try:
                shutil.copyfile(temp_output, swap_path)
                os.replace(swap_path, final_output)
            except Exception:
                try:
                    if os.path.exists(swap_path):
                        os.remove(swap_path)
                except Exception:
                    pass
                raise
            finally:
                try:
                    if os.path.exists(temp_output):
                        os.remove(temp_output)
                except Exception:
                    pass

        try:
            now = time.time()
            if preserve_timestamp and original_times:
                set_path_timestamps(
                    final_output,
                    created=original_times.get("created"),
                    accessed=original_times.get("accessed"),
                    modified=original_times.get("modified"),
                )
            else:
                set_path_timestamps(final_output, created=now, accessed=now, modified=now)
        except Exception:
            pass

    def update_overwrite_ui(self):
        overwrite = bool(self.overwrite_original_var.get())
        self.overwrite_hint_var.set(self.msg("overwrite_hint") if overwrite else "")
        self.overwrite_warning_var.set("AUF EIGENE GEFAHR!" if overwrite and self.get_lang_code() == "de" else "USE AT YOUR OWN RISK!" if overwrite else "")

        try:
            self.chk_preserve_timestamp.config(state=("normal" if overwrite else "disabled"))
        except Exception:
            pass

        try:
            self.output_label.grid()
            self.out_row.grid()
            self.output_entry.config(state="normal")
            self.btn_browse_out.config(state="normal")
        except Exception:
            pass

    def _build_left_column(self):
        self.source_frame = tk.LabelFrame(
            self.left_source_holder,
            text="Quelle",
            bg="#202020",
            fg="#9fdcff",
            font=("Segoe UI Semibold", 10),
            bd=1,
        )
        self.source_frame.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.source_frame.grid_columnconfigure(0, weight=1)

        top_bar = tk.Frame(self.source_frame, bg="#202020")
        top_bar.grid(row=0, column=0, sticky="ew", padx=6, pady=(4, 4))
        top_bar.grid_columnconfigure(10, weight=1)

        self.rb_folder = tk.Radiobutton(
            top_bar,
            text="Ordner",
            variable=self.source_mode_var,
            value="folder",
            command=self.on_source_mode_changed,
            bg="#202020",
            fg="white",
            selectcolor="#303030",
            activebackground="#202020",
            activeforeground="white",
            font=("Segoe UI", 9),
        )
        self.rb_folder.grid(row=0, column=0, sticky="w")

        self.rb_files = tk.Radiobutton(
            top_bar,
            text="Einzeldateien",
            variable=self.source_mode_var,
            value="files",
            command=self.on_source_mode_changed,
            bg="#202020",
            fg="white",
            selectcolor="#303030",
            activebackground="#202020",
            activeforeground="white",
            font=("Segoe UI", 9),
        )
        self.rb_files.grid(row=0, column=1, sticky="w", padx=(8, 0))

        self.dnd_hint_var = tk.StringVar(
            value="Drag & Drop aktiv" if DND_AVAILABLE else "Drag & Drop aus"
        )
        tk.Label(
            top_bar,
            textvariable=self.dnd_hint_var,
            bg="#202020",
            fg="#9fdcff" if DND_AVAILABLE else "#ffb070",
            font=("Segoe UI", 9),
        ).grid(row=0, column=10, sticky="e")

        self.folder_frame = tk.Frame(self.source_frame, bg="#202020")
        self.folder_frame.grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 5))
        self.folder_frame.grid_columnconfigure(0, weight=1)

        tk.Label(
            self.folder_frame,
            text="Eingabeordner",
            bg="#202020",
            fg="white",
            font=("Segoe UI", 9),
        ).grid(row=0, column=0, sticky="w", pady=(0, 2))

        folder_input_row = tk.Frame(self.folder_frame, bg="#202020")
        folder_input_row.grid(row=1, column=0, sticky="ew")
        folder_input_row.grid_columnconfigure(0, weight=1)

        self.input_entry = tk.Entry(
            folder_input_row,
            textvariable=self.input_var,
            bg="#2d2d2d",
            fg="white",
            insertbackground="white",
            relief="solid",
            bd=1,
            font=("Segoe UI", 10),
        )
        self.input_entry.grid(row=0, column=0, sticky="ew", padx=(0, 6), ipady=3)

        self.btn_browse_in = tk.Button(
            folder_input_row,
            text="Durchsuchen",
            command=self.browse_input,
            bg="#373737",
            fg="white",
            activebackground="#4a4a4a",
            relief="flat",
            font=("Segoe UI", 9),
            width=10,
        )
        self.btn_browse_in.grid(row=0, column=1, sticky="e", ipadx=4, ipady=2)

        self.files_frame = tk.Frame(self.source_frame, bg="#202020")
        self.files_frame.grid(row=2, column=0, sticky="ew", padx=6, pady=(0, 6))
        self.files_frame.grid_columnconfigure(0, weight=1)
        self.files_frame.grid_rowconfigure(1, weight=1)

        files_btn_bar = tk.Frame(self.files_frame, bg="#202020")
        files_btn_bar.grid(row=0, column=0, sticky="ew", pady=(0, 4))
        files_btn_bar.grid_columnconfigure(4, weight=1)

        self.btn_add_files = tk.Button(
            files_btn_bar,
            text="Dateien",
            command=self.add_files_dialog,
            bg="#373737",
            fg="white",
            activebackground="#4a4a4a",
            relief="flat",
            font=("Segoe UI", 9),
        )
        self.btn_add_files.grid(row=0, column=0, padx=(0, 4), ipadx=4, ipady=2)

        self.btn_add_folder = tk.Button(
            files_btn_bar,
            text="Ordner",
            command=self.add_folder_dialog,
            bg="#373737",
            fg="white",
            activebackground="#4a4a4a",
            relief="flat",
            font=("Segoe UI", 9),
        )
        self.btn_add_folder.grid(row=0, column=1, padx=(0, 4), ipadx=4, ipady=2)

        self.btn_remove_selected = tk.Button(
            files_btn_bar,
            text="Entfernen",
            command=self.remove_selected_files,
            bg="#5b3a3a",
            fg="white",
            activebackground="#734545",
            relief="flat",
            font=("Segoe UI", 9),
        )
        self.btn_remove_selected.grid(row=0, column=2, padx=(0, 4), ipadx=4, ipady=2)

        self.btn_clear_files = tk.Button(
            files_btn_bar,
            text="Leeren",
            command=self.clear_file_list,
            bg="#5b3a3a",
            fg="white",
            activebackground="#734545",
            relief="flat",
            font=("Segoe UI", 9),
        )
        self.btn_clear_files.grid(row=0, column=3, padx=(0, 4), ipadx=4, ipady=2)

        self.file_count_var = tk.StringVar(value="0 Dateien")
        tk.Label(
            files_btn_bar,
            textvariable=self.file_count_var,
            bg="#202020",
            fg="#9fdcff",
            font=("Segoe UI", 9),
        ).grid(row=0, column=4, sticky="e")

        list_wrap = tk.Frame(self.files_frame, bg="#202020")
        list_wrap.grid(row=1, column=0, sticky="ew")
        list_wrap.grid_rowconfigure(0, weight=1)
        list_wrap.grid_columnconfigure(0, weight=1)

        self.file_listbox = tk.Listbox(
            list_wrap,
            selectmode=tk.EXTENDED,
            bg="#141414",
            fg="#dcdcdc",
            selectbackground="#2d4f6c",
            selectforeground="white",
            font=("Consolas", 9),
            relief="solid",
            bd=1,
            height=4,
        )
        self.file_listbox.grid(row=0, column=0, sticky="ew")

        self.file_list_scroll = tk.Scrollbar(list_wrap, command=self.file_listbox.yview)
        self.file_list_scroll.grid(row=0, column=1, sticky="ns")
        self.file_listbox.configure(yscrollcommand=self.file_list_scroll.set)

        self.drop_hint_var = tk.StringVar(
            value="Dateien oder Ordner hier hineinziehen" if DND_AVAILABLE else "Buttons zum Hinzufügen verwenden"
        )
        tk.Label(
            self.files_frame,
            textvariable=self.drop_hint_var,
            bg="#202020",
            fg="#ffd278",
            anchor="w",
            font=("Segoe UI", 9),
        ).grid(row=2, column=0, sticky="ew", pady=(4, 0))

        self.control_frame = tk.Frame(
            self.left_col,
            bg="#202020",
            bd=0,
            highlightthickness=0,
        )
        self.control_frame.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.control_frame.grid_columnconfigure(0, weight=1)

        self.output_label = tk.Label(
            self.control_frame,
            text="Ausgabeordner",
            bg="#202020",
            fg="white",
            font=("Segoe UI", 9),
        )
        self.output_label.grid(row=0, column=0, sticky="w", padx=6, pady=(4, 2))

        self.out_row = tk.Frame(self.control_frame, bg="#202020")
        self.out_row.grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 5))
        self.out_row.grid_columnconfigure(0, weight=1)

        self.output_entry = tk.Entry(
            self.out_row,
            textvariable=self.output_var,
            bg="#2d2d2d",
            fg="white",
            insertbackground="white",
            relief="solid",
            bd=1,
            font=("Segoe UI", 10),
        )
        self.output_entry.grid(row=0, column=0, sticky="ew", padx=(0, 6), ipady=3)

        self.btn_browse_out = tk.Button(
            self.out_row,
            text="Durchsuchen",
            command=self.browse_output,
            bg="#373737",
            fg="white",
            activebackground="#4a4a4a",
            relief="flat",
            font=("Segoe UI", 9),
            width=10,
        )
        self.btn_browse_out.grid(row=0, column=1, sticky="e", ipadx=4, ipady=2)

        tk.Label(self.control_frame, text="Audio", bg="#202020", fg="white", font=("Segoe UI", 9)).grid(row=2, column=0, sticky="w", padx=6)
        self.audio_combo = ttk.Combobox(
            self.control_frame,
            textvariable=self.audio_var,
            values=["AAC", "E-AC3"],
            state="readonly",
            font=("Segoe UI", 10),
        )
        self.audio_combo.grid(row=3, column=0, sticky="ew", padx=6, pady=(0, 2))

        tk.Label(self.control_frame, text="Audio-Bitrate", bg="#202020", fg="white", font=("Segoe UI", 9)).grid(row=4, column=0, sticky="w", padx=6)
        self.audio_bitrate_combo = ttk.Combobox(
            self.control_frame,
            textvariable=self.audio_bitrate_var,
            values=["128k", "192k", "256k", "320k", "384k"],
            state="readonly",
            font=("Segoe UI", 10),
        )
        self.audio_bitrate_combo.grid(row=5, column=0, sticky="ew", padx=6, pady=(0, 4))

        tk.Label(self.control_frame, text="Normalisierung", bg="#202020", fg="white", font=("Segoe UI", 9)).grid(row=6, column=0, sticky="w", padx=6)
        self.audio_track_mode_combo = ttk.Combobox(
            self.control_frame,
            textvariable=self.audio_track_mode_var,
            values=["Auto (bevorzugte Sprache)", "Alle Spuren", "Nur bevorzugte Sprache"],
            state="readonly",
            font=("Segoe UI", 10),
        )
        self.audio_track_mode_combo.grid(row=7, column=0, sticky="ew", padx=6, pady=(0, 2))

        tk.Label(self.control_frame, text="Bevorzugte Sprache", bg="#202020", fg="white", font=("Segoe UI", 9)).grid(row=8, column=0, sticky="w", padx=6)
        self.preferred_language_combo = ttk.Combobox(
            self.control_frame,
            textvariable=self.preferred_language_var,
            values=self.get_preferred_language_options(),
            state="readonly",
            font=("Segoe UI", 10),
        )
        self.preferred_language_combo.grid(row=9, column=0, sticky="ew", padx=6, pady=(0, 2))

        tk.Label(
            self.control_frame,
            textvariable=self.audio_track_mode_hint_var,
            bg="#202020",
            fg="#ffd278",
            justify="left",
            anchor="w",
            wraplength=300,
            font=("Segoe UI", 8),
        ).grid(row=10, column=0, sticky="ew", padx=6, pady=(0, 2))

        self.chk_prefer_german_first = tk.Checkbutton(
            self.control_frame,
            text="Bevorzugte Sprache nach vorne setzen + als Default markieren",
            variable=self.prefer_german_first_var,
            bg="#202020",
            fg="white",
            selectcolor="#303030",
            activebackground="#202020",
            activeforeground="white",
            font=("Segoe UI", 9),
            anchor="w",
            justify="left",
            command=self.schedule_audio_preview_refresh,
        )
        self.chk_prefer_german_first.grid(row=11, column=0, sticky="ew", padx=6, pady=(0, 1))

        tk.Label(
            self.control_frame,
            textvariable=self.prefer_german_first_hint_var,
            bg="#202020",
            fg="#9fdcff",
            justify="left",
            anchor="w",
            wraplength=300,
            font=("Segoe UI", 8),
        ).grid(row=12, column=0, sticky="ew", padx=6, pady=(0, 2))

        tk.Label(self.control_frame, text="Video", bg="#202020", fg="white", font=("Segoe UI", 9)).grid(row=13, column=0, sticky="w", padx=6)
        self.video_combo = ttk.Combobox(
            self.control_frame,
            textvariable=self.video_var,
            values=["COPY", "HEVC NVENC"],
            state="readonly",
            font=("Segoe UI", 10),
        )
        self.video_combo.grid(row=14, column=0, sticky="ew", padx=6, pady=(0, 2))

        tk.Label(self.control_frame, text="Video-Preset", bg="#202020", fg="white", font=("Segoe UI", 9)).grid(row=15, column=0, sticky="w", padx=6)
        self.video_preset_combo = ttk.Combobox(
            self.control_frame,
            textvariable=self.video_preset_var,
            values=["p7 slow", "p6 slower", "p5 balanced", "p4 faster", "p3 fastest"],
            state="readonly",
            font=("Segoe UI", 10),
        )
        self.video_preset_combo.grid(row=16, column=0, sticky="ew", padx=6, pady=(0, 2))

        tk.Label(self.control_frame, text="Video-Bitrate", bg="#202020", fg="white", font=("Segoe UI", 9)).grid(row=17, column=0, sticky="w", padx=6)
        self.video_bitrate_combo = ttk.Combobox(
            self.control_frame,
            textvariable=self.video_bitrate_var,
            values=["CQ 19 (quality)", "4 Mbps", "8 Mbps", "12 Mbps", "20 Mbps", "30 Mbps"],
            state="readonly",
            font=("Segoe UI", 10),
        )
        self.video_bitrate_combo.grid(row=18, column=0, sticky="ew", padx=6, pady=(0, 4))

        tk.Label(self.control_frame, text="Parallel-Jobs", bg="#202020", fg="white", font=("Segoe UI", 9)).grid(row=19, column=0, sticky="w", padx=6)
        self.jobs_combo = ttk.Combobox(
            self.control_frame,
            textvariable=self.jobs_var,
            values=["Auto", "1", "2", "3", "4", "5", "6", "7", "8"],
            state="readonly",
            font=("Segoe UI", 10),
        )
        self.jobs_combo.grid(row=20, column=0, sticky="ew", padx=6, pady=(0, 2))

        tk.Label(
            self.control_frame,
            textvariable=self.parallel_hint_var,
            bg="#202020",
            fg="#9fdcff",
            font=("Segoe UI", 8),
            anchor="w",
            justify="left",
            wraplength=300,
        ).grid(row=21, column=0, sticky="ew", padx=6, pady=(0, 6))

        self.chk_resume_jobs = tk.Checkbutton(
            self.control_frame,
            text="Unterbrochene Jobs fortsetzen",
            variable=self.resume_jobs_var,
            bg="#202020",
            fg="white",
            selectcolor="#303030",
            activebackground="#202020",
            activeforeground="white",
            font=("Segoe UI", 9),
            anchor="w",
            justify="left",
        )
        self.chk_resume_jobs.grid(row=22, column=0, sticky="ew", padx=6, pady=(4, 1))

        tk.Label(
            self.control_frame,
            textvariable=self.resume_jobs_hint_var,
            bg="#202020",
            fg="#9fdcff",
            justify="left",
            anchor="w",
            wraplength=300,
            font=("Segoe UI", 8),
        ).grid(row=23, column=0, sticky="ew", padx=6, pady=(0, 2))

        self.chk_overwrite_original = tk.Checkbutton(
            self.control_frame,
            text="Originaldateien überschreiben",
            variable=self.overwrite_original_var,
            bg="#202020",
            fg="white",
            selectcolor="#303030",
            activebackground="#202020",
            activeforeground="white",
            font=("Segoe UI", 9),
            anchor="w",
            justify="left",
            command=self.update_overwrite_ui,
        )
        self.chk_overwrite_original.grid(row=24, column=0, sticky="ew", padx=6, pady=(0, 1))

        self.chk_preserve_timestamp = tk.Checkbutton(
            self.control_frame,
            text="Ursprünglichen Timestamp beibehalten",
            variable=self.preserve_timestamp_var,
            bg="#202020",
            fg="white",
            selectcolor="#303030",
            activebackground="#202020",
            activeforeground="white",
            font=("Segoe UI", 9),
            anchor="w",
            justify="left",
        )
        self.chk_preserve_timestamp.grid(row=25, column=0, sticky="ew", padx=18, pady=(0, 1))

        tk.Label(
            self.control_frame,
            textvariable=self.overwrite_warning_var,
            bg="#202020",
            fg="#ff8080",
            justify="left",
            anchor="w",
            wraplength=300,
            font=("Segoe UI", 8, "bold"),
        ).grid(row=26, column=0, sticky="ew", padx=18, pady=(0, 1))

        tk.Label(
            self.control_frame,
            textvariable=self.overwrite_hint_var,
            bg="#202020",
            fg="#ffb070",
            justify="left",
            anchor="w",
            wraplength=300,
            font=("Segoe UI", 8),
        ).grid(row=27, column=0, sticky="ew", padx=6, pady=(0, 1))

        actions = tk.Frame(self.left_actions_holder, bg="#202020")
        actions.grid(row=0, column=0, sticky="ew", padx=6, pady=(0, 6))
        actions.grid_columnconfigure(0, weight=1)
        actions.grid_columnconfigure(1, weight=1)
        actions.grid_columnconfigure(2, weight=1)

        self.btn_start = tk.Button(
            actions,
            text="Start",
            command=self.start_processing,
            bg="#2f6f3e",
            fg="white",
            activebackground="#2f6f3e",
            relief="flat",
            font=("Segoe UI", 9),
        )
        self.btn_start.grid(row=0, column=0, sticky="ew", padx=(0, 4), ipady=3)

        self.btn_cancel = tk.Button(
            actions,
            text="Abbrechen",
            command=self.request_cancel,
            bg="#6a5120",
            fg="white",
            activebackground="#6a5120",
            relief="flat",
            font=("Segoe UI", 9),
        )
        self.btn_cancel.grid(row=0, column=1, sticky="ew", padx=(0, 4), ipady=3)
        self.btn_cancel.config(state="disabled")

        self.btn_close = tk.Button(
            actions,
            text="Schliessen",
            command=self.on_close,
            bg="#2f2f2f",
            fg="white",
            activebackground="#2f2f2f",
            relief="flat",
            font=("Segoe UI", 9),
        )
        self.btn_close.grid(row=0, column=2, sticky="ew", ipady=3)

        self.tools_frame = tk.LabelFrame(
            self.left_tools_holder,
            text="Tools",
            bg="#202020",
            fg="#9fdcff",
            font=("Segoe UI Semibold", 10),
            bd=1,
        )
        self.tools_frame.grid(row=0, column=0, sticky="ew")
        self.tools_frame.grid_columnconfigure(1, weight=1)

        self.ffmpeg_used_var = tk.StringVar(value=self.ffmpeg_path or self.not_found_text())
        self.ffprobe_used_var = tk.StringVar(value=self.ffprobe_path or self.not_found_text())

        tk.Label(self.tools_frame, text="ffmpeg", bg="#202020", fg="#9fdcff", font=("Segoe UI Semibold", 9)).grid(row=0, column=0, sticky="w", padx=6, pady=(4, 2))
        tk.Label(
            self.tools_frame,
            textvariable=self.ffmpeg_used_var,
            bg="#202020",
            fg="#dcdcdc",
            font=("Consolas", 8),
            anchor="w",
            justify="left",
            wraplength=300,
        ).grid(row=0, column=1, sticky="ew", padx=(0, 6), pady=(4, 2))

        tk.Label(self.tools_frame, text="ffprobe", bg="#202020", fg="#9fdcff", font=("Segoe UI Semibold", 9)).grid(row=1, column=0, sticky="w", padx=6, pady=(2, 6))
        tk.Label(
            self.tools_frame,
            textvariable=self.ffprobe_used_var,
            bg="#202020",
            fg="#dcdcdc",
            font=("Consolas", 8),
            anchor="w",
            justify="left",
            wraplength=300,
        ).grid(row=1, column=1, sticky="ew", padx=(0, 6), pady=(2, 6))



    def _on_jobs_mousewheel(self, event):
        try:
            if hasattr(event, "delta") and event.delta:
                steps = int(-1 * (event.delta / 120))
                if steps == 0:
                    steps = -1 if event.delta > 0 else 1
            elif getattr(event, "num", None) == 4:
                steps = -1
            elif getattr(event, "num", None) == 5:
                steps = 1
            else:
                steps = 0

            if steps:
                self.jobs_canvas.yview_scroll(steps, "units")
                return "break"
        except Exception:
            pass
        return None

    def _bind_jobs_mousewheel_widget(self, widget):
        try:
            widget.bind("<MouseWheel>", self._on_jobs_mousewheel, add="+")
            widget.bind("<Button-4>", self._on_jobs_mousewheel, add="+")
            widget.bind("<Button-5>", self._on_jobs_mousewheel, add="+")
        except Exception:
            pass
        try:
            for child in widget.winfo_children():
                self._bind_jobs_mousewheel_widget(child)
        except Exception:
            pass

    def _bind_jobs_mousewheel(self):
        try:
            self._bind_jobs_mousewheel_widget(self.jobs_frame)
        except Exception:
            pass

    def _build_right_column(self):
        self.progress_frame = tk.LabelFrame(
            self.right_col,
            text="Fortschritt",
            bg="#202020",
            fg="#9fdcff",
            font=("Segoe UI Semibold", 10),
            bd=1,
        )
        self.progress_frame.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.progress_frame.grid_columnconfigure(0, weight=1)

        self.progress_total = ttk.Progressbar(self.progress_frame, maximum=100, mode="determinate")
        self.progress_total.grid(row=0, column=0, sticky="ew", padx=6, pady=(4, 3))

        info = tk.Frame(self.progress_frame, bg="#202020")
        info.grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 4))
        info.grid_columnconfigure(0, weight=1)

        self.lbl_progress = tk.Label(info, text="Bereit", bg="#202020", fg="white", font=("Segoe UI", 9))
        self.lbl_progress.grid(row=0, column=0, sticky="w")

        self.lbl_percent = tk.Label(info, text="0 %", bg="#202020", fg="#78ff78", font=("Segoe UI Semibold", 9))
        self.lbl_percent.grid(row=0, column=1, sticky="e", padx=(8, 14))

        self.lbl_eta = tk.Label(info, text="ETA: --", bg="#202020", fg="#9fdcff", font=("Segoe UI Semibold", 9))
        self.lbl_eta.grid(row=0, column=2, sticky="e")

        self.preview_header = tk.Frame(self.right_col, bg="#202020")
        self.preview_header.grid(row=1, column=0, sticky="ew", pady=(0, 4))
        self.preview_header.grid_columnconfigure(0, weight=1)

        tk.Label(
            self.preview_header,
            text="Audio-Track Vorschau",
            bg="#202020",
            fg="#9fdcff",
            font=("Segoe UI Semibold", 10),
            anchor="w",
        ).grid(row=0, column=0, sticky="w")

        self.btn_toggle_preview = tk.Button(
            self.preview_header,
            text="Vorschau ausblenden",
            command=self.toggle_audio_preview,
            bg="#373737",
            fg="white",
            activebackground="#4a4a4a",
            relief="flat",
            font=("Segoe UI", 8),
            padx=8,
            pady=1,
        )
        self.btn_toggle_preview.grid(row=0, column=1, sticky="e")

        self.preview_frame = tk.LabelFrame(
            self.right_col,
            text="",
            bg="#202020",
            fg="#9fdcff",
            font=("Segoe UI Semibold", 10),
            bd=1,
        )
        self.preview_frame.grid(row=2, column=0, sticky="nsew", pady=(0, 6))
        self.preview_frame.grid_columnconfigure(0, weight=1)
        self.preview_frame.grid_rowconfigure(2, weight=1)
        self.preview_frame.grid_propagate(False)
        self.right_col.grid_rowconfigure(2, minsize=self.calculate_preview_frame_height(PREVIEW_MIN_ROWS))

        tk.Label(
            self.preview_frame,
            textvariable=self.audio_preview_file_var,
            bg="#202020",
            fg="white",
            anchor="w",
            justify="left",
            wraplength=720,
            font=("Segoe UI", 9),
        ).grid(row=0, column=0, sticky="ew", padx=6, pady=(4, 1))

        tk.Label(
            self.preview_frame,
            textvariable=self.audio_preview_status_var,
            bg="#202020",
            fg="#ffd278",
            anchor="w",
            justify="left",
            wraplength=720,
            font=("Segoe UI", 8),
        ).grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 4))

        preview_wrap = tk.Frame(self.preview_frame, bg="#202020")
        preview_wrap.grid(row=2, column=0, sticky="nsew", padx=6, pady=(0, 6))
        preview_wrap.grid_columnconfigure(0, weight=1)
        preview_wrap.grid_rowconfigure(0, weight=1)

        self.audio_preview_tree = ttk.Treeview(
            preview_wrap,
            columns=("out", "lang", "title", "format", "flags", "action"),
            show="headings",
            height=PREVIEW_MIN_ROWS,
        )
        self.audio_preview_tree.grid(row=0, column=0, sticky="nsew")

        self.audio_preview_tree.heading("out", text="Out")
        self.audio_preview_tree.heading("lang", text="Sprache")
        self.audio_preview_tree.heading("title", text="Titel")
        self.audio_preview_tree.heading("format", text="Format")
        self.audio_preview_tree.heading("flags", text="Flags")
        self.audio_preview_tree.heading("action", text="Aktion")

        self.audio_preview_tree.column("out", width=48, anchor="center", stretch=False)
        self.audio_preview_tree.column("lang", width=90, anchor="w", stretch=False)
        self.audio_preview_tree.column("title", width=220, anchor="w")
        self.audio_preview_tree.column("format", width=170, anchor="w", stretch=False)
        self.audio_preview_tree.column("flags", width=120, anchor="w", stretch=False)
        self.audio_preview_tree.column("action", width=170, anchor="w", stretch=False)

        preview_scroll = tk.Scrollbar(preview_wrap, command=self.audio_preview_tree.yview)
        preview_scroll.grid(row=0, column=1, sticky="ns")
        self.audio_preview_tree.configure(yscrollcommand=preview_scroll.set)

        self.audio_preview_tree.tag_configure("normalize", background="#213828", foreground="#d9ffd9")
        self.audio_preview_tree.tag_configure("copy", background="#202020", foreground="#dcdcdc")
        self.audio_preview_tree.tag_configure("default", background="#24384a", foreground="#e2f1ff")

        self.paned = tk.PanedWindow(
            self.right_col,
            orient=tk.VERTICAL,
            sashwidth=6,
            sashrelief="flat",
            bg="#202020",
            bd=0,
            relief="flat",
        )
        self.paned.grid(row=3, column=0, sticky="nsew")
        self.right_col.grid_rowconfigure(3, weight=1)

        self.jobs_frame = tk.LabelFrame(
            self.paned,
            text="Aktive Jobs",
            bg="#202020",
            fg="#9fdcff",
            font=("Segoe UI Semibold", 10),
            bd=1,
        )
        self.jobs_frame.grid_rowconfigure(0, weight=1)
        self.jobs_frame.grid_columnconfigure(0, weight=1)

        jobs_wrap = tk.Frame(self.jobs_frame, bg="#202020")
        jobs_wrap.grid(row=0, column=0, sticky="nsew", padx=4, pady=(2, 4))
        jobs_wrap.grid_rowconfigure(0, weight=1)
        jobs_wrap.grid_columnconfigure(0, weight=1)

        self.jobs_canvas = tk.Canvas(
            jobs_wrap,
            bg="#202020",
            highlightthickness=0,
            bd=0,
            yscrollincrement=20,
        )
        self.jobs_canvas.grid(row=0, column=0, sticky="nsew")

        self.jobs_scrollbar = tk.Scrollbar(jobs_wrap, orient="vertical", command=self.jobs_canvas.yview)
        self.jobs_scrollbar.grid(row=0, column=1, sticky="ns")
        self.jobs_canvas.configure(yscrollcommand=self.jobs_scrollbar.set)

        self.jobs_inner = tk.Frame(self.jobs_canvas, bg="#202020")
        self.jobs_inner.grid_columnconfigure(0, weight=1)
        self.jobs_canvas_window = self.jobs_canvas.create_window((0, 0), window=self.jobs_inner, anchor="nw")

        self.jobs_inner.bind("<Configure>", self._on_jobs_inner_configure)
        self.jobs_canvas.bind("<Configure>", self._on_jobs_canvas_configure)

        self.no_active_jobs_label = tk.Label(
            self.jobs_inner,
            text=self.msg("no_active_jobs"),
            bg="#202020",
            fg="#9a9a9a",
            font=("Segoe UI", 9, "italic"),
            anchor="w",
        )
        self.no_active_jobs_label.grid(row=0, column=0, sticky="ew", padx=6, pady=8)

        self.job_rows = []
        for idx in range(MAX_JOB_ROWS):
            row = tk.Frame(self.jobs_inner, bg="#202020", highlightbackground="#3a3a3a", highlightthickness=1)
            row.grid(row=idx, column=0, sticky="ew", padx=4, pady=(4 if idx == 0 else 0, 4))
            row.grid_columnconfigure(1, weight=1)

            lbl_job = tk.Label(row, text=f"Job {idx + 1}", bg="#202020", fg="#9fdcff", font=("Segoe UI Semibold", 9))
            lbl_job.grid(row=0, column=0, sticky="w", padx=6, pady=(3, 0))

            lbl_name = tk.Label(
                row,
                text=self.msg("waiting"),
                bg="#202020",
                fg="white",
                font=("Segoe UI", 9),
                anchor="w",
            )
            lbl_name.grid(row=0, column=1, sticky="ew", padx=(0, 6), pady=(3, 0))

            lbl_pct = tk.Label(row, text="0 %", bg="#202020", fg="#ffd278", font=("Segoe UI Semibold", 9))
            lbl_pct.grid(row=0, column=2, sticky="e", padx=6, pady=(3, 0))

            lbl_phase = tk.Label(row, text="-", bg="#202020", fg="#ffd278", font=("Segoe UI", 8), anchor="w")
            lbl_phase.grid(row=1, column=1, sticky="ew", padx=(0, 6), pady=(0, 1))

            lbl_eta = tk.Label(row, text="ETA --", bg="#202020", fg="#9fdcff", font=("Segoe UI", 8), anchor="e")
            lbl_eta.grid(row=1, column=2, sticky="e", padx=6, pady=(0, 1))

            pbar = ttk.Progressbar(row, maximum=100, mode="determinate")
            pbar.grid(row=2, column=0, columnspan=3, sticky="ew", padx=6, pady=(0, 4), ipady=1)

            self.job_rows.append(
                {
                    "frame": row,
                    "job_label": lbl_job,
                    "name_label": lbl_name,
                    "phase_label": lbl_phase,
                    "eta_label": lbl_eta,
                    "progress": pbar,
                    "pct_label": lbl_pct,
                    "current_name": "",
                    "started_ts": None,
                    "phase_name": None,
                    "phase_started_ts": None,
                }
            )

        self.log_frame = tk.LabelFrame(
            self.paned,
            text="Log",
            bg="#202020",
            fg="#9fdcff",
            font=("Segoe UI Semibold", 10),
            bd=1,
        )
        self.log_frame.grid_rowconfigure(0, weight=1)
        self.log_frame.grid_columnconfigure(0, weight=1)

        self.log_text = tk.Text(
            self.log_frame,
            bg="#141414",
            fg="#dcdcdc",
            insertbackground="white",
            font=("Consolas", 9),
            wrap="word",
        )
        self.log_text.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
        self.log_text.config(state="disabled")

        scroll = tk.Scrollbar(self.log_frame, command=self.log_text.yview)
        scroll.grid(row=0, column=1, sticky="ns", pady=6)
        self.log_text.configure(yscrollcommand=scroll.set)

        self._bind_jobs_mousewheel()

        self.paned.add(self.jobs_frame, minsize=130, height=self.calculate_jobs_frame_height(self.get_parallel_jobs()))
        self.paned.add(self.log_frame, minsize=150)

        self.update_audio_bitrate_ui()
        self.update_video_options_ui()
        self.update_overwrite_ui()
        self.update_preview_toggle_ui()


    def get_audio_bitrate_options(self):
        if self.audio_var.get() == "E-AC3":
            return ["192k", "256k", "384k", "448k", "640k"]
        return ["128k", "192k", "256k", "320k", "384k"]

    def update_audio_bitrate_ui(self):
        options = self.get_audio_bitrate_options()
        try:
            self.audio_bitrate_combo.configure(values=options)
        except Exception:
            return
        if self.audio_bitrate_var.get() not in options:
            self.audio_bitrate_var.set(options[-1] if self.audio_var.get() == "AAC" else "640k")

    def update_video_options_ui(self):
        is_nvenc = self.video_var.get() == "HEVC NVENC"
        preset_state = "readonly" if is_nvenc else "disabled"
        bitrate_state = "readonly" if is_nvenc else "disabled"
        try:
            self.video_preset_combo.config(state=preset_state)
            self.video_bitrate_combo.config(state=bitrate_state)
        except Exception:
            pass

    def _bind_events(self):
        self.video_combo.bind("<<ComboboxSelected>>", lambda e: self.on_video_changed())
        self.audio_combo.bind("<<ComboboxSelected>>", lambda e: self.on_audio_codec_changed())
        self.preferred_language_combo.bind("<<ComboboxSelected>>", lambda e: self.on_preferred_language_changed())
        self.jobs_combo.bind("<<ComboboxSelected>>", lambda e: self.on_jobs_changed())
        self.audio_track_mode_combo.bind("<<ComboboxSelected>>", lambda e: self.on_audio_track_mode_changed())
        self.language_combo.bind("<<ComboboxSelected>>", lambda e: self.on_language_changed())
        self.file_listbox.bind("<<ListboxSelect>>", lambda e: self.schedule_audio_preview_refresh())
        self.input_var.trace_add("write", lambda *args: self.schedule_audio_preview_refresh())
        self.language_var.trace_add("write", lambda *args: self.on_language_changed())

    def _enable_drag_drop(self):
        widgets = [
            self.root,
            self.main,
            self.source_frame,
            self.folder_frame,
            self.files_frame,
            self.file_listbox,
        ]
        for w in widgets:
            try:
                w.drop_target_register(DND_FILES)
                w.dnd_bind("<<Drop>>", self.on_drop_files)
            except Exception:
                pass

    def on_drop_files(self, event):
        paths = parse_dnd_files(event.data)
        if not paths:
            return

        files_to_add = []
        for p in paths:
            if os.path.isdir(p):
                files_to_add.extend(collect_videos_from_folder(p))
            elif os.path.isfile(p) and is_video_file(p):
                files_to_add.append(p)

        added = self.add_files_to_list(files_to_add)
        if added > 0:
            self.source_mode_var.set("files")
            self.on_source_mode_changed()
            self.log(f"{self.msg('dragdrop_prefix')}: {added} {('Datei(en) hinzugefügt.' if self.get_lang_code() == 'de' else 'file(s) added.')}")
        else:
            self.log(f"{self.msg('dragdrop_prefix')}: " + ("keine passenden Videodateien gefunden." if self.get_lang_code() == "de" else "no matching video files found."))

    def on_source_mode_changed(self):
        mode = self.source_mode_var.get()
        if mode == "folder":
            self.folder_frame.grid()
            self.files_frame.grid_remove()
        else:
            self.folder_frame.grid_remove()
            self.files_frame.grid()
        self.schedule_audio_preview_refresh(force_probe=True)

    def browse_input(self):
        start_dir = self.input_var.get().strip()
        if not start_dir or not os.path.isdir(start_dir):
            start_dir = get_app_dir()
        path = filedialog.askdirectory(initialdir=start_dir)
        if path:
            self.input_var.set(path)
            self.schedule_audio_preview_refresh(force_probe=True)


    def get_effective_temp_work_dir(self, input_file: str = "") -> str:
        custom_dir = sanitize_windows_config_path(self.temp_work_dir_var.get())

        if not custom_dir:
            if bool(self.overwrite_original_var.get()):
                raise RuntimeError("Temp work dir is required when overwrite mode is enabled.")
            return get_data_dir()

        try:
            custom_dir = os.path.normpath(custom_dir)
            os.makedirs(custom_dir, exist_ok=True)
        except Exception as e:
            raise RuntimeError(f"Invalid temp work dir: {custom_dir}. Use forward slashes in loudnorm_settings.json, e.g. H:/testvideo_temp") from e

        if not os.path.isdir(custom_dir):
            raise RuntimeError(f"Invalid temp work dir: {custom_dir}. Use forward slashes in loudnorm_settings.json, e.g. H:/testvideo_temp")

        probe_path = ""
        try:
            fd, probe_path = tempfile.mkstemp(prefix=".write_test_", suffix=".tmp", dir=custom_dir)
            os.close(fd)
            with open(probe_path, "w", encoding="utf-8") as f:
                f.write("ok")
        except Exception as e:
            raise RuntimeError(f"Temp work dir is not writable: {custom_dir}") from e
        finally:
            if probe_path:
                try:
                    os.remove(probe_path)
                except FileNotFoundError:
                    pass
                except Exception:
                    pass

        return custom_dir

    def get_selected_audio_stream_indices(self, audio_stream_info):
        key = self.get_audio_track_mode_key()
        if not audio_stream_info:
            return []

        if key == "all":
            return list(range(len(audio_stream_info)))

        preferred_indices = self.get_preferred_audio_stream_indices(audio_stream_info)

        if key == "preferred_only":
            return preferred_indices

        if preferred_indices:
            return [preferred_indices[0]]

        return [0]

    def get_output_audio_order(self, audio_stream_info):
        base_order = list(range(len(audio_stream_info)))
        if self.get_audio_track_mode_key() != "preferred_only":
            return base_order

        preferred_indices = self.get_preferred_audio_stream_indices(audio_stream_info)
        if not preferred_indices or not self.prefer_german_first_var.get():
            return base_order

        rest = [idx for idx in base_order if idx not in set(preferred_indices)]
        return preferred_indices + rest

    def get_default_output_audio_index(self, audio_stream_info, output_audio_order):
        key = self.get_audio_track_mode_key()
        if key in {"auto", "preferred_only"}:
            preferred_indices = self.get_preferred_audio_stream_indices(audio_stream_info)
            if preferred_indices:
                first_input_index = preferred_indices[0]
                for out_idx, input_idx in enumerate(output_audio_order):
                    if input_idx == first_input_index:
                        return out_idx

        for out_idx, input_idx in enumerate(output_audio_order):
            info = audio_stream_info[input_idx]
            if int(info.get("default", 0) or 0):
                return out_idx

        return 0 if output_audio_order else None

    def get_audio_mode_log_text(self):
        key = self.get_audio_track_mode_key()
        lang = self.get_lang_code()
        pref_label = self.get_preferred_language_display(self.get_preferred_language_key())
        if key == "all":
            return "alle Audiospuren normalisieren (langsamer)" if lang == "de" else "normalize all audio tracks (slower)"
        if key == "preferred_only":
            if self.prefer_german_first_var.get():
                return (
                    f"nur {pref_label}-Spur(en) normalisieren, nach vorne + als Default (mittlere Geschwindigkeit)"
                    if lang == "de"
                    else (f"normalize {pref_label} track(s) only, move first match + default (medium speed)" if pref_label.lower() != "first track" else "normalize the first audio track only, move it first + set default (medium speed)")
                )
            return (
                f"nur {pref_label}-Spur(en) normalisieren (mittlere Geschwindigkeit)"
                if lang == "de"
                else (f"normalize {pref_label} track(s) only (medium speed)" if pref_label.lower() != "first track" else "normalize the first audio track only (medium speed)")
            )
        return (
            f"Auto-Modus: {pref_label}-Spur bevorzugen, sonst erste Spur normalisieren (schnellste Option)"
            if lang == "de"
            else (f"Auto mode: normalize the first matching {pref_label} track, otherwise the first audio track (fastest option)" if pref_label.lower() != "first track" else "Auto mode: normalize the first audio track (fastest option)")
        )

    def format_audio_stream_language(self, info):
        lang = (info.get("language") or "").strip()
        return lang if lang else "-"

    def format_audio_stream_title(self, info):
        title = (info.get("title") or "").strip()
        return title if title else "-"

    def format_audio_stream_format(self, info):
        parts = []
        codec = (info.get("codec") or "").strip()
        if codec:
            parts.append(codec)
        layout = (info.get("channel_layout") or "").strip()
        channels = info.get("channels")
        if layout:
            parts.append(layout)
        elif channels:
            parts.append(f"{channels} ch")
        sample_rate = str(info.get("sample_rate") or "").strip()
        if sample_rate.isdigit():
            parts.append(f"{sample_rate} Hz")
        return ", ".join(parts) if parts else "-"

    def get_audio_preview_target_file(self):
        if self.source_mode_var.get() == "files":
            selection = list(self.file_listbox.curselection())
            if selection:
                try:
                    return self.file_listbox.get(selection[0])
                except Exception:
                    pass
            return self.file_list[0] if self.file_list else None

        folder = self.input_var.get().strip()
        if not folder or not os.path.isdir(folder):
            return None
        return find_first_video_in_folder(folder)

    def schedule_audio_preview_refresh(self, force_probe: bool = False):
        if self.audio_preview_after_id:
            try:
                self.root.after_cancel(self.audio_preview_after_id)
            except Exception:
                pass
            self.audio_preview_after_id = None
        self.audio_preview_after_id = self.root.after(
            180,
            lambda: self.refresh_audio_preview(force_probe=force_probe),
        )

    def refresh_audio_preview(self, force_probe: bool = False):
        self.audio_preview_after_id = None
        target_file = self.get_audio_preview_target_file()
        lang = self.get_lang_code()

        if not target_file:
            self.preview_file_path = None
            self.preview_audio_info = []
            self.audio_preview_file_var.set("Keine Datei fuer Audio-Vorschau ausgewaehlt" if lang == "de" else "No file selected for audio preview")
            self.audio_preview_status_var.set("Waehle eine Datei oder einen Ordner mit Videodateien aus." if lang == "de" else "Choose a file or folder with video files.")
            self.render_audio_preview([], "")
            self.update_audio_preview_layout(0)
            return

        if not self.ffprobe_path:
            self.ffprobe_path = resolve_tool_path("ffprobe.exe")

        display_path = os.path.basename(target_file)
        self.audio_preview_file_var.set((f"Datei: {display_path}") if lang == "de" else (f"File: {display_path}"))

        if not self.ffprobe_path:
            self.audio_preview_status_var.set("ffprobe nicht gefunden. Audio-Vorschau ist nicht verfuegbar." if lang == "de" else "ffprobe not found. Audio preview unavailable.")
            self.render_audio_preview([], target_file)
            self.update_audio_preview_layout(0)
            return

        if not force_probe and target_file == self.preview_file_path and self.preview_audio_info:
            self.render_audio_preview(self.preview_audio_info, target_file)
            return

        self.audio_preview_request_id += 1
        request_id = self.audio_preview_request_id
        self.audio_preview_status_var.set("Lade Audio-Track Vorschau..." if lang == "de" else "Loading audio track preview...")

        threading.Thread(
            target=self._load_audio_preview_worker,
            args=(request_id, target_file),
            daemon=True,
        ).start()
    def _load_audio_preview_worker(self, request_id: int, target_file: str):
        info = get_audio_stream_info(self.ffprobe_path, target_file) if self.ffprobe_path else []
        self.ui(self.apply_audio_preview_result, request_id, target_file, info)

    def apply_audio_preview_result(self, request_id: int, target_file: str, audio_stream_info):
        if request_id != self.audio_preview_request_id:
            return
        self.preview_file_path = target_file
        self.preview_audio_info = audio_stream_info or []
        self.render_audio_preview(self.preview_audio_info, target_file)

    def render_audio_preview(self, audio_stream_info, target_file: str):
        for item in self.audio_preview_tree.get_children():
            self.audio_preview_tree.delete(item)

        lang = self.get_lang_code()

        if not target_file:
            self.update_audio_preview_layout(0)
            return

        if not audio_stream_info:
            self.audio_preview_status_var.set("Keine Audiospuren gefunden oder Vorschau nicht verfuegbar." if lang == "de" else "No audio streams found or preview unavailable.")
            self.update_audio_preview_layout(0)
            return

        selected_indices = set(self.get_selected_audio_stream_indices(audio_stream_info))
        output_audio_order = self.get_output_audio_order(audio_stream_info)
        default_output_idx = self.get_default_output_audio_index(audio_stream_info, output_audio_order)
        output_positions = {input_idx: out_idx for out_idx, input_idx in enumerate(output_audio_order)}

        for input_idx in output_audio_order:
            info = audio_stream_info[input_idx]
            out_idx = output_positions.get(input_idx, input_idx)
            flags = []
            if int(info.get("default", 0) or 0):
                flags.append("orig. Default" if lang == "de" else "orig. default")
            if int(info.get("forced", 0) or 0):
                flags.append("Forced")
            if default_output_idx is not None and out_idx == default_output_idx:
                flags.append("neu Default" if lang == "de" else "new default")

            action = ("Normalisieren" if lang == "de" else "Normalize") if input_idx in selected_indices else "Copy"
            tags = ["normalize" if input_idx in selected_indices else "copy"]
            if default_output_idx is not None and out_idx == default_output_idx:
                tags = ["default"]

            self.audio_preview_tree.insert(
                "",
                "end",
                values=(
                    out_idx + 1,
                    self.format_audio_stream_language(info),
                    self.format_audio_stream_title(info),
                    self.format_audio_stream_format(info),
                    ", ".join(flags) if flags else "-",
                    action,
                ),
                tags=tuple(tags),
            )

        selected_count = len(selected_indices)
        total_count = len(audio_stream_info)
        mode_text = self.get_audio_mode_log_text()
        if lang == "de":
            self.audio_preview_status_var.set(f"{mode_text} | Vorschau: {selected_count} von {total_count} Spur(en) werden normalisiert.")
        else:
            self.audio_preview_status_var.set(f"{mode_text} | Preview: {selected_count} of {total_count} track(s) will be normalized.")
        self.update_audio_preview_layout(total_count)
    def get_parallel_jobs(self) -> int:
        current = self.jobs_var.get().strip()
        if current.lower() == "auto":
            return self.detect_auto_parallel_jobs()
        try:
            return max(1, min(MAX_JOB_ROWS, int(current)))
        except Exception:
            return self.detect_auto_parallel_jobs()

    def detect_auto_analysis_jobs(self) -> int:
        cpu_count = os.cpu_count() or 4
        if self.video_var.get() == "HEVC NVENC":
            return max(3, min(MAX_JOB_ROWS, cpu_count // 2 or 3))
        return max(2, min(MAX_JOB_ROWS, cpu_count // 2 or 2))

    def get_analysis_parallel_jobs(self) -> int:
        current = self.jobs_var.get().strip()
        if current.lower() == "auto":
            return self.detect_auto_analysis_jobs()
        try:
            return max(1, min(MAX_JOB_ROWS, int(current)))
        except Exception:
            return self.detect_auto_analysis_jobs()

    def calculate_preview_frame_height(self, track_count: int) -> int:
        if not self.preview_visible_var.get():
            return 0
        rows = max(PREVIEW_MIN_ROWS, min(PREVIEW_MAX_ROWS, int(track_count) if track_count else PREVIEW_MIN_ROWS))
        return PREVIEW_BASE_HEIGHT + (rows * PREVIEW_ROW_HEIGHT)

    def update_audio_preview_layout(self, track_count: int | None = None):
        try:
            if track_count is None:
                track_count = len(self.preview_audio_info)

            rows = max(PREVIEW_MIN_ROWS, min(PREVIEW_MAX_ROWS, int(track_count) if track_count else PREVIEW_MIN_ROWS))
            desired_height = self.calculate_preview_frame_height(rows)

            self.audio_preview_tree.configure(height=rows)

            if self.preview_visible_var.get():
                self.preview_frame.configure(height=desired_height)
                self.preview_frame.grid()
                self.right_col.grid_rowconfigure(2, minsize=desired_height)
                self.preview_frame.grid_rowconfigure(2, minsize=max(96, rows * PREVIEW_ROW_HEIGHT))
            else:
                self.preview_frame.configure(height=1)
                self.right_col.grid_rowconfigure(2, minsize=1)
                self.preview_frame.grid_remove()

            self.root.update_idletasks()
        except Exception:
            pass

    def calculate_jobs_frame_height(self, active_rows: int) -> int:
        active_rows = max(0, min(MAX_JOB_ROWS, int(active_rows)))
        if active_rows <= 0:
            return JOB_FRAME_BASE_HEIGHT + 44
        visible_rows = min(active_rows, VISIBLE_JOB_ROWS)
        extra_scroll_space = 18 if active_rows > VISIBLE_JOB_ROWS else 0
        return JOB_FRAME_BASE_HEIGHT + (visible_rows * JOB_ROW_HEIGHT) + extra_scroll_space

    def update_jobs_frame_height(self, active_rows: int | None = None):
        try:
            if active_rows is None:
                active_rows = len(self.active_job_map)
            desired_height = self.calculate_jobs_frame_height(active_rows)
            self.paned.paneconfigure(self.jobs_frame, height=desired_height)
            self.root.update_idletasks()
        except Exception:
            pass

    def _on_jobs_inner_configure(self, event=None):
        try:
            self.jobs_canvas.configure(scrollregion=self.jobs_canvas.bbox("all"))
        except Exception:
            pass

    def _on_jobs_canvas_configure(self, event):
        try:
            self.jobs_canvas.itemconfigure(self.jobs_canvas_window, width=event.width)
        except Exception:
            pass

    def update_preview_toggle_ui(self):
        if self.preview_visible_var.get():
            self.btn_toggle_preview.config(text=self.tr("Vorschau ausblenden"))
        else:
            self.btn_toggle_preview.config(text=self.tr("Vorschau anzeigen"))
        self.update_audio_preview_layout(len(self.preview_audio_info))

    def toggle_audio_preview(self):
        self.preview_visible_var.set(not self.preview_visible_var.get())
        self.update_audio_bitrate_ui()
        self.update_video_options_ui()
        self.update_preview_toggle_ui()

    def update_job_rows_visibility(self):
        self.refresh_active_job_rows()

    def refresh_active_job_rows(self, active_count: int | None = None):
        with self.active_job_map_lock:
            active_indices = sorted(set(int(v) for v in self.active_job_map.values() if isinstance(v, int)))

        configured_max = max(self.get_parallel_jobs(), self.get_analysis_parallel_jobs())
        active_indices = [idx for idx in active_indices if 0 <= idx < configured_max]
        active_count = len(active_indices)

        if active_count <= 0:
            try:
                self.no_active_jobs_label.grid()
            except Exception:
                pass
        else:
            try:
                self.no_active_jobs_label.grid_remove()
            except Exception:
                pass

        visible_set = set(active_indices)
        for idx, row in enumerate(self.job_rows):
            if idx in visible_set:
                row["frame"].grid()
            else:
                row["frame"].grid_remove()
                self.clear_job_row(idx)

        self.update_jobs_frame_height(active_count)

    def set_ui_enabled(self, enabled: bool):
        state = "normal" if enabled else "disabled"

        self.btn_start.config(state=state)
        self.btn_browse_in.config(state=state)
        self.btn_browse_out.config(state=state)
        self.btn_add_files.config(state=state)
        self.btn_add_folder.config(state=state)
        self.btn_remove_selected.config(state=state)
        self.btn_clear_files.config(state=state)

        self.input_entry.config(state=state)
        self.output_entry.config(state=state)

        self.audio_combo.config(state="readonly" if enabled else "disabled")
        self.audio_track_mode_combo.config(state="readonly" if enabled else "disabled")
        self.video_combo.config(state="readonly" if enabled else "disabled")
        if enabled:
            self.update_audio_track_mode_hint()
        else:
            self.chk_prefer_german_first.config(state="disabled")

        self.rb_folder.config(state=state)
        self.rb_files.config(state=state)
        self.chk_prefer_german_first.config(state=state)
        self.chk_resume_jobs.config(state=state)
        self.chk_overwrite_original.config(state=state)

        if enabled:
            self.update_parallel_ui()
            self.update_video_options_ui()
            self.update_audio_bitrate_ui()
            self.update_overwrite_ui()
        else:
            self.jobs_combo.config(state="disabled")

        self.btn_cancel.config(state="disabled" if enabled else "normal")

    def request_cancel(self):
        self.cancel_requested = True
        self.kill_all_processes()
        self.log("")
        self.log(self.msg("cancel_requested"))

    def register_process(self, proc):
        with self.processes_lock:
            self.processes.append(proc)

    def unregister_process(self, proc):
        with self.processes_lock:
            if proc in self.processes:
                self.processes.remove(proc)

    def kill_all_processes(self):
        with self.processes_lock:
            procs = list(self.processes)
        for proc in procs:
            try:
                if proc and proc.poll() is None:
                    proc.kill()
            except Exception:
                pass

    def on_close(self):
        self.save_settings()
        self.cancel_requested = True
        self.kill_all_processes()
        self.root.destroy()

    def process_ui_queue(self):
        try:
            while True:
                fn, args = self.ui_queue.get_nowait()
                fn(*args)
        except queue.Empty:
            pass
        self.root.after(100, self.process_ui_queue)

    def ui(self, fn, *args):
        self.ui_queue.put((fn, args))

    def set_total_progress(self, done: int, total: int):
        pct = int((done / total) * 100) if total else 0
        self.progress_total["maximum"] = max(total, 1)
        self.progress_total["value"] = done
        self.lbl_progress.config(text=f"{done} / {total} fertig")
        self.lbl_percent.config(text=f"{pct} %")
        self.lbl_eta.config(text=self.calculate_eta_text(done, total))

    def calculate_eta_text(self, done: int, total: int) -> str:
        if total <= 0 or done <= 0 or done >= total:
            return "ETA: --" if done < total else "ETA: 0 min"

        samples = self.completed_times_for_eta
        if not samples:
            return "ETA: --"

        window = samples[-5:]
        avg = sum(window) / len(window)

        if len(samples) < 3 and self.run_started_ts:
            elapsed = max(1.0, time.time() - self.run_started_ts)
            global_avg = elapsed / max(1, len(samples))
            avg = (avg + global_avg) / 2.0

        remaining = total - done
        eta_seconds = avg * remaining

        parallel_jobs = self.get_parallel_jobs()
        if parallel_jobs > 1:
            eta_seconds /= parallel_jobs

        return format_eta(eta_seconds)

    def set_job_row(self, row_index: int, name: str, phase: str, pct: int):
        if 0 <= row_index < len(self.job_rows):
            row = self.job_rows[row_index]
            pct = max(0, min(100, pct))
            now = time.time()

            if row.get("current_name") != name:
                row["current_name"] = name
                row["started_ts"] = now
                row["phase_name"] = phase
                row["phase_started_ts"] = now

            if row.get("phase_name") != phase:
                row["phase_name"] = phase
                row["phase_started_ts"] = now

            row["name_label"].config(text=name)
            row["phase_label"].config(text=phase)
            try:
                row["progress"].stop()
                row["progress"].configure(mode="determinate")
            except Exception:
                pass
            row["progress"]["value"] = pct
            row["pct_label"].config(text=f"{pct} %")

            phase_started = row.get("phase_started_ts")
            if pct < 15 or not phase_started:
                row["eta_label"].config(text="ETA --")
            else:
                phase_elapsed = max(0.001, now - phase_started)
                if phase_elapsed < 20:
                    row["eta_label"].config(text="ETA --")
                else:
                    total_est = phase_elapsed / max(0.15, pct / 100.0)
                    remaining = max(0, total_est - phase_elapsed)
                    if remaining > 6 * 3600:
                        row["eta_label"].config(text="ETA --")
                    else:
                        row["eta_label"].config(text=self.msg("eta_short", eta=format_eta_short(remaining)))

    def clear_job_row(self, row_index: int):
        if 0 <= row_index < len(self.job_rows):
            row = self.job_rows[row_index]
            row["name_label"].config(text=self.msg("waiting"))
            row["phase_label"].config(text="-")
            row["eta_label"].config(text="ETA --")
            try:
                row["progress"].stop()
                row["progress"].configure(mode="determinate")
            except Exception:
                pass
            row["progress"]["value"] = 0
            row["pct_label"].config(text="0 %")
            row["current_name"] = ""
            row["started_ts"] = None
            row["phase_name"] = None
            row["phase_started_ts"] = None

    def finish_job_row(self, row_index: int, name: str, result_text: str):
        if 0 <= row_index < len(self.job_rows):
            row = self.job_rows[row_index]
            try:
                row["progress"].stop()
                row["progress"].configure(mode="determinate")
            except Exception:
                pass
            row["name_label"].config(text=name)
            row["phase_label"].config(text=result_text)
            row["eta_label"].config(text="ETA 0s")
            row["progress"]["value"] = 100
            row["pct_label"].config(text="100 %")
            row["current_name"] = name
            row["phase_name"] = result_text

    def start_job_row_activity(self, row_index: int, name: str, phase: str):
        if 0 <= row_index < len(self.job_rows):
            row = self.job_rows[row_index]
            row["name_label"].config(text=name)
            row["phase_label"].config(text=phase)
            row["pct_label"].config(text="...")
            row["eta_label"].config(text="ETA --")
            try:
                row["progress"].configure(mode="indeterminate")
                row["progress"].start(10)
            except Exception:
                pass

    def stop_job_row_activity(self, row_index: int):
        if 0 <= row_index < len(self.job_rows):
            row = self.job_rows[row_index]
            try:
                row["progress"].stop()
                row["progress"].configure(mode="determinate")
            except Exception:
                pass

    def allocate_job_row(self, file_key: str) -> int:
        while not self.cancel_requested:
            max_rows = max(self.get_parallel_jobs(), self.get_analysis_parallel_jobs())
            for idx, lock in enumerate(self.job_row_locks[:max_rows]):
                if lock.acquire(blocking=False):
                    with self.active_job_map_lock:
                        self.active_job_map[file_key] = idx
                    self.ui(self.refresh_active_job_rows)
                    return idx
            time.sleep(0.1)
        return -1

    def release_job_row(self, file_key: str):
        with self.active_job_map_lock:
            idx = self.active_job_map.pop(file_key, None)
        if idx is not None:
            try:
                self.job_row_locks[idx].release()
            except Exception:
                pass
        self.ui(self.refresh_active_job_rows)

    def run_ffmpeg_with_progress(self, args, progress_file, duration_seconds, file_key, display_name, phase_text):
        stderr_lines = []
        proc = None
        progress_pos = 0
        progress_remainder = ""
        last_pct = -1

        row_index = self.active_job_map.get(file_key)
        if row_index is not None:
            self.ui(self.start_job_row_activity, row_index, display_name, phase_text)

        def reader_thread(pipe, collector):
            try:
                while True:
                    line = pipe.readline()
                    if not line:
                        break
                    collector.append(line)
            except Exception:
                pass

        def update_progress_from_file():
            nonlocal progress_pos, progress_remainder, last_pct

            if not progress_file or not os.path.exists(progress_file) or duration_seconds <= 0:
                return

            try:
                with open(progress_file, "r", encoding="utf-8", errors="replace") as f:
                    f.seek(progress_pos)
                    chunk = f.read()
                    progress_pos = f.tell()
            except Exception:
                return

            if not chunk:
                return

            data = progress_remainder + chunk
            plines = data.splitlines()

            if data and not data.endswith(("\n", "\r")):
                progress_remainder = plines.pop() if plines else data
            else:
                progress_remainder = ""

            for line in reversed(plines):
                if not line.startswith("out_time_ms="):
                    continue

                try:
                    out_ms = float(line.split("=", 1)[1].strip())
                    pct = int(max(0, min(100, (out_ms / 1000000.0) / duration_seconds * 100)))
                except Exception:
                    continue

                if pct <= last_pct:
                    return

                last_pct = pct
                row_index2 = self.active_job_map.get(file_key)
                if row_index2 is not None:
                    self.ui(self.set_job_row, row_index2, display_name, phase_text, pct)
                return

        try:
            proc = subprocess.Popen(
                args,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=NO_WINDOW,
            )
            self.register_process(proc)

            t_err = threading.Thread(target=reader_thread, args=(proc.stderr, stderr_lines), daemon=True)
            t_err.start()

            while proc.poll() is None:
                if self.cancel_requested:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                    break

                update_progress_from_file()
                time.sleep(0.2)

            update_progress_from_file()

            try:
                proc.wait(timeout=5)
            except Exception:
                pass

            try:
                t_err.join(timeout=2)
            except Exception:
                pass

            stderr_acc = "".join(stderr_lines)
            exit_code = proc.returncode if proc else -1
            cancelled = self.cancel_requested

            row_index2 = self.active_job_map.get(file_key)
            if row_index2 is not None:
                self.ui(self.stop_job_row_activity, row_index2)
                if not cancelled and last_pct >= 0:
                    self.ui(self.set_job_row, row_index2, display_name, phase_text, max(last_pct, 99))

            return exit_code, "", stderr_acc, cancelled

        finally:
            if proc:
                self.unregister_process(proc)
                try:
                    if proc.stderr:
                        proc.stderr.close()
                except Exception:
                    pass

            try:
                if progress_file and os.path.exists(progress_file):
                    os.remove(progress_file)
            except Exception:
                pass


    def prepare_file_job_context(self, file_path: str, input_root: str, output_root: str, tag: str, item_started: float | None = None):
        input_file = str(file_path)
        p = Path(input_file)
        display_name = p.name
        item_started = item_started or time.time()

        overwrite_original = bool(self.overwrite_original_var.get())
        rel_path = os.path.relpath(input_file, input_root)
        rel_dir = os.path.dirname(rel_path)
        target_dir = output_root if rel_dir in ("", ".") else os.path.join(output_root, rel_dir)
        os.makedirs(target_dir, exist_ok=True)

        if overwrite_original:
            final_output = input_file
            temp_work_dir = self.get_effective_temp_work_dir(input_file)
            if not temp_work_dir or not os.path.isdir(temp_work_dir):
                raise RuntimeError(f"Invalid temp work dir: {temp_work_dir}")
            tmp_name = f"{p.stem}.__ln_tmp__{p.suffix}"
            output_file = os.path.join(temp_work_dir, tmp_name)
            existing_candidates = []
        else:
            final_output = os.path.join(target_dir, p.stem + tag + p.suffix)
            output_file = final_output
            existing_candidates = [
                os.path.join(target_dir, p.name),
                os.path.join(target_dir, p.stem + "_loudnorm" + p.suffix),
                os.path.join(target_dir, p.stem + "_loudnorm_nvenc" + p.suffix),
            ]

        input_base = normalize_name(p.stem)
        existing_file = next((x for x in existing_candidates if os.path.exists(x)), None)

        if not overwrite_original and not existing_file and os.path.exists(target_dir):
            for f in os.listdir(target_dir):
                full = os.path.join(target_dir, f)
                if not os.path.isfile(full):
                    continue
                base = os.path.splitext(f)[0]
                base_norm = normalize_name(canonical_output_stem(base))
                if input_base == base_norm:
                    existing_file = full
                    break

        if existing_file:
            return {
                "File": input_file,
                "Output": final_output if overwrite_original else existing_file,
                "Status": "SKIP_EXISTS",
                "Phase": self.msg("job_skipped"),
                "Details": "",
                "DisplayName": display_name,
                "Elapsed": time.time() - item_started,
            }

        if overwrite_original:
            self.ui(self.log, f"{display_name}: overwrite mode prepared -> temp file {output_file}")

        audio_stream_count = get_audio_stream_count(self.ffprobe_path, input_file)
        if audio_stream_count == 0:
            return {
                "File": input_file,
                "Output": final_output,
                "Status": "ERROR_NO_AUDIO",
                "Phase": "Vorbereitung",
                "Details": "Keine Audiospur gefunden.",
                "DisplayName": display_name,
                "Elapsed": time.time() - item_started,
            }

        audio_stream_info = get_audio_stream_info(self.ffprobe_path, input_file)
        selected_audio_indices = self.get_selected_audio_stream_indices(audio_stream_info)
        output_audio_order = self.get_output_audio_order(audio_stream_info)
        default_output_audio_index = self.get_default_output_audio_index(audio_stream_info, output_audio_order)

        if not selected_audio_indices:
            return {
                "File": input_file,
                "Output": final_output,
                "Status": "ERROR_NO_MATCHING_AUDIO",
                "Phase": "Vorbereitung",
                "Details": "Keine passende Audiospur fuer den gewaehlten Modus gefunden.",
                "DisplayName": display_name,
                "Elapsed": time.time() - item_started,
            }

        return {
            "File": input_file,
            "Output": final_output,
            "EncodeOutput": output_file,
            "OverwriteOriginal": overwrite_original,
            "DisplayName": display_name,
            "ElapsedStarted": item_started,
            "AudioStreamInfo": audio_stream_info,
            "SelectedAudioIndices": selected_audio_indices,
            "OutputAudioOrder": output_audio_order,
            "DefaultOutputAudioIndex": default_output_audio_index,
            "DurationSeconds": get_media_duration_seconds(self.ffprobe_path, input_file),
        }

    def analyze_one_file(self, file_path: str, input_root: str, output_root: str, tag: str):
        if self.cancel_requested:
            return None

        file_key = f"analyze::{file_path}"
        row_index = self.allocate_job_row(file_key)
        if row_index < 0:
            return None

        item_started = time.time()
        try:
            ctx = self.prepare_file_job_context(file_path, input_root, output_root, tag, item_started=item_started)
            display_name = ctx.get("DisplayName", Path(str(file_path)).name)

            if ctx.get("Status") == "SKIP_EXISTS":
                self.ui(self.finish_job_row, row_index, display_name, self.msg("job_skipped"))
                return ctx

            if ctx.get("Status", "").startswith("ERROR"):
                self.ui(self.set_job_row, row_index, display_name, ctx.get("Details", "Fehler"), 100)
                return ctx

            audio_stream_info = ctx["AudioStreamInfo"]
            selected_audio_indices = ctx["SelectedAudioIndices"]
            default_output_audio_index = ctx["DefaultOutputAudioIndex"]

            if self.audio_track_mode_var.get() in {"Auto (Deutsch bevorzugt)", "Nur bevorzugte Sprache"}:
                german_indices = self.get_preferred_audio_stream_indices(audio_stream_info)
                if german_indices:
                    first_german_input_index = german_indices[0]
                    if self.audio_track_mode_var.get() == "Auto (Deutsch bevorzugt)":
                        self.ui(self.log, f"{display_name}: Auto-Modus verwendet bevorzugte Sprache {first_german_input_index + 1} fuer die Normalisierung und setzt sie als Default.")
                    elif default_output_audio_index is not None:
                        self.ui(self.log, f"{display_name}: Deutsche Spur {first_german_input_index + 1} wird als Default gesetzt.")
                else:
                    self.ui(self.log, f"{display_name}: Auto-Modus hat keine bevorzugte Sprache gefunden, daher wird Spur 1 verwendet.")

            duration_seconds = ctx["DurationSeconds"]
            guid = next(tempfile._get_candidate_names())
            loudnorm_stats = {}
            selected_count = len(selected_audio_indices)

            for analysis_pos, stream_idx in enumerate(selected_audio_indices, start=1):
                pass1_progress = os.path.join(tempfile.gettempdir(), f"pass1_{guid}_{stream_idx}.progress")
                phase_text = self.msg("pass1_analyze_track", current=analysis_pos, total=selected_count)
                self.ui(self.set_job_row, row_index, display_name, phase_text, 0)

                pass1_args = [
                    self.ffmpeg_path,
                    "-hide_banner",
                    "-nostats",
                    "-loglevel", "info",
                    "-progress", pass1_progress,
                    "-i", ctx["File"],
                    "-map", f"0:a:{stream_idx}",
                    "-vn",
                    "-sn",
                    "-dn",
                    "-af", "loudnorm=I=-16:TP=-1.5:LRA=11:print_format=json",
                    "-f", "null", "NUL",
                ]

                _, _, pass1_err, cancelled = self.run_ffmpeg_with_progress(
                    pass1_args,
                    pass1_progress,
                    duration_seconds,
                    file_key,
                    display_name,
                    phase_text,
                )
                if cancelled:
                    return None

                json_obj = find_loudnorm_json(pass1_err)
                stats = parse_loudnorm_stats(json_obj)
                if not stats:
                    self.ui(self.set_job_row, row_index, display_name, self.msg("pass1_error"), 100)
                    details = " | ".join((pass1_err or self.msg("pass1_missing_stats", track=stream_idx + 1)).splitlines()[-12:])
                    return {
                        "File": ctx["File"],
                        "Output": ctx["Output"],
                        "Status": "ERROR_PASS1",
                        "Phase": self.msg("pass1_phase_track", track=stream_idx + 1),
                        "Details": details or self.msg("pass1_json_incomplete", track=stream_idx + 1),
                        "DisplayName": display_name,
                        "Elapsed": time.time() - item_started,
                    }

                loudnorm_stats[stream_idx] = stats

            ctx["LoudnormStats"] = loudnorm_stats
            ctx["Status"] = "ANALYZED"
            ctx["Elapsed"] = time.time() - item_started
            self.ui(self.finish_job_row, row_index, display_name, self.msg("analysis_finished"))
            return ctx
        finally:
            time.sleep(0.1)
            self.release_job_row(file_key)

    def encode_one_file(self, ctx, audio_codec: str, audio_bitrate: str, video_args):
        if self.cancel_requested or not ctx:
            return None

        file_key = f"encode::{ctx['File']}"
        row_index = self.allocate_job_row(file_key)
        if row_index < 0:
            return None

        item_started = time.time()
        try:
            display_name = ctx["DisplayName"]
            duration_seconds = ctx["DurationSeconds"]
            audio_stream_info = ctx["AudioStreamInfo"]
            output_audio_order = ctx["OutputAudioOrder"]
            default_output_audio_index = ctx["DefaultOutputAudioIndex"]
            loudnorm_stats = ctx["LoudnormStats"]

            guid = next(tempfile._get_candidate_names())
            pass2_progress = os.path.join(tempfile.gettempdir(), f"pass2_{guid}.progress")
            self.ui(self.set_job_row, row_index, display_name, self.msg("pass2_normalization"), 0)

            filter_parts = []
            for stream_idx in ctx["SelectedAudioIndices"]:
                stats = loudnorm_stats[stream_idx]
                filter_parts.append(
                    f"[0:a:{stream_idx}]loudnorm=I=-16:TP=-1.5:LRA=11:"
                    f"measured_I={stats['input_i']}:measured_TP={stats['input_tp']}:measured_LRA={stats['input_lra']}:"
                    f"measured_thresh={stats['input_thresh']}:offset={stats['target_offset']}:linear=true:print_format=summary[anorm{stream_idx}]"
                )
            filter_str = ";".join(filter_parts)

            pass2_args = [
                self.ffmpeg_path,
                "-hide_banner",
                "-nostats",
                "-loglevel", "info",
                "-progress", pass2_progress,
                "-y",
                "-i", ctx["File"],
                "-filter_complex", filter_str,
                "-map_metadata", "0",
                "-map_chapters", "0",
                "-map", "0:v?",
            ]

            for input_audio_idx in output_audio_order:
                if input_audio_idx in loudnorm_stats:
                    pass2_args += ["-map", f"[anorm{input_audio_idx}]"]
                else:
                    pass2_args += ["-map", f"0:a:{input_audio_idx}?"]

            pass2_args += ["-map", "0:s?", "-map", "0:d?", "-map", "0:t?"]
            pass2_args += video_args

            for output_audio_idx, input_audio_idx in enumerate(output_audio_order):
                if input_audio_idx in loudnorm_stats:
                    pass2_args += [f"-c:a:{output_audio_idx}", audio_codec, f"-b:a:{output_audio_idx}", audio_bitrate]
                    sample_rate = str(audio_stream_info[input_audio_idx].get("sample_rate") or "").strip()
                    if sample_rate.isdigit():
                        pass2_args += [f"-ar:a:{output_audio_idx}", sample_rate]
                else:
                    pass2_args += [f"-c:a:{output_audio_idx}", "copy"]

            for output_audio_idx, input_audio_idx in enumerate(output_audio_order):
                info = audio_stream_info[input_audio_idx]
                lang = (info.get("language") or "").strip()
                title = (info.get("title") or "").strip()
                forced = int(info.get("forced", 0) or 0)

                if lang:
                    pass2_args += [f"-metadata:s:a:{output_audio_idx}", f"language={lang}"]
                if title:
                    pass2_args += [f"-metadata:s:a:{output_audio_idx}", f"title={title}"]

                disposition_parts = []
                if default_output_audio_index is not None and output_audio_idx == default_output_audio_index:
                    disposition_parts.append("default")
                if forced:
                    disposition_parts.append("forced")
                pass2_args += [f"-disposition:a:{output_audio_idx}", "+".join(disposition_parts) if disposition_parts else "0"]

            pass2_args += ["-c:s", "copy", "-c:d", "copy", "-c:t", "copy", ctx["Output"]]

            _, _, pass2_err, cancelled = self.run_ffmpeg_with_progress(
                pass2_args,
                pass2_progress,
                duration_seconds,
                file_key,
                display_name,
                self.msg("pass2_normalization"),
            )
            if cancelled:
                try:
                    encode_output = ctx.get("EncodeOutput", ctx["Output"])
                    if ctx.get("OverwriteOriginal") and encode_output and os.path.exists(encode_output):
                        os.remove(encode_output)
                except Exception:
                    pass
                return None

            encode_output = ctx.get("EncodeOutput", ctx["Output"])
            success = os.path.exists(encode_output) and os.path.getsize(encode_output) > 1024 * 1024
            if success:
                if ctx.get("OverwriteOriginal"):
                    try:
                        self.replace_original_file(encode_output, ctx["Output"], preserve_timestamp=bool(self.preserve_timestamp_var.get()))
                        self.ui(self.log, f"{display_name}: {self.msg('overwrite_done')}")
                    except Exception as e:
                        try:
                            if os.path.exists(encode_output):
                                os.remove(encode_output)
                        except Exception:
                            pass
                        self.ui(self.set_job_row, row_index, display_name, self.msg("replace_failed", error=str(e)), 100)
                        return {
                            "File": ctx["File"],
                            "Output": ctx["Output"],
                            "Status": "ERROR_REPLACE",
                            "Phase": self.msg("job_done"),
                            "Details": self.msg("replace_failed", error=str(e)),
                            "DisplayName": display_name,
                            "Elapsed": time.time() - item_started,
                        }

                self.ui(self.finish_job_row, row_index, display_name, self.msg("job_done"))
                return {
                    "File": ctx["File"],
                    "Output": ctx["Output"],
                    "Status": "OK",
                    "Phase": "Pass 2",
                    "Details": "",
                    "DisplayName": display_name,
                    "Elapsed": time.time() - item_started,
                }

            self.ui(self.set_job_row, row_index, display_name, "Pass 2 Fehler", 100)
            try:
                if ctx.get("OverwriteOriginal") and os.path.exists(ctx.get("EncodeOutput", "")):
                    os.remove(ctx.get("EncodeOutput", ""))
            except Exception:
                pass
            details = " | ".join((pass2_err or "FFmpeg Pass 2 fehlgeschlagen.").splitlines()[-12:])
            return {
                "File": ctx["File"],
                "Output": ctx["Output"],
                "Status": "ERROR_PASS2",
                "Phase": "Pass 2",
                "Details": details,
                "DisplayName": display_name,
                "Elapsed": time.time() - item_started,
            }
        finally:
            time.sleep(0.1)
            self.release_job_row(file_key)

    def process_one_file(self, file_path: str, input_root: str, output_root: str, audio_codec: str, audio_bitrate: str, video_args, tag: str):
        if self.cancel_requested:
            return None

        file_key = str(file_path)
        row_index = self.allocate_job_row(file_key)
        if row_index < 0:
            return None

        input_file = str(file_path)
        p = Path(input_file)
        display_name = p.name
        item_started = time.time()

        try:
            self.ui(self.set_job_row, row_index, display_name, "Vorbereitung", 0)

            overwrite_original = bool(self.overwrite_original_var.get())
    
            rel_path = os.path.relpath(input_file, input_root)
            rel_dir = os.path.dirname(rel_path)
            target_dir = output_root if rel_dir in ("", ".") else os.path.join(output_root, rel_dir)
            os.makedirs(target_dir, exist_ok=True)

            if overwrite_original:
                final_output = input_file
                temp_work_dir = self.get_effective_temp_work_dir(input_file)
                if not temp_work_dir or not os.path.isdir(temp_work_dir):
                    raise RuntimeError(f"Invalid temp work dir: {temp_work_dir}")
                tmp_name = f"{p.stem}.__ln_tmp__{p.suffix}"
                output_file = os.path.join(temp_work_dir, tmp_name)
                existing_candidates = []
            else:
                final_output = os.path.join(target_dir, p.stem + tag + p.suffix)
                output_file = final_output
                existing_candidates = [
                    os.path.join(target_dir, p.name),
                    os.path.join(target_dir, p.stem + "_loudnorm" + p.suffix),
                    os.path.join(target_dir, p.stem + "_loudnorm_nvenc" + p.suffix),
                ]

            input_base = normalize_name(p.stem)
            existing_file = next((x for x in existing_candidates if os.path.exists(x)), None)

            if not overwrite_original and not existing_file and os.path.exists(target_dir):
                for f in os.listdir(target_dir):
                    full = os.path.join(target_dir, f)
                    if not os.path.isfile(full):
                        continue
                    base = os.path.splitext(f)[0]
                    base_norm = normalize_name(canonical_output_stem(base))
                    if input_base == base_norm:
                        existing_file = full
                        break

            if existing_file:
                self.ui(self.finish_job_row, row_index, display_name, self.msg("job_skipped"))
                return {
                    "File": input_file,
                    "Output": final_output if overwrite_original else existing_file,
                    "Status": "SKIP_EXISTS",
                    "Phase": self.msg("job_skipped"),
                    "Details": "",
                    "DisplayName": display_name,
                    "Elapsed": time.time() - item_started,
                }

            if overwrite_original:
                self.ui(self.log, f"{display_name}: overwrite mode -> temp file {output_file}")

            audio_stream_count = get_audio_stream_count(self.ffprobe_path, input_file)
            if audio_stream_count == 0:
                self.ui(self.set_job_row, row_index, display_name, "Keine Audiospur", 100)
                return {
                    "File": input_file,
                    "Output": final_output,
                    "Status": "ERROR_NO_AUDIO",
                    "Phase": "Vorbereitung",
                    "Details": "Keine Audiospur gefunden.",
                    "DisplayName": display_name,
                    "Elapsed": time.time() - item_started,
                }

            audio_stream_info = get_audio_stream_info(self.ffprobe_path, input_file)
            selected_audio_indices = self.get_selected_audio_stream_indices(audio_stream_info)
            output_audio_order = self.get_output_audio_order(audio_stream_info)
            default_output_audio_index = self.get_default_output_audio_index(audio_stream_info, output_audio_order)

            if not selected_audio_indices:
                self.ui(self.set_job_row, row_index, display_name, "Keine passende Audiospur", 100)
                return {
                    "File": input_file,
                    "Output": final_output,
                    "Status": "ERROR_NO_MATCHING_AUDIO",
                    "Phase": "Vorbereitung",
                    "Details": "Keine passende Audiospur fuer den gewaehlten Modus gefunden.",
                    "DisplayName": display_name,
                    "Elapsed": time.time() - item_started,
                }

            if self.audio_track_mode_var.get() in {"Auto (Deutsch bevorzugt)", "Nur bevorzugte Sprache"}:
                german_indices = self.get_preferred_audio_stream_indices(audio_stream_info)
                if german_indices:
                    first_german_input_index = german_indices[0]
                    if self.audio_track_mode_var.get() == "Auto (Deutsch bevorzugt)":
                        self.ui(self.log, f"{display_name}: Auto-Modus verwendet bevorzugte Sprache {first_german_input_index + 1} fuer die Normalisierung und setzt sie als Default.")
                    elif default_output_audio_index is not None:
                        self.ui(self.log, f"{display_name}: Deutsche Spur {first_german_input_index + 1} wird als Default gesetzt.")
                else:
                    self.ui(self.log, f"{display_name}: Auto-Modus hat keine bevorzugte Sprache gefunden, daher wird Spur 1 verwendet.")

            duration_seconds = get_media_duration_seconds(self.ffprobe_path, input_file)
            guid = next(tempfile._get_candidate_names())
            pass2_progress = os.path.join(tempfile.gettempdir(), f"pass2_{guid}.progress")

            loudnorm_stats = {}
            selected_count = len(selected_audio_indices)

            for analysis_pos, stream_idx in enumerate(selected_audio_indices, start=1):
                pass1_progress = os.path.join(tempfile.gettempdir(), f"pass1_{guid}_{stream_idx}.progress")
                phase_text = self.msg("pass1_analyze_track", current=analysis_pos, total=selected_count)
                self.ui(self.set_job_row, row_index, display_name, phase_text, 0)

                pass1_args = [
                    self.ffmpeg_path,
                    "-hide_banner",
                    "-nostats",
                    "-loglevel", "info",
                    "-progress", pass1_progress,
                    "-i", input_file,
                    "-map", f"0:a:{stream_idx}",
                    "-vn",
                    "-sn",
                    "-dn",
                    "-af", "loudnorm=I=-16:TP=-1.5:LRA=11:print_format=json",
                    "-f", "null", "NUL",
                ]

                _, _, pass1_err, cancelled = self.run_ffmpeg_with_progress(
                    pass1_args,
                    pass1_progress,
                    duration_seconds,
                    file_key,
                    display_name,
                    phase_text,
                )
                if cancelled:
                    return None

                json_obj = find_loudnorm_json(pass1_err)
                stats = parse_loudnorm_stats(json_obj)

                if not stats:
                    self.ui(self.set_job_row, row_index, display_name, self.msg("pass1_error"), 100)
                    details = " | ".join((pass1_err or self.msg("pass1_missing_stats", track=stream_idx + 1)).splitlines()[-12:])
                    return {
                        "File": input_file,
                        "Output": output_file,
                        "Status": "ERROR_PASS1",
                        "Phase": self.msg("pass1_phase_track", track=stream_idx + 1),
                        "Details": details or self.msg("pass1_json_incomplete", track=stream_idx + 1),
                        "DisplayName": display_name,
                        "Elapsed": time.time() - item_started,
                    }

                loudnorm_stats[stream_idx] = stats

            self.ui(self.set_job_row, row_index, display_name, self.msg("pass2_normalization"), 0)

            filter_parts = []
            for stream_idx in selected_audio_indices:
                stats = loudnorm_stats[stream_idx]
                filter_parts.append(
                    f"[0:a:{stream_idx}]loudnorm=I=-16:TP=-1.5:LRA=11:"
                    f"measured_I={stats['input_i']}:measured_TP={stats['input_tp']}:measured_LRA={stats['input_lra']}:"
                    f"measured_thresh={stats['input_thresh']}:offset={stats['target_offset']}:linear=true:print_format=summary[anorm{stream_idx}]"
                )
            filter_str = ";".join(filter_parts)

            pass2_args = [
                self.ffmpeg_path,
                "-hide_banner",
                "-nostats",
                "-loglevel", "info",
                "-progress", pass2_progress,
                "-y",
                "-i", input_file,
                "-filter_complex", filter_str,
                "-map_metadata", "0",
                "-map_chapters", "0",
                "-map", "0:v?",
            ]

            for input_audio_idx in output_audio_order:
                if input_audio_idx in loudnorm_stats:
                    pass2_args += ["-map", f"[anorm{input_audio_idx}]"]
                else:
                    pass2_args += ["-map", f"0:a:{input_audio_idx}?"]

            pass2_args += [
                "-map", "0:s?",
                "-map", "0:d?",
                "-map", "0:t?",
            ]

            pass2_args += video_args

            for output_audio_idx, input_audio_idx in enumerate(output_audio_order):
                if input_audio_idx in loudnorm_stats:
                    pass2_args += [f"-c:a:{output_audio_idx}", audio_codec, f"-b:a:{output_audio_idx}", audio_bitrate]
                else:
                    pass2_args += [f"-c:a:{output_audio_idx}", "copy"]

            for output_audio_idx, input_audio_idx in enumerate(output_audio_order):
                info = audio_stream_info[input_audio_idx]
                lang = (info.get("language") or "").strip()
                title = (info.get("title") or "").strip()
                forced = int(info.get("forced", 0) or 0)

                if lang:
                    pass2_args += [f"-metadata:s:a:{output_audio_idx}", f"language={lang}"]

                if title:
                    pass2_args += [f"-metadata:s:a:{output_audio_idx}", f"title={title}"]

                disposition_parts = []
                if default_output_audio_index is not None and output_audio_idx == default_output_audio_index:
                    disposition_parts.append("default")
                if forced:
                    disposition_parts.append("forced")

                pass2_args += [f"-disposition:a:{output_audio_idx}", "+".join(disposition_parts) if disposition_parts else "0"]

            pass2_args += [
                "-c:s", "copy",
                "-c:d", "copy",
                "-c:t", "copy",
                output_file,
            ]

            _, _, pass2_err, cancelled = self.run_ffmpeg_with_progress(
                pass2_args,
                pass2_progress,
                duration_seconds,
                file_key,
                display_name,
                self.msg("pass2_normalization"),
            )
            if cancelled:
                try:
                    if overwrite_original and output_file and os.path.exists(output_file):
                        os.remove(output_file)
                except Exception:
                    pass
                return None

            success = os.path.exists(output_file) and os.path.getsize(output_file) > 1024 * 1024

            if success:
                if overwrite_original:
                    try:
                        self.replace_original_file(output_file, final_output, preserve_timestamp=bool(self.preserve_timestamp_var.get()))
                    except Exception as e:
                        try:
                            if os.path.exists(output_file):
                                os.remove(output_file)
                        except Exception:
                            pass
                        self.ui(self.set_job_row, row_index, display_name, self.msg("replace_failed", error=str(e)), 100)
                        return {
                            "File": input_file,
                            "Output": final_output,
                            "Status": "ERROR_REPLACE",
                            "Phase": "Pass 2",
                            "Details": self.msg("replace_failed", error=str(e)),
                            "DisplayName": display_name,
                            "Elapsed": time.time() - item_started,
                        }

                self.ui(self.finish_job_row, row_index, display_name, self.msg("job_done"))
                return {
                    "File": input_file,
                    "Output": final_output if overwrite_original else output_file,
                    "Status": "OK",
                    "Phase": "Pass 2",
                    "Details": "",
                    "DisplayName": display_name,
                    "Elapsed": time.time() - item_started,
                }

            self.ui(self.set_job_row, row_index, display_name, "Pass 2 Fehler", 100)
            details = " | ".join((pass2_err or "FFmpeg Pass 2 fehlgeschlagen.").splitlines()[-12:])
            return {
                "File": input_file,
                "Output": output_file,
                "Status": "ERROR_PASS2",
                "Phase": "Pass 2",
                "Details": details,
                "DisplayName": display_name,
                "Elapsed": time.time() - item_started,
            }
        finally:
            time.sleep(0.3)
            self.release_job_row(file_key)


    def get_resume_state_path(self, output_root: str) -> str:
        return os.path.join(self.get_effective_temp_work_dir(), RESUME_STATE_FILE)

    def load_resume_state(self, output_root: str):
        path = self.get_resume_state_path(output_root)
        state = {}
        if not os.path.exists(path):
            return state

        try:
            with open(path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    file_key = os.path.normcase(os.path.abspath((row.get("File") or "").strip()))
                    if not file_key:
                        continue
                    state[file_key] = row
        except Exception as e:
            self.ui(self.log, f"Resume-Datei konnte nicht gelesen werden: {e!r}")
        return state

    def append_resume_state(self, output_root: str, result: dict):
        try:
            path = self.get_resume_state_path(output_root)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            exists = os.path.exists(path)

            with open(path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=["Timestamp", "File", "Output", "Status", "Phase", "Details"],
                )
                if not exists or os.path.getsize(path) == 0:
                    writer.writeheader()
                writer.writerow(
                    {
                        "Timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "File": result.get("File", ""),
                        "Output": result.get("Output", ""),
                        "Status": result.get("Status", ""),
                        "Phase": result.get("Phase", ""),
                        "Details": result.get("Details", ""),
                    }
                )
        except Exception as e:
            self.ui(self.log, f"Resume-Status konnte nicht geschrieben werden: {e!r}")

    def filter_files_for_resume(self, files, output_root: str):
        if not self.resume_jobs_var.get():
            return files, 0

        resume_state = self.load_resume_state(output_root)
        if not resume_state:
            return files, 0

        filtered = []
        skipped = 0

        for file_path in files:
            file_key = os.path.normcase(os.path.abspath(file_path))
            row = resume_state.get(file_key)

            if not row:
                filtered.append(file_path)
                continue

            status = (row.get("Status") or "").strip().upper()
            output_file = os.path.abspath((row.get("Output") or "").strip()) if (row.get("Output") or "").strip() else ""

            if status in {"OK", "SKIP_EXISTS"} and output_file and os.path.exists(output_file):
                skipped += 1
                continue

            filtered.append(file_path)

        return filtered, skipped

    def build_job_file_list(self):
        mode = self.source_mode_var.get()

        if mode == "folder":
            input_root = self.input_var.get().strip()
            if not input_root or not os.path.isdir(input_root):
                raise ValueError(self.msg("input_folder_not_found"))
            files = collect_videos_from_folder(input_root)
            files = sorted(set(files), key=lambda x: x.lower())
            return input_root, files

        if not self.file_list:
            raise ValueError("Keine Dateien in der Liste.")

        files = [os.path.abspath(p) for p in self.file_list if os.path.isfile(p)]
        files = sorted(set(files), key=lambda x: x.lower())

        if not files:
            raise ValueError("Keine gueltigen Dateien in der Liste.")

        common_root = os.path.commonpath(files)
        if os.path.isfile(common_root):
            common_root = os.path.dirname(common_root)

        return common_root, files


    def worker_main(self):
        try:
            if not self.ffmpeg_path:
                self.ui(
                    messagebox.showerror,
                    self.msg("ffmpeg_missing_title"),
                    self.msg("ffmpeg_missing_message"),
                )
                return

            if not self.ffprobe_path:
                self.ui(
                    messagebox.showerror,
                    self.msg("ffmpeg_missing_title"),
                    self.msg("ffprobe_missing_message"),
                )
                return

            self.ui(self.update_tool_labels)

            try:
                input_root, files = self.build_job_file_list()
            except Exception as e:
                self.ui(messagebox.showerror, "Fehler", str(e))
                return

            output_root = self.output_var.get().strip()
            if not self.overwrite_original_var.get() and not output_root:
                self.ui(messagebox.showerror, "Fehler", "Bitte Ausgabeordner angeben.")
                return

            if self.overwrite_original_var.get():
                self.get_effective_temp_work_dir()
                output_root = output_root or input_root
            os.makedirs(output_root, exist_ok=True)
            files, resume_skipped = self.filter_files_for_resume(files, output_root)

            if self.audio_var.get() == "E-AC3":
                audio_codec = "eac3"
            else:
                audio_codec = "aac"
            audio_bitrate = self.audio_bitrate_var.get().strip() or ("640k" if audio_codec == "eac3" else "384k")

            if self.video_var.get() == "HEVC NVENC":
                video_mode = "NVENC"
                video_preset = (self.video_preset_var.get().strip().split()[0] or "p5")
                video_bitrate_choice = self.video_bitrate_var.get().strip()
                if video_bitrate_choice.startswith("CQ "):
                    cq_value = "".join(ch for ch in video_bitrate_choice if ch.isdigit()) or "19"
                    video_args = ["-c:v", "hevc_nvenc", "-rc", "vbr", "-cq", cq_value, "-preset", video_preset]
                else:
                    bitrate_num = "".join(ch for ch in video_bitrate_choice if ch.isdigit()) or "12"
                    target = f"{bitrate_num}M"
                    maxrate = f"{int(bitrate_num) * 2}M"
                    bufsize = f"{int(bitrate_num) * 2}M"
                    video_args = ["-c:v", "hevc_nvenc", "-preset", video_preset, "-b:v", target, "-maxrate", maxrate, "-bufsize", bufsize]
                tag = "_loudnorm_nvenc"
            else:
                video_mode = "COPY"
                video_args = ["-c:v", "copy"]
                video_preset = "-"
                video_bitrate_choice = "-"
                tag = "_loudnorm"

            parallel_jobs = self.get_parallel_jobs()
            analysis_jobs = self.get_analysis_parallel_jobs()

            lang = self.get_lang_code()
            source_label = ("Ordner" if self.source_mode_var.get() == "folder" else "Einzeldateien") if lang == "de" else ("Folder" if self.source_mode_var.get() == "folder" else "Files")
            self.ui(self.log, f"{'Quelle' if lang == 'de' else 'Source'}   : {source_label}")
            self.ui(self.log, f"Input    : {input_root}")
            self.ui(self.log, f"Output   : {output_root}")
            self.ui(self.log, f"Audio    : {audio_codec} ({audio_bitrate})")
            if video_mode == "NVENC":
                self.ui(self.log, f"{'VideoCfg' if lang == 'de' else 'VideoCfg'} : {video_preset} / {video_bitrate_choice}")
            self.ui(self.log, f"{'AudioMod' if lang == 'de' else 'AudioMode'} : {self.get_audio_mode_log_text()}")
            self.ui(self.log, f"Video    : {video_mode}")
            if video_mode == "NVENC" and self.jobs_var.get().strip().lower() == "auto":
                self.ui(self.log, f"{'Analyse' if lang == 'de' else 'Analysis'}  : {analysis_jobs}")
                self.ui(self.log, f"Encode   : {parallel_jobs}")
            else:
                self.ui(self.log, f"{'Parallel' if lang == 'de' else 'Parallel'} : {parallel_jobs}")
            self.ui(self.log, f"ffmpeg   : {self.ffmpeg_path}")
            self.ui(self.log, f"ffprobe  : {self.ffprobe_path}")
            self.ui(self.log, f"Resume   : {('aktiv' if self.resume_jobs_var.get() else 'aus') if lang == 'de' else ('on' if self.resume_jobs_var.get() else 'off')}")
            self.ui(self.log, f"{'Overwrite' if lang == 'en' else 'Overwrite'}: " + ("on" if self.overwrite_original_var.get() else "off"))
            self.ui(self.log, "")

            total = len(files)
            if total == 0:
                self.ui(messagebox.showinfo, "Info" if lang == "de" else "Info", "Keine passenden Dateien gefunden." if lang == "de" else "No matching files found.")
                self.ui(self.lbl_progress.config, {"text": "Keine Dateien gefunden" if lang == "de" else "No files found"})
                return

            self.ui(self.log, f"{'Gesamtdateien' if lang == 'de' else 'Total files'}: {total}")
            if resume_skipped:
                self.ui(self.log, f"{'Resume-Übersprungen' if lang == 'de' else 'Resume-skipped'}: {resume_skipped}")
            self.ui(self.log, "")

            results = []
            completed = 0

            def handle_result(result):
                nonlocal completed
                if result is None:
                    return
                results.append(result)
                self.append_resume_state(output_root, result)
                completed += 1

                elapsed_val = float(result.get("Elapsed", 0.0))
                self.completed_times.append(elapsed_val)
                if result["Status"] not in {"SKIP_EXISTS", "ERROR_NO_AUDIO"} and elapsed_val > 1.0:
                    self.completed_times_for_eta.append(elapsed_val)

                self.ui(self.set_total_progress, completed, total)

                display_name = result["DisplayName"]
                status = result["Status"]
                if status == "SKIP_EXISTS":
                    self.ui(self.log, f"[{completed}/{total}] SKIP_EXISTS - {display_name}")
                    self.ui(self.log, "")
                elif status == "OK":
                    self.ui(self.log, f"[{completed}/{total}] OK - {display_name}")
                    self.ui(self.log, "")
                else:
                    self.ui(self.log, f"[{completed}/{total}] {status} - {display_name}")
                    self.ui(self.log, f"    {result['Details']}")
                    self.ui(self.log, "")

            use_split_pipeline = video_mode == "NVENC" and self.jobs_var.get().strip().lower() == "auto"

            if use_split_pipeline:
                analysis_outputs = []
                with ThreadPoolExecutor(max_workers=analysis_jobs) as executor:
                    futures = [executor.submit(self.analyze_one_file, file_path, input_root, output_root, tag) for file_path in files]
                    for future in as_completed(futures):
                        if self.cancel_requested:
                            for f in futures:
                                f.cancel()
                            break
                        result = future.result()
                        if result is None:
                            continue
                        if result.get("Status") == "ANALYZED":
                            analysis_outputs.append(result)
                        else:
                            handle_result(result)

                if not self.cancel_requested and analysis_outputs:
                    with ThreadPoolExecutor(max_workers=parallel_jobs) as executor:
                        futures = [executor.submit(self.encode_one_file, ctx, audio_codec, audio_bitrate, video_args) for ctx in analysis_outputs]
                        for future in as_completed(futures):
                            if self.cancel_requested:
                                for f in futures:
                                    f.cancel()
                                break
                            handle_result(future.result())
            else:
                if parallel_jobs == 1:
                    for file_path in files:
                        if self.cancel_requested:
                            break
                        handle_result(self.process_one_file(file_path, input_root, output_root, audio_codec, audio_bitrate, video_args, tag))
                else:
                    with ThreadPoolExecutor(max_workers=parallel_jobs) as executor:
                        futures = [
                            executor.submit(self.process_one_file, file_path, input_root, output_root, audio_codec, audio_bitrate, video_args, tag)
                            for file_path in files
                        ]
                        for future in as_completed(futures):
                            if self.cancel_requested:
                                for f in futures:
                                    f.cancel()
                                break
                            handle_result(future.result())

            if self.cancel_requested:
                self.ui(self.log, "")
                self.ui(self.log, self.msg("processing_aborted"))
                return

            log_file = os.path.join(get_data_dir(), "loudnorm_log.csv")
            with open(log_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["File", "Output", "Status", "Phase", "Details"])
                writer.writeheader()
                for r in results:
                    writer.writerow(
                        {
                            "File": r["File"],
                            "Output": r["Output"],
                            "Status": r["Status"],
                            "Phase": r["Phase"],
                            "Details": r["Details"],
                        }
                    )

            ok_count = sum(1 for r in results if r["Status"] == "OK")
            skip_count = sum(1 for r in results if r["Status"] == "SKIP_EXISTS")
            error_count = sum(1 for r in results if r["Status"].startswith("ERROR"))

            self.ui(self.lbl_progress.config, {"text": self.msg("done")})
            self.ui(self.lbl_percent.config, {"text": "100 %"})
            self.ui(self.lbl_eta.config, {"text": "ETA: 0 min"})
            self.ui(self.log, "Zusammenfassung")
            self.ui(self.log, "--------------")
            self.ui(self.log, f"OK      : {ok_count}")
            self.ui(self.log, f"Skipped : {skip_count}")
            self.ui(self.log, f"Fehler  : {error_count}")
            self.ui(self.log, f"Log     : {log_file}")

            for i in range(max(self.get_parallel_jobs(), self.get_analysis_parallel_jobs())):
                self.ui(self.finish_job_row, i, f"Job {i + 1}", self.msg("ready"))

            self.ui(messagebox.showinfo, self.msg("done_title"), self.msg("processing_completed"))
        except Exception as e:
            msg = repr(e)
            write_crash_log(msg)
            self.ui(self.log, "")
            self.ui(self.log, "KRITISCHER FEHLER:")
            self.ui(self.log, msg)
            self.ui(messagebox.showerror, "Start-Fehler", msg)
        finally:
            self.ui(self.set_ui_enabled, True)

    def start_processing(self):
        if self.worker_thread and self.worker_thread.is_alive():
            return

        self.ffmpeg_path = resolve_tool_path("ffmpeg.exe")
        self.ffprobe_path = resolve_tool_path("ffprobe.exe")
        self.update_tool_labels()

        self.cancel_requested = False
        self.run_started_ts = time.time()
        self.completed_times = []
        self.completed_times_for_eta = []

        self.set_ui_enabled(False)

        self.log_text.config(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.config(state="disabled")

        self.progress_total["value"] = 0
        self.lbl_progress.config(text=self.msg("checking_settings"))
        self.lbl_percent.config(text="0 %")
        self.lbl_eta.config(text="ETA: --")

        for i in range(MAX_JOB_ROWS):
            self.clear_job_row(i)
        self.refresh_active_job_rows(0)

        self.worker_thread = threading.Thread(target=self.worker_main, daemon=True)
        self.worker_thread.start()



    def browse_temp_work_dir(self):
        start_dir = sanitize_windows_config_path(self.temp_work_dir_var.get()) or self.output_var.get().strip() or get_app_dir()
        selected = filedialog.askdirectory(initialdir=start_dir)
        if selected:
            self.temp_work_dir_var.set(sanitize_windows_config_path(selected))
            self.save_settings()

    def clear_temp_work_dir(self):
        self.temp_work_dir_var.set("")
        self.save_settings()

    def browse_output(self):
        start_dir = self.output_var.get().strip()
        if not start_dir or not os.path.isdir(start_dir):
            start_dir = get_app_dir()
        path = filedialog.askdirectory(initialdir=start_dir)
        if path:
            self.output_var.set(path)

    def add_files_dialog(self):
        files = filedialog.askopenfilenames(
            title=self.msg("select_video_files_title"),
            filetypes=[("Video", "*.mkv *.mp4 *.avi *.mov *.m4v *.ts"), ("Alle Dateien", "*.*")],
        )
        if files:
            added = self.add_files_to_list(files)
            self.log(self.msg("files_added", count=added))

    def add_folder_dialog(self):
        folder = filedialog.askdirectory(title=self.msg("select_folder_title"))
        if folder:
            files = collect_videos_from_folder(folder)
            added = self.add_files_to_list(files)
            self.log(self.msg("files_added_from_folder", count=added))

    def add_files_to_list(self, files):
        existing = {os.path.normcase(os.path.abspath(p)) for p in self.file_list}
        added = 0

        for f in files:
            if not f:
                continue
            full = os.path.abspath(f)
            if not os.path.isfile(full):
                continue
            if not is_video_file(full):
                continue
            if re.search(r"_loudnorm($|_)", Path(full).stem, re.IGNORECASE):
                continue

            key = os.path.normcase(full)
            if key in existing:
                continue

            self.file_list.append(full)
            existing.add(key)
            added += 1

        self.file_list.sort(key=lambda x: x.lower())
        self.refresh_file_listbox()
        self.schedule_audio_preview_refresh(force_probe=True)
        return added

    def refresh_file_listbox(self):
        self.file_listbox.delete(0, tk.END)
        for p in self.file_list:
            self.file_listbox.insert(tk.END, p)
        if self.get_lang_code() == "en":
            self.file_count_var.set(f"{len(self.file_list)} files")
        else:
            self.file_count_var.set(f"{len(self.file_list)} Dateien")
        if self.file_list:
            self.file_listbox.selection_clear(0, tk.END)
            self.file_listbox.selection_set(0)
            self.file_listbox.activate(0)

    def remove_selected_files(self):
        selection = list(self.file_listbox.curselection())
        if not selection:
            return
        selected_paths = {self.file_listbox.get(i) for i in selection}
        self.file_list = [p for p in self.file_list if p not in selected_paths]
        self.refresh_file_listbox()
        self.schedule_audio_preview_refresh(force_probe=True)

    def clear_file_list(self):
        self.file_list = []
        self.refresh_file_listbox()
        self.schedule_audio_preview_refresh(force_probe=True)

    def log(self, text: str):
        self.log_text.config(state="normal")
        self.log_text.insert("end", text + "\n")
        self.log_text.see("end")
        self.log_text.config(state="disabled")
        self.root.update_idletasks()

    def update_tool_labels(self):
        self.ffmpeg_used_var.set(self.ffmpeg_path or self.not_found_text())
        self.ffprobe_used_var.set(self.ffprobe_path or self.not_found_text())

    def on_audio_codec_changed(self):
        self.update_audio_bitrate_ui()

    def on_video_changed(self):
        self.update_parallel_ui()
        self.update_video_options_ui()
        self.update_job_rows_visibility()
        self.schedule_audio_preview_refresh()

    def on_jobs_changed(self):
        self.update_parallel_ui()
        self.update_job_rows_visibility()
        self.schedule_audio_preview_refresh()

    def on_audio_track_mode_changed(self):
        self.update_audio_track_mode_hint()
        self.schedule_audio_preview_refresh()

    def detect_auto_parallel_jobs(self) -> int:
        cpu_count = os.cpu_count() or 4
        if self.video_var.get() == "HEVC NVENC":
            return max(2, min(4, cpu_count // 4 or 2))
        return max(2, min(MAX_JOB_ROWS, cpu_count // 2 or 2))

    def update_parallel_hint(self):
        cpu_count = os.cpu_count() or 4
        auto_copy = max(2, min(MAX_JOB_ROWS, cpu_count // 2 or 2))
        auto_nvenc_encode = max(2, min(4, cpu_count // 4 or 2))
        auto_nvenc_analysis = max(3, min(MAX_JOB_ROWS, cpu_count // 2 or 3))

        current = self.jobs_var.get().strip()
        if current.lower() == "auto":
            effective = self.get_parallel_jobs()
            if self.video_var.get() == "HEVC NVENC":
                self.parallel_hint_var.set(
                    self.msg("parallel_auto_analysis_encode", analysis=auto_nvenc_analysis, encode=effective, copy=auto_copy, nvenc=auto_nvenc_encode)
                )
            else:
                self.parallel_hint_var.set(
                    self.msg("parallel_auto_copy_nvenc", jobs=effective, copy=auto_copy, nvenc=auto_nvenc_encode)
                )
        else:
            self.parallel_hint_var.set(
                self.msg("parallel_manual_copy_nvenc", jobs=current, copy=auto_copy, nvenc=auto_nvenc_encode)
            )

    def update_parallel_ui(self):
        current = self.jobs_var.get().strip()
        valid_values = {"Auto", "1", "2", "3", "4", "5", "6", "7", "8"}
        if current not in valid_values:
            self.jobs_var.set("Auto")
        self.jobs_combo.config(state="readonly")
        self.update_parallel_hint()

    def update_audio_track_mode_hint(self):
        key = self.get_audio_track_mode_key()
        lang = self.get_lang_code()
        pref_key = self.get_preferred_language_key()
        pref_label = self.get_preferred_language_display(pref_key)

        if key == "all":
            if lang == "de":
                text = "Hinweis: Langsamer, weil jede Audiospur analysiert und neu encodiert wird."
                prefer_hint = f"Zusatzoption nur fuer 'Nur bevorzugte Sprache': erste passende {pref_label}-Spur nach vorne setzen und als Default markieren."
            else:
                text = "Hint: Slower because every audio track is analyzed and re-encoded."
                prefer_hint = f"Extra option only for 'Preferred language only': move the first matching {pref_label} track to the front and mark it as default."
            self.chk_prefer_german_first.config(state="disabled")
            self.prefer_german_first_hint_var.set(prefer_hint)
        elif key == "preferred_only":
            self.chk_prefer_german_first.config(state="normal")
            if lang == "de":
                text = f"Hinweis: Meist schneller als 'Alle Spuren'. Es werden nur Spuren in {pref_label} normalisiert, andere Spuren bleiben unveraendert."
                prefer_hint = (
                    f"Aktiv: Erste passende {pref_label}-Spur wird nach vorne gesetzt und als Default markiert."
                    if self.prefer_german_first_var.get()
                    else f"Optional: Erste passende {pref_label}-Spur automatisch nach vorne setzen und als Default markieren."
                )
            else:
                text = f"Hint: Usually faster than 'All tracks'. Only {pref_label} tracks are normalized; other tracks stay unchanged."
                prefer_hint = (
                    f"Active: The first matching {pref_label} track is moved to the front and marked as default."
                    if self.prefer_german_first_var.get()
                    else f"Optional: Move the first matching {pref_label} track to the front and mark it as default."
                )
            self.prefer_german_first_hint_var.set(prefer_hint)
        else:
            self.chk_prefer_german_first.config(state="disabled")
            if lang == "de":
                text = f"Hinweis: Schnellste Option. Wenn eine {pref_label}-Spur erkannt wird, wird diese normalisiert, sonst die erste Audiospur. Andere Spuren bleiben unveraendert."
                prefer_hint = f"Im Auto-Modus wird eine gefundene {pref_label}-Spur bevorzugt. Die Zusatzoption bleibt nur fuer 'Nur bevorzugte Sprache' aktiv."
            else:
                text = f"Hint: Fastest option. If a {pref_label} track is found, it is normalized; otherwise the first audio track is used. Other tracks stay unchanged."
                prefer_hint = f"In auto mode a matching {pref_label} track is preferred. The extra option stays active only for 'Preferred language only'."
            self.prefer_german_first_hint_var.set(prefer_hint)

        self.audio_track_mode_hint_var.set(text)

    def get_preferred_audio_stream_indices(self, audio_stream_info):
        pref_key = self.get_preferred_language_key()
        return [idx for idx, info in enumerate(audio_stream_info) if stream_matches_language(info, pref_key)]


def create_root():
    set_windows_appusermodel_id()
    if DND_AVAILABLE:
        return TkinterDnD.Tk()
    return tk.Tk()


if __name__ == "__main__":
    try:
        root = create_root()
        app = LoudnormApp(root)
        root.mainloop()
    except Exception as e:
        msg = repr(e)
        write_crash_log(msg)
        try:
            messagebox.showerror("Startup-Fehler", msg)
        except Exception:
            pass
