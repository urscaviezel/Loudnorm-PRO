# Changelog

## v2.0.0

### Neu
- Komplett neue Oberfläche mit Avalonia UI – läuft nativ auf **Windows, macOS (Apple Silicon) und Linux**
- Farbschema nach pfdhub.com, Dunkel / Hell / wie System
- Menüleiste (Datei, Einstellungen, Info) – auf macOS in der globalen Menüleiste
- Einstellungen in einem Dialog mit Seiten; Menüeinträge springen direkt zur passenden Seite
- „Aktive Einstellungen“ als Kacheln im Hauptfenster
- Lautheitsziel einstellbar (I / TP / LRA) mit Profilen: Streaming −16, EBU R128 −23, YouTube/Spotify −14, benutzerdefiniert
- Eigene Profile speichern und anwenden
- Messwerte vorher → nachher (LUFS) pro Datei, Modus „Nur analysieren“
- Status pro Datei in der Liste, Kontextmenü „Im Ordner anzeigen“
- Sprachen DE / EN / FR als JSON-Dateien – eigene Sprachen lassen sich ohne Programmänderung ergänzen
- Apple VideoToolbox (HEVC) auf macOS
- Fortschritt in der Windows-Taskleiste, Benachrichtigung bei Abschluss
- Nach Abschluss optional Energie sparen oder herunterfahren
- FFmpeg-Diagnose mit loudnorm-Testlauf
- Tastenkürzel (Ctrl/⌘+O, Ctrl/⌘+Shift+O, Ctrl/⌘+E, Ctrl/⌘+„,“, F5, Esc)

### Verbessert
- Analyse und Encoding laufen bei Hardware-Encodern als Pipeline mit getrennten Limits
- Fortschritt per `-progress pipe:1` (keine Temp-Dateien mehr)
- Abgebrochene Ausgaben werden entfernt und beim nächsten Lauf nicht fälschlich übersprungen
- Überschreib-Modus ohne Temp-Ordner möglich (temporäre Datei neben der Quelle)
- ffmpeg wird auf macOS auch unter /opt/homebrew/bin gefunden
- Bestehende `loudnorm_settings.json` aus v1 wird übernommen

## v1.0.4
- WebM-Unterstützung, automatische MKV-Ausgabe
- Drag & Drop mit Sonderzeichen
