using System.Text.Json;
using LoudnormPro.Core.Platform;

namespace LoudnormPro.Core.Settings;

/// <summary>Laedt und speichert die Einstellungen (atomar, mit Migration von v1).</summary>
public static class SettingsStore
{
    private static readonly object Gate = new();

    public static AppSettings Load(string? path = null)
    {
        path ??= AppPaths.SettingsFile;
        AppSettings settings;
        try
        {
            settings = File.Exists(path)
                ? JsonSerializer.Deserialize(File.ReadAllText(path), SettingsJsonContext.Default.AppSettings) ?? new AppSettings()
                : new AppSettings();
        }
        catch (Exception ex) when (ex is JsonException or IOException or UnauthorizedAccessException)
        {
            AppPaths.WriteCrashLog($"Settings konnten nicht gelesen werden: {ex.Message}");
            try { File.Copy(path, path + ".broken", overwrite: true); } catch { /* ignoriert */ }
            settings = new AppSettings();
        }

        Migrate(settings);
        if (string.IsNullOrWhiteSpace(settings.UiLanguage) && OperatingSystem.IsWindows() && !AppPaths.IsPortable)
        {
            var languageFile = Path.Combine(AppPaths.AppDir, "setup-language.txt");
            try
            {
                if (File.Exists(languageFile))
                {
                    var code = File.ReadAllText(languageFile).Trim();
                    if (code is "de" or "en" or "fr") settings.UiLanguage = code;
                }
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                AppPaths.WriteCrashLog($"Setup-Sprache konnte nicht gelesen werden: {ex.Message}");
            }
        }
        settings.Normalize();
        return settings;
    }

    public static void Save(AppSettings settings, string? path = null)
    {
        path ??= AppPaths.SettingsFile;
        lock (Gate)
        {
            try
            {
                // v1-kompatible Felder mitschreiben, damit auch die alte Version die Datei versteht
                settings.LegacyLanguage = settings.UiLanguage switch
                {
                    "de" => "Deutsch",
                    "en" => "English",
                    _ => settings.LegacyLanguage ?? "English",
                };
                settings.LegacyTheme = settings.UiTheme == "light" ? "Hell" : "Dunkel";

                var dir = Path.GetDirectoryName(path);
                if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);
                var tmp = path + ".tmp";
                File.WriteAllText(tmp, JsonSerializer.Serialize(settings, SettingsJsonContext.Default.AppSettings));
                File.Move(tmp, path, overwrite: true);
            }
            catch (Exception ex)
            {
                AppPaths.WriteCrashLog($"Settings konnten nicht gespeichert werden: {ex.Message}");
            }
        }
    }

    /// <summary>Uebernimmt Sprache und Theme aus einer v1-Datei.</summary>
    internal static void Migrate(AppSettings s)
    {
        // Nur v1 hat kein ui_language-Feld. Ein leerer v2-Wert steht fuer die Standardsprache.
        if (s.UiLanguage is null)
        {
            s.UiLanguage = s.LegacyLanguage switch
            {
                "Deutsch" => "de",
                "English" => "en",
                _ => string.Empty, // leer = Systemsprache verwenden
            };
        }

        if (string.IsNullOrWhiteSpace(s.UiTheme))
        {
            s.UiTheme = s.LegacyTheme switch
            {
                "Hell" or "Light" => "light",
                _ => "dark",
            };
        }

        // v1 speicherte Windows-Pfade mit "/" - beides ist unter .NET gueltig.
        s.TempWorkDir = (s.TempWorkDir ?? string.Empty).Trim().Trim('"');
    }
}
