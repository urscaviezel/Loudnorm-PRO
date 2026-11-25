namespace LoudnormPro.Core.Platform;

/// <summary>
/// Pfade fuer Programm- und Benutzerdaten. Kompatibel zu v1:
/// Windows %LOCALAPPDATA%\Loudnorm PRO, Linux ~/.local/share/Loudnorm PRO.
/// Neu: macOS ~/Library/Application Support/Loudnorm PRO.
/// Portable-Modus: Datei "portable.mode" oder "portable.flag" neben der EXE.
/// </summary>
public static class AppPaths
{
    public const string SettingsFileName = "loudnorm_settings.json";
    public const string ResumeFileName = "loudnorm_resume_state.csv";
    public const string RunLogFileName = "loudnorm_log.csv";
    public const string CrashLogFileName = "loudnorm_gui_crash.log";

    private static string? _dataDirOverride;

    /// <summary>Nur fuer Tests: Datenordner umleiten.</summary>
    public static void OverrideDataDir(string? path) => _dataDirOverride = path;

    public static string AppDir => AppContext.BaseDirectory;

    public static bool IsPortable =>
        File.Exists(Path.Combine(AppDir, "portable.mode")) || File.Exists(Path.Combine(AppDir, "portable.flag"));

    public static string DataDir
    {
        get
        {
            var dir = _dataDirOverride ?? ResolveDataDir();
            Directory.CreateDirectory(dir);
            return dir;
        }
    }

    public static string SettingsFile => Path.Combine(DataDir, SettingsFileName);
    public static string ResumeFile => Path.Combine(DataDir, ResumeFileName);
    public static string RunLogFile => Path.Combine(DataDir, RunLogFileName);
    public static string CrashLogFile => Path.Combine(DataDir, CrashLogFileName);

    /// <summary>Mitgelieferte Sprachdateien (neben der EXE).</summary>
    public static string BuiltInLanguagesDir => Path.Combine(AppDir, "Languages");

    /// <summary>Eigene / angepasste Sprachdateien des Benutzers.</summary>
    public static string UserLanguagesDir
    {
        get
        {
            var dir = Path.Combine(DataDir, "languages");
            Directory.CreateDirectory(dir);
            return dir;
        }
    }

    private static string ResolveDataDir()
    {
        if (IsPortable)
            return AppDir;

        if (OperatingSystem.IsWindows())
        {
            var local = Environment.GetEnvironmentVariable("LOCALAPPDATA");
            return string.IsNullOrEmpty(local) ? AppDir : Path.Combine(local, "Loudnorm PRO");
        }

        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        if (OperatingSystem.IsMacOS())
            return Path.Combine(home, "Library", "Application Support", "Loudnorm PRO");

        var xdg = Environment.GetEnvironmentVariable("XDG_DATA_HOME");
        return string.IsNullOrEmpty(xdg)
            ? Path.Combine(home, ".local", "share", "Loudnorm PRO")
            : Path.Combine(xdg, "Loudnorm PRO");
    }

    public static void WriteCrashLog(string text)
    {
        try
        {
            File.AppendAllText(CrashLogFile, $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {text}{Environment.NewLine}");
        }
        catch
        {
            // Crash-Log darf nie selbst abstuerzen
        }
    }
}
