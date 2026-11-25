namespace LoudnormPro.Core.Platform;

/// <summary>
/// Sucht ffmpeg / ffprobe. Reihenfolge:
/// 1. Manuell gesetzter Pfad (Einstellungen)
/// 2. Programmordner, ./ffmpeg, ./ffmpeg/bin
/// 3. Bekannte Installationsorte je Betriebssystem
///    (Windows C:\ffmpeg\bin, macOS Homebrew/MacPorts, Linux /usr/bin)
/// 4. PATH
/// Hinweis macOS: Apps aus dem Finder erben den PATH der Shell nicht,
/// deshalb werden /opt/homebrew/bin und /usr/local/bin explizit geprueft.
/// </summary>
public static class ToolLocator
{
    public static string? Resolve(string toolName, string? overridePath = null)
    {
        if (!string.IsNullOrWhiteSpace(overridePath))
        {
            var p = overridePath.Trim().Trim('"');
            if (File.Exists(p))
                return Path.GetFullPath(p);
        }

        var baseName = toolName.EndsWith(".exe", StringComparison.OrdinalIgnoreCase) ? toolName[..^4] : toolName;
        var names = OperatingSystem.IsWindows()
            ? new[] { baseName + ".exe", baseName }
            : new[] { baseName };

        foreach (var root in SearchRoots())
        {
            foreach (var name in names)
            {
                try
                {
                    var candidate = Path.Combine(root, name);
                    if (File.Exists(candidate))
                        return candidate;
                }
                catch
                {
                    // ungueltiger Pfad
                }
            }
        }

        var pathVar = Environment.GetEnvironmentVariable("PATH") ?? string.Empty;
        foreach (var dir in pathVar.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
        {
            foreach (var name in names)
            {
                try
                {
                    var candidate = Path.Combine(dir.Trim().Trim('"'), name);
                    if (File.Exists(candidate))
                        return candidate;
                }
                catch
                {
                    // ungueltiger PATH-Eintrag
                }
            }
        }

        return null;
    }

    public static IEnumerable<string> SearchRoots()
    {
        var app = AppPaths.AppDir;
        yield return app;
        yield return Path.Combine(app, "ffmpeg");
        yield return Path.Combine(app, "ffmpeg", "bin");

        if (OperatingSystem.IsWindows())
        {
            yield return @"C:\ffmpeg";
            yield return @"C:\ffmpeg\bin";
            var pf = Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles);
            if (!string.IsNullOrEmpty(pf))
                yield return Path.Combine(pf, "ffmpeg", "bin");
        }
        else if (OperatingSystem.IsMacOS())
        {
            // .app-Bundle: Contents/MacOS -> auch Contents/Resources/ffmpeg pruefen
            yield return Path.GetFullPath(Path.Combine(app, "..", "Resources"));
            yield return "/opt/homebrew/bin";
            yield return "/usr/local/bin";
            yield return "/opt/local/bin";
        }
        else
        {
            yield return "/usr/bin";
            yield return "/usr/local/bin";
            yield return "/snap/bin";
        }
    }
}
