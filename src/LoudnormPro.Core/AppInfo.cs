using System.Reflection;

namespace LoudnormPro.Core;

/// <summary>Statische Programm-Informationen (Version, Repository, Lizenz).</summary>
public static class AppInfo
{
    public const string Name = "Loudnorm PRO";
    public const string GitHubRepo = "urscaviezel/Loudnorm-PRO";
    public const string RepoUrl = "https://github.com/" + GitHubRepo;
    public const string ReleasesUrl = RepoUrl + "/releases";
    public const string ApiLatestRelease = "https://api.github.com/repos/" + GitHubRepo + "/releases/latest";
    public const string License = "GNU GPL v3";

    public static string Version { get; } = ResolveVersion();

    public static string BuildDate { get; } = ResolveBuildDate();

    public static string RuntimeDescription =>
        $"{System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription} · {System.Runtime.InteropServices.RuntimeInformation.RuntimeIdentifier}";

    private static string ResolveVersion()
    {
        var asm = typeof(AppInfo).Assembly;
        var info = asm.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (!string.IsNullOrWhiteSpace(info))
        {
            var plus = info.IndexOf('+');
            return plus > 0 ? info[..plus] : info;
        }
        return asm.GetName().Version?.ToString(3) ?? "2.0.0";
    }

    private static string ResolveBuildDate()
    {
        try
        {
            var location = Path.Combine(AppContext.BaseDirectory, typeof(AppInfo).Assembly.GetName().Name + ".dll");
            if (File.Exists(location))
                return File.GetLastWriteTime(location).ToString("yyyy-MM-dd");
            var exe = Environment.ProcessPath;
            if (!string.IsNullOrEmpty(exe) && File.Exists(exe))
                return File.GetLastWriteTime(exe).ToString("yyyy-MM-dd");
        }
        catch
        {
            // ignoriert
        }
        return "-";
    }
}
