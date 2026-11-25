using System.Diagnostics;
using System.Runtime.InteropServices;

namespace LoudnormPro.App.Services;

/// <summary>Ordner/URLs oeffnen und Dateien im Dateimanager anzeigen (Windows, macOS, Linux).</summary>
public static class PlatformHelpers
{
    public static bool OpenFolder(string path)
    {
        if (string.IsNullOrWhiteSpace(path)) return false;
        try
        {
            Directory.CreateDirectory(path);
            if (OperatingSystem.IsWindows())
                return Start("explorer.exe", path);
            if (OperatingSystem.IsMacOS())
                return Start("open", path);
            return Start("xdg-open", path);
        }
        catch
        {
            return false;
        }
    }

    public static bool RevealFile(string file)
    {
        if (!File.Exists(file)) return OpenFolder(Path.GetDirectoryName(file) ?? string.Empty);
        if (OperatingSystem.IsWindows())
            return Start("explorer.exe", $"/select,\"{file}\"", raw: true);
        if (OperatingSystem.IsMacOS())
            return Start("open", "-R", file);
        return OpenFolder(Path.GetDirectoryName(file) ?? string.Empty);
    }

    public static bool OpenUrl(string url)
    {
        try
        {
            Process.Start(new ProcessStartInfo { FileName = url, UseShellExecute = true });
            return true;
        }
        catch
        {
            if (OperatingSystem.IsMacOS()) return Start("open", url);
            if (OperatingSystem.IsLinux()) return Start("xdg-open", url);
            return false;
        }
    }

    public static bool OpenFile(string file) => OpenUrl(file);

    private static bool Start(string exe, string arg, bool raw)
    {
        try
        {
            var psi = new ProcessStartInfo { FileName = exe, UseShellExecute = false };
            if (raw) psi.Arguments = arg;
            else psi.ArgumentList.Add(arg);
            Process.Start(psi);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool Start(string exe, params string[] args)
    {
        try
        {
            var psi = new ProcessStartInfo { FileName = exe, UseShellExecute = false };
            foreach (var a in args) psi.ArgumentList.Add(a);
            Process.Start(psi);
            return true;
        }
        catch
        {
            return false;
        }
    }

    public static string PlatformName =>
        $"{RuntimeInformation.OSDescription.Trim()} ({RuntimeInformation.OSArchitecture})";
}
