using LoudnormPro.Core.Platform;

namespace LoudnormPro.Core.Services;

/// <summary>Aktion nach Abschluss der Verarbeitung: Ruhezustand oder Herunterfahren.</summary>
public static class PowerActions
{
    public static bool Sleep()
    {
        if (OperatingSystem.IsWindows())
            return ProcessRunner.TryStartDetached("rundll32.exe", "powrprof.dll,SetSuspendState", "0,1,0");
        if (OperatingSystem.IsMacOS())
            return ProcessRunner.TryStartDetached("pmset", "sleepnow");
        return ProcessRunner.TryStartDetached("systemctl", "suspend");
    }

    /// <summary>Windows: 60 s Vorwarnung (abbrechbar mit "shutdown /a").</summary>
    public static bool Shutdown()
    {
        if (OperatingSystem.IsWindows())
            return ProcessRunner.TryStartDetached("shutdown", "/s", "/t", "60", "/c", "Loudnorm PRO: Verarbeitung abgeschlossen");
        if (OperatingSystem.IsMacOS())
            return ProcessRunner.TryStartDetached("osascript", "-e", "tell application \"System Events\" to shut down");
        return ProcessRunner.TryStartDetached("systemctl", "poweroff");
    }
}
