using Avalonia;
using LoudnormPro.Core.Platform;

namespace LoudnormPro.App;

internal static class Program
{
    public static Services.SingleInstance? Instance { get; private set; }

    [STAThread]
    public static void Main(string[] args)
    {
        AppDomain.CurrentDomain.UnhandledException += (_, e) => AppPaths.WriteCrashLog(e.ExceptionObject?.ToString() ?? "unknown");
        TaskScheduler.UnobservedTaskException += (_, e) =>
        {
            AppPaths.WriteCrashLog(e.Exception.ToString());
            e.SetObserved();
        };

        // Nur eine Instanz: eine weitere uebergibt ihre Dateien an die laufende und beendet sich
        using var instance = new Services.SingleInstance();
        if (!instance.IsFirstInstance)
        {
            if (instance.SignalFirstInstance(args)) return;
            // Signal fehlgeschlagen (z. B. haengende alte Instanz) -> normal starten
        }
        Instance = instance;
        instance.StartListening();

        try
        {
            BuildAvaloniaApp().StartWithClassicDesktopLifetime(args);
        }
        catch (Exception ex)
        {
            AppPaths.WriteCrashLog(ex.ToString());
            throw;
        }
    }

    public static AppBuilder BuildAvaloniaApp() =>
        AppBuilder.Configure<App>()
            .UsePlatformDetect()
            .With(new MacOSPlatformOptions { ShowInDock = true })
            .LogToTrace();
}
