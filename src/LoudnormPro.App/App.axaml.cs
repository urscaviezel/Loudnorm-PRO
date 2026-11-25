using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;
using LoudnormPro.App.ViewModels;
using LoudnormPro.App.Views;

namespace LoudnormPro.App;

public partial class App : Application
{
    public override void Initialize()
    {
        AvaloniaXamlLoader.Load(this);
    }

    public override void OnFrameworkInitializationCompleted()
    {
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            var vm = new MainWindowViewModel();
            var window = new MainWindow(vm);
            desktop.MainWindow = window;

            // Dateien per Kommandozeile / "Oeffnen mit"
            if (desktop.Args is { Length: > 0 } args)
                window.Opened += (_, _) => vm.AddPaths(args);

            // Weitere Programmstarts: Fenster wiederherstellen, nach vorne holen, Dateien uebernehmen
            if (Program.Instance != null)
            {
                Program.Instance.ActivationRequested += paths => Avalonia.Threading.Dispatcher.UIThread.Post(() =>
                {
                    window.BringToFront();
                    if (paths.Count > 0 && vm.CanEditList) vm.AddPaths(paths);
                });
            }
        }

        base.OnFrameworkInitializationCompleted();
    }
}
