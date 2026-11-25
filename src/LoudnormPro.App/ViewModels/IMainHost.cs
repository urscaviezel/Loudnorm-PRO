using Avalonia.Controls;
using Avalonia.Controls.Notifications;

namespace LoudnormPro.App.ViewModels;

/// <summary>Was das ViewModel vom Hauptfenster braucht (Dialoge, Dateiauswahl, Zwischenablage ...).</summary>
public interface IMainHost
{
    Window Window { get; }
    IntPtr NativeHandle { get; }
    Task<IReadOnlyList<string>> PickFilesAsync(string title);
    Task<string?> PickFolderAsync(string title, string? start = null);
    Task<string?> PickToolAsync(string title);
    Task<string?> SaveTextFileAsync(string title, string suggestedName);
    Task SetClipboardAsync(string text);
    void Notify(string title, string message, NotificationType type);
    void ScrollLogToEnd();
    void RebuildMenu();
    Task<bool> ShowSettingsAsync(SettingsViewModel vm);
}
