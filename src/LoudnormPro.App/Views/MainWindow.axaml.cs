using System.Collections.Specialized;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.Notifications;
using Avalonia.Input;
using Avalonia.Input.Platform;
using Avalonia.Interactivity;
using Avalonia.Platform.Storage;
using Avalonia.Threading;
using LoudnormPro.App.Localization;
using LoudnormPro.App.ViewModels;
using LoudnormPro.Core.Media;

namespace LoudnormPro.App.Views;

public partial class MainWindow : Window, IMainHost
{
    private readonly MainWindowViewModel _vm;
    private readonly NativeMenu _windowMenu = new();
    private WindowNotificationManager? _notifications;
    private bool _closeConfirmed;
    private bool _scrollPending;

    public MainWindow()
        : this(new MainWindowViewModel())
    {
    }

    public MainWindow(MainWindowViewModel vm)
    {
        _vm = vm;
        InitializeComponent();
        DataContext = vm;
        vm.AttachHost(this);

        Width = Math.Max(MinWidth, vm.Settings.WindowWidth);
        Height = vm.Settings.WindowHeight;
        if (vm.Settings.WindowX is int x && vm.Settings.WindowY is int y)
        {
            var saved = new PixelPoint(x, y);
            var screen = Screens.ScreenFromPoint(saved) ?? Screens.Primary;
            if (screen != null)
            {
                var area = screen.WorkingArea;
                Width = Math.Max(MinWidth, Math.Min(Width, area.Width / screen.Scaling));
                Height = Math.Max(MinHeight, Math.Min(Height, area.Height / screen.Scaling));
                WindowStartupLocation = WindowStartupLocation.Manual;
                Position = new PixelPoint(Math.Clamp(x, area.X, Math.Max(area.X, area.Right - 96)),
                    Math.Clamp(y, area.Y, Math.Max(area.Y, area.Bottom - 64)));
            }
        }
        _normalPosition = WindowStartupLocation == WindowStartupLocation.Manual ? Position : null;
        _normalSize = new Size(Width, Height);
        if (vm.Settings.WindowMaximized) WindowState = WindowState.Maximized;

        RebuildMenu();
        NativeMenu.SetMenu(this, _windowMenu);

        // handledEventsToo: Listen/ScrollViewer markieren Drag-Ereignisse teils als erledigt,
        // die Ablage soll aber im ganzen Fenster funktionieren.
        const RoutingStrategies routes = RoutingStrategies.Bubble | RoutingStrategies.Direct;
        AddHandler(DragDrop.DragEnterEvent, OnDragEnter, routes, handledEventsToo: true);
        AddHandler(DragDrop.DragOverEvent, OnDragOver, routes, handledEventsToo: true);
        AddHandler(DragDrop.DragLeaveEvent, OnDragLeave, routes, handledEventsToo: true);
        AddHandler(DragDrop.DropEvent, OnDrop, routes, handledEventsToo: true);

        FileList.KeyDown += (_, e) =>
        {
            if ((e.Key == Key.Delete || (OperatingSystem.IsMacOS() && e.Key == Key.Back)) && vm.RemoveSelectedCommand.CanExecute(FileList.SelectedItems))
            {
                vm.RemoveSelectedCommand.Execute(FileList.SelectedItems);
                e.Handled = true;
            }
        };
        FileList.DoubleTapped += (_, _) => vm.RevealFileCommand.Execute(null);
        FileList.SelectionChanged += (_, _) => vm.RemoveSelectedCommand.NotifyCanExecuteChanged();

        vm.Log.CollectionChanged += OnLogChanged;

        RegisterShortcuts();

        Opened += async (_, _) =>
        {
            RememberNormalBounds();
            PositionChanged += (_, _) => ScheduleBoundsUpdate();
            SizeChanged += (_, _) => ScheduleBoundsUpdate();
            // Windows: klassische Dateiablage aus dem Explorer (siehe WindowsFileDrop)
            Services.WindowsFileDrop.Attach(this, paths => { if (_vm.CanEditList) _vm.AddPaths(paths); });
            _notifications = new WindowNotificationManager(this)
            {
                Position = NotificationPosition.BottomRight,
                MaxItems = 3,
                Margin = new Thickness(0, 0, 8, 70),
            };
            await vm.InitializeAsync();
        };

        Closing += OnClosing;
    }

    private PixelPoint? _normalPosition;
    private Size _normalSize;
    private bool _boundsUpdatePending;

    private void ScheduleBoundsUpdate()
    {
        if (_boundsUpdatePending) return;
        _boundsUpdatePending = true;
        Dispatcher.UIThread.Post(() =>
        {
            _boundsUpdatePending = false;
            RememberNormalBounds();
        }, DispatcherPriority.Background);
    }

    private void RememberNormalBounds()
    {
        if (WindowState != WindowState.Normal || !IsVisible || Bounds.Width <= 0 || Bounds.Height <= 0) return;
        _normalPosition = Position;
        _normalSize = Bounds.Size;
    }

    private WindowState _lastVisibleState = WindowState.Normal;

    protected override void OnPropertyChanged(AvaloniaPropertyChangedEventArgs change)
    {
        base.OnPropertyChanged(change);
        if (change.Property == WindowStateProperty && WindowState != WindowState.Minimized)
            _lastVisibleState = WindowState;
    }

    /// <summary>Minimiertes/verdecktes Fenster wiederherstellen und aktivieren.</summary>
    public void BringToFront()
    {
        if (!IsVisible) Show();
        if (WindowState == WindowState.Minimized)
            WindowState = _lastVisibleState;
        Activate();
        // Windows verweigert Fremdprozessen oft den Fokus: kurz "Topmost" erzwingt das Nach-vorne-Holen
        Topmost = true;
        Topmost = false;
        Focus();
    }

    // ------------------------------------------------------------------
    // Menue + Tastenkuerzel
    // ------------------------------------------------------------------
    // macOS verknuepft das native Fenstermenue mit dieser Instanz.
    public void RebuildMenu()
    {
        Classes.Set("tipsOff", !_vm.Settings.QuickTips);
        MenuBuilder.Populate(_windowMenu, _vm, () => new GuideWindow().ShowDialog(this), enabled =>
        {
            _vm.Settings.QuickTips = enabled;
            _vm.SaveSettings();
            RebuildMenu();
        });
    }

    private void RegisterShortcuts()
    {
        // Auf macOS liefert die native Menueleiste die Kuerzel selbst.
        if (OperatingSystem.IsMacOS()) return;
        var cmd = MenuBuilder.CommandModifier;
        KeyBindings.Add(new KeyBinding { Gesture = new KeyGesture(Key.O, cmd), Command = _vm.AddFilesCommand });
        KeyBindings.Add(new KeyBinding { Gesture = new KeyGesture(Key.O, cmd | KeyModifiers.Shift), Command = _vm.AddFolderCommand });
        KeyBindings.Add(new KeyBinding { Gesture = new KeyGesture(Key.E, cmd), Command = _vm.OpenOutputCommand });
        KeyBindings.Add(new KeyBinding { Gesture = new KeyGesture(Key.OemComma, cmd), Command = _vm.OpenSettingsCommand, CommandParameter = "norm" });
        KeyBindings.Add(new KeyBinding { Gesture = new KeyGesture(Key.Escape), Command = _vm.CancelCommand });
    }

    // ------------------------------------------------------------------
    // Drag & Drop
    // ------------------------------------------------------------------
    private void OnDragEnter(object? sender, DragEventArgs e)
    {
        OnDragOver(sender, e);
    }

    private void OnDragOver(object? sender, DragEventArgs e)
    {
        // Kein Format-Vorabcheck: Windows liefert je nach Quelle (Explorer, Desktop, Archiv)
        // unterschiedliche Formate. Ob es Dateien sind, wird erst beim Ablegen geprueft.
        e.DragEffects = _vm.CanEditList ? DragDropEffects.Copy : DragDropEffects.None;
        DropZone.Classes.Set("active", true);
        e.Handled = true;
    }

    private void OnDragLeave(object? sender, DragEventArgs e) =>
        DropZone.Classes.Set("active", false);

    private void OnDrop(object? sender, DragEventArgs e)
    {
        OnDragLeave(sender, e);
        e.Handled = true;
        if (!_vm.CanEditList) return;
        var paths = GetDroppedPaths(e);
        if (paths.Count > 0) _vm.AddPaths(paths);
    }

    private static List<string> GetDroppedPaths(DragEventArgs e)
    {
        var result = new List<string>();
        try
        {
            var items = e.DataTransfer.TryGetFiles();
            if (items != null)
                result.AddRange(items.Select(i => i.TryGetLocalPath()).Where(p => !string.IsNullOrEmpty(p)).Cast<string>());
        }
        catch
        {
            // keine Dateien in der Ablage
        }

        return result;
    }

    // ------------------------------------------------------------------
    // Log automatisch scrollen
    // ------------------------------------------------------------------
    private void OnLogChanged(object? sender, NotifyCollectionChangedEventArgs e)
    {
        if (e.Action == NotifyCollectionChangedAction.Add && _vm.LogAutoScroll) ScrollLogToEnd();
    }

    public void ScrollLogToEnd()
    {
        if (_scrollPending) return;
        _scrollPending = true;
        Dispatcher.UIThread.Post(() =>
        {
            _scrollPending = false;
            if (_vm.Log.Count > 0) LogList.ScrollIntoView(_vm.Log.Count - 1);
        }, DispatcherPriority.Background);
    }

    // ------------------------------------------------------------------
    // Schliessen
    // ------------------------------------------------------------------
    private async void OnClosing(object? sender, WindowClosingEventArgs e)
    {
        if (_closeConfirmed) return;
        if (_vm.IsRunning)
        {
            e.Cancel = true;
            if (await _vm.ConfirmCloseAsync())
            {
                _closeConfirmed = true;
                SaveWindowState();
                Close();
            }
            return;
        }
        SaveWindowState();
    }

    private void SaveWindowState()
    {
        RememberNormalBounds();
        _vm.SaveWindowState(_normalSize.Width, _normalSize.Height, _normalPosition?.X, _normalPosition?.Y,
            _lastVisibleState == WindowState.Maximized);
    }

    // ------------------------------------------------------------------
    // IMainHost
    // ------------------------------------------------------------------
    public Window Window => this;

    public IntPtr NativeHandle => TryGetPlatformHandle()?.Handle ?? IntPtr.Zero;

    public async Task<IReadOnlyList<string>> PickFilesAsync(string title)
    {
        var patterns = FileNaming.VideoExtensions.Select(x => "*" + x).ToList();
        var files = await StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions
        {
            Title = title,
            AllowMultiple = true,
            FileTypeFilter = new[]
            {
                new FilePickerFileType(Loc.Instance["picker.video_filter"]) { Patterns = patterns },
                FilePickerFileTypes.All,
            },
        });
        return files.Select(f => f.TryGetLocalPath()).Where(p => !string.IsNullOrEmpty(p)).Cast<string>().ToList();
    }

    public async Task<string?> PickFolderAsync(string title, string? start = null)
    {
        var options = new FolderPickerOpenOptions { Title = title, AllowMultiple = false };
        if (!string.IsNullOrWhiteSpace(start) && Directory.Exists(start))
            options.SuggestedStartLocation = await StorageProvider.TryGetFolderFromPathAsync(start);
        var result = await StorageProvider.OpenFolderPickerAsync(options);
        return result.FirstOrDefault()?.TryGetLocalPath();
    }

    public async Task<string?> PickToolAsync(string title)
    {
        var files = await StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions { Title = title, AllowMultiple = false });
        return files.FirstOrDefault()?.TryGetLocalPath();
    }

    public async Task<string?> SaveTextFileAsync(string title, string suggestedName)
    {
        var file = await StorageProvider.SaveFilePickerAsync(new FilePickerSaveOptions
        {
            Title = title,
            SuggestedFileName = suggestedName,
            DefaultExtension = "txt",
            FileTypeChoices = new[] { new FilePickerFileType(Loc.Instance["picker.log_filter"]) { Patterns = new[] { "*.txt", "*.log" } } },
        });
        return file?.TryGetLocalPath();
    }

    public async Task SetClipboardAsync(string text)
    {
        if (Clipboard != null) await Clipboard.SetTextAsync(text);
    }

    public void Notify(string title, string message, NotificationType type) =>
        _notifications?.Show(new Notification(title, message, type, TimeSpan.FromSeconds(6)));

    public async Task<bool> ShowSettingsAsync(SettingsViewModel vm)
    {
        var w = new SettingsWindow(vm, this);
        w.Classes.Set("tipsOff", !_vm.Settings.QuickTips);
        var result = await w.ShowDialog<bool?>(this);
        return result == true;
    }
}
