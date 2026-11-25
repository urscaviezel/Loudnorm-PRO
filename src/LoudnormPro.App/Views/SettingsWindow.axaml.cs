using Avalonia.Controls;
using Avalonia.Interactivity;
using Avalonia.Platform.Storage;
using LoudnormPro.App.Localization;
using LoudnormPro.App.ViewModels;

namespace LoudnormPro.App.Views;

public partial class SettingsWindow : Window
{
    private readonly SettingsViewModel? _vm;
    private readonly IMainHost? _host;

    public SettingsWindow()
    {
        InitializeComponent();
    }

    public SettingsWindow(SettingsViewModel vm, IMainHost host)
    {
        _vm = vm;
        _host = host;
        InitializeComponent();
        DataContext = vm;
    }

    private void OnOk(object? sender, RoutedEventArgs e) => Close(true);

    private void OnCancel(object? sender, RoutedEventArgs e) => Close(false);

    private async void OnBrowseFfmpeg(object? sender, RoutedEventArgs e)
    {
        if (_vm == null || _host == null) return;
        var p = await PickAsync();
        if (p != null) _vm.FfmpegPath = p;
    }

    private async void OnBrowseFfprobe(object? sender, RoutedEventArgs e)
    {
        if (_vm == null || _host == null) return;
        var p = await PickAsync();
        if (p != null) _vm.FfprobePath = p;
    }

    private async void OnBrowseOutput(object? sender, RoutedEventArgs e)
    {
        if (_vm == null) return;
        var options = new FolderPickerOpenOptions { Title = Loc.Instance["picker.output"] };
        if (!string.IsNullOrWhiteSpace(_vm.OutputFolder) && Directory.Exists(_vm.OutputFolder))
            options.SuggestedStartLocation = await StorageProvider.TryGetFolderFromPathAsync(_vm.OutputFolder);
        var r = await StorageProvider.OpenFolderPickerAsync(options);
        var path = r.FirstOrDefault()?.TryGetLocalPath();
        if (path != null) _vm.OutputFolder = path;
    }

    private async void OnBrowseTemp(object? sender, RoutedEventArgs e)
    {
        if (_vm == null) return;
        var options = new Avalonia.Platform.Storage.FolderPickerOpenOptions { Title = Loc.Instance["picker.tempdir"] };
        var r = await StorageProvider.OpenFolderPickerAsync(options);
        var path = r.FirstOrDefault()?.TryGetLocalPath();
        if (path != null) _vm.TempWorkDir = path;
    }

    private async Task<string?> PickAsync()
    {
        var files = await StorageProvider.OpenFilePickerAsync(new Avalonia.Platform.Storage.FilePickerOpenOptions
        {
            Title = Loc.Instance["picker.tool"],
            AllowMultiple = false,
        });
        return files.FirstOrDefault()?.TryGetLocalPath();
    }
}
