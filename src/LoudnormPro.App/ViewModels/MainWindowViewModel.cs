using System.Collections;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Reflection;
using System.Text;
using Avalonia;
using Avalonia.Controls.Notifications;
using Avalonia.Styling;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using LoudnormPro.App.Localization;
using LoudnormPro.App.Services;
using LoudnormPro.App.Views;
using LoudnormPro.Core;
using LoudnormPro.Core.Localization;
using LoudnormPro.Core.Media;
using LoudnormPro.Core.Platform;
using LoudnormPro.Core.Processing;
using LoudnormPro.Core.Services;
using LoudnormPro.Core.Settings;

namespace LoudnormPro.App.ViewModels;

public sealed partial class MainWindowViewModel : ObservableObject
{
    private readonly Dictionary<string, FileItemViewModel> _fileIndex = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _probeGate = new(3);
    private readonly DispatcherTimer _elapsedTimer;
    private CancellationTokenSource? _cts;
    private DateTime _runStarted;
    private BatchSummary? _lastSummary;
    private IMainHost? _host;

    public MainWindowViewModel()
    {
        Settings = SettingsStore.Load();
        Loc.Instance.Attach(LanguageCatalog.LoadDefault(typeof(App).Assembly));
        Loc.Instance.Catalog.SetLanguage(Settings.UiLanguage);
        ApplyTheme(Settings.UiTheme, save: false);

        _outputFolder = Settings.Output;
        _logAutoScroll = Settings.LogAutoScroll;
        _elapsedTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(1) };
        _elapsedTimer.Tick += (_, _) => ElapsedText = FormatElapsed(DateTime.UtcNow - _runStarted);

        Loc.Instance.Changed += (_, _) => RefreshAllTexts();
        ResolveTools();
        RefreshAllTexts();

        if (Loc.Instance.Catalog.LoadErrors.Count > 0)
            AddLog(LogLevel.Warning, Loc.Instance.F("log.languages_errors", ("errors", string.Join("; ", Loc.Instance.Catalog.LoadErrors))));
    }

    public AppSettings Settings { get; }
    public EncoderCapabilities Caps { get; private set; } = EncoderCapabilities.None;
    public string? FfmpegPath { get; private set; }
    public string? FfprobePath { get; private set; }

    public void AttachHost(IMainHost host) => _host = host;
    private IMainHost Host => _host ?? throw new InvalidOperationException("Host not attached");

    // ===================================================================
    // Sammlungen
    // ===================================================================
    public ObservableCollection<FileItemViewModel> Files { get; } = new();
    public ObservableCollection<ActiveJobViewModel> ActiveJobs { get; } = new();
    public ObservableCollection<LogEntryViewModel> Log { get; } = new();
    public ObservableCollection<AudioTrackRowViewModel> Tracks { get; } = new();
    public ObservableCollection<ChipViewModel> AudioChips { get; } = new();
    public ObservableCollection<ChipViewModel> VideoChips { get; } = new();
    public ObservableCollection<ChipViewModel> JobChips { get; } = new();
    public ObservableCollection<EncoderChipViewModel> HeaderEncoders { get; } = new();

    // ===================================================================
    // Zustand
    // ===================================================================
    [ObservableProperty] [NotifyCanExecuteChangedFor(nameof(RemoveSelectedCommand))] private FileItemViewModel? _selectedFile;
    [ObservableProperty] private string _outputFolder;
    [ObservableProperty] [NotifyCanExecuteChangedFor(nameof(ToggleAnalysisModeCommand))] [NotifyPropertyChangedFor(nameof(CanStart), nameof(CanEditList))] private bool _isRunning;
    [ObservableProperty] private bool _isCancelling;
    [ObservableProperty] private double _overallPercent;
    [ObservableProperty] private string _percentText = "0 %";
    [ObservableProperty] private string _filesText = "0 / 0";
    [ObservableProperty] private string _etaText = "--:--";
    [ObservableProperty] private string _elapsedText = "00:00";
    [ObservableProperty] private string _stateText = string.Empty;
    private string _stateKey = "progress.state.ready";

    private void SetState(string key)
    {
        _stateKey = key;
        StateText = Loc.Instance[key];
    }
    [ObservableProperty] private string _statusText = string.Empty;
    [ObservableProperty] private string _countText = string.Empty;
    [ObservableProperty] private string _startText = string.Empty;
    [ObservableProperty] private string _tracksFile = string.Empty;
    [ObservableProperty] private string _tracksSummary = string.Empty;
    [ObservableProperty] private bool _tracksLoading;
    [ObservableProperty] private bool _isLogTab = true;
    [ObservableProperty] private bool _logAutoScroll;
    [ObservableProperty] private string _ffmpegChipText = "ffmpeg";
    [ObservableProperty] private bool _ffmpegOk;
    [ObservableProperty] private string _profileText = string.Empty;

    public bool IsTracksTab
    {
        get => !IsLogTab;
        set => IsLogTab = !value;
    }

    partial void OnIsLogTabChanged(bool value) => OnPropertyChanged(nameof(IsTracksTab));

    public bool ToolsMissing => FfmpegPath == null || FfprobePath == null;
    public string MissingToolMessage => Loc.Instance[FfmpegPath == null ? "status.ffmpeg_missing" : "status.ffprobe_missing"];
    public string ToolInstallHint => Loc.Instance["status.tool_install_hint"] + Environment.NewLine +
        string.Join(Environment.NewLine, ToolLocator.SearchRoots().Append("PATH"));

    public bool CanStart => !IsRunning;
    public bool CanEditList => !IsRunning;
    [RelayCommand(CanExecute = nameof(CanEditList))]
    private void ToggleAnalysisMode()
    {
        Settings.AnalyzeOnly = !Settings.AnalyzeOnly;
        SaveSettings();
        RefreshAllTexts();
    }
    public bool HasFiles => Files.Count > 0;
    public bool HasNoFiles => Files.Count == 0;
    public bool HasActiveJobs => ActiveJobs.Count > 0;
    public bool HasNoActiveJobs => ActiveJobs.Count == 0;
    public bool HasTracks => Tracks.Count > 0;
    public bool HasProfile => ProfileText.Length > 0;
    public bool OverwriteActive => Settings.OverwriteOriginal && !Settings.AnalyzeOnly;
    public bool OutputEditable => !OverwriteActive;

    partial void OnProfileTextChanged(string value) => OnPropertyChanged(nameof(HasProfile));

    public string OutputTooltip => OverwriteActive
        ? Loc.Instance["output.overwrite_active"]
        : string.IsNullOrWhiteSpace(OutputFolder) ? Loc.Instance["output.placeholder"] : OutputFolder;

    partial void OnOutputFolderChanged(string value)
    {
        OnPropertyChanged(nameof(OutputTooltip));
        Settings.Output = value?.Trim() ?? string.Empty;
        SaveSettings();
        RefreshStatus();
    }

    partial void OnLogAutoScrollChanged(bool value)
    {
        Settings.LogAutoScroll = value;
        SaveSettings();
    }

    partial void OnSelectedFileChanged(FileItemViewModel? value) => _ = RefreshTracksAsync();

    // ===================================================================
    // Start / Initialisierung
    // ===================================================================
    public async Task InitializeAsync()
    {
        AddLog(LogLevel.Accent, $"Loudnorm PRO {AppInfo.Version} · {AppInfo.RuntimeDescription}");
        AddLog(LogLevel.Info, $"{"ffmpeg",-9}: {FfmpegPath ?? Loc.Instance["build.not_found"]}");
        AddLog(LogLevel.Info, $"{"ffprobe",-9}: {FfprobePath ?? Loc.Instance["build.not_found"]}");

        if (FfmpegPath is { } ffmpeg)
        {
            var snapshot = Settings.Clone();
            await Task.Run(() => AnalysisCache.MigrateKnownFiles(ffmpeg, snapshot));
        }
        Caps = await EncoderDetector.DetectAsync(FfmpegPath);
        RefreshEncoderChips();
        var encoders = string.Join(", ", Caps.AvailableModes().Where(m => m != VideoModes.Copy));
        AddLog(Caps.FfmpegVersion.Length > 0 ? LogLevel.Info : LogLevel.Warning,
            $"{"Encoder",-9}: {(encoders.Length > 0 ? encoders : "COPY")}" + (Caps.FfmpegVersion.Length > 0 ? $" · ffmpeg {Caps.FfmpegVersion}" : string.Empty));

        if (ToolsMissing) AddLog(LogLevel.Error, $"{MissingToolMessage} https://www.ffmpeg.org\n{ToolInstallHint}");
        RefreshAllTexts();

        if (Settings.CheckUpdatesOnStart)
        {
            await Task.Delay(1500);
            await CheckUpdatesInternalAsync(manual: false);
        }
    }

    private void ResolveTools()
    {
        FfmpegPath = ToolLocator.Resolve("ffmpeg", Settings.FfmpegPath);
        FfprobePath = ToolLocator.Resolve("ffprobe", Settings.FfprobePath);
        OnPropertyChanged(nameof(ToolsMissing));
        OnPropertyChanged(nameof(MissingToolMessage));
        OnPropertyChanged(nameof(FfmpegPath));
        OnPropertyChanged(nameof(FfprobePath));
    }

    private void RefreshEncoderChips()
    {
        HeaderEncoders.Clear();
        FfmpegOk = FfmpegPath != null;
        FfmpegChipText = FfmpegPath == null ? "ffmpeg ✗" : $"ffmpeg {ShortVersion(Caps.FfmpegVersion)}".TrimEnd();
        if (Caps.Nvenc) HeaderEncoders.Add(new() { Name = "NVENC", Available = true });
        if (Caps.Amf) HeaderEncoders.Add(new() { Name = "AMF", Available = true });
        if (Caps.Vaapi) HeaderEncoders.Add(new() { Name = "VAAPI", Available = true });
        if (Caps.VideoToolbox) HeaderEncoders.Add(new() { Name = "VideoToolbox", Available = true });
    }

    /// <summary>"7.1.1-full_build" -> "7.1.1"; Nightly-Builds ("2026-09-28-git-...") -> "✓".</summary>
    private static string ShortVersion(string v)
    {
        var m = System.Text.RegularExpressions.Regex.Match(v ?? string.Empty, @"^n?(\d+\.\d+(?:\.\d+)?)");
        return m.Success ? m.Groups[1].Value : "✓";
    }

    // ===================================================================
    // Texte
    // ===================================================================
    public void RefreshAllTexts()
    {
        OnPropertyChanged(nameof(MissingToolMessage));
        OnPropertyChanged(nameof(ToolInstallHint));
        foreach (var f in Files) f.RefreshTexts();
        RefreshChips();
        RefreshStatus();
        StartText = Settings.AnalyzeOnly ? Loc.Instance["footer.analyze"] : Loc.Instance["footer.start"];
        StateText = Loc.Instance[_stateKey];
        OnPropertyChanged(nameof(OverwriteActive));
        OnPropertyChanged(nameof(OutputEditable));
        OnPropertyChanged(nameof(OutputTooltip));
        _ = RefreshTracksAsync();
    }

    private void RefreshStatus()
    {
        CountText = Loc.Instance.F("sources.count", ("count", Files.Count));
        OnPropertyChanged(nameof(HasFiles));
        OnPropertyChanged(nameof(HasNoFiles));
        if (IsRunning) return;
        if (_lastSummary != null)
        {
            StatusText = _lastSummary.Cancelled
                ? Loc.Instance["status.cancelled"]
                : Loc.Instance.F("status.done", ("ok", _lastSummary.Ok), ("skipped", _lastSummary.Skipped + _lastSummary.ResumeSkipped), ("errors", _lastSummary.Errors));
            return;
        }
        StatusText = Files.Count == 0
            ? Loc.Instance["status.empty"]
            : Loc.Instance.F("status.ready", ("count", Files.Count));
    }

    public void RefreshChips()
    {
        var l = Loc.Instance;
        var s = Settings;
        var inv = CultureInfo.InvariantCulture;
        var t = s.Target;
        AudioChips.Clear();
        VideoChips.Clear();
        JobChips.Clear();
        AudioChips.Add(new() { Command = OpenSettingsCommand, Text = string.Create(inv, $"{t.Integrated:0.#} LUFS · TP {t.TruePeak:0.#}"), Page = "norm" });
        AudioChips.Add(new() { Command = OpenSettingsCommand, Text = $"{s.Audio} {s.AudioBitrate}".TrimEnd(), Page = "audio" });
        var language = s.PreferredLanguage == AudioTrackSelector.Original ? l["chip.original"] : l["lang." + s.PreferredLanguage];
        var mode = s.TrackMode switch
        {
            AudioTrackMode.All => l["trackmode.all"],
            AudioTrackMode.PreferredOnly => l.F("chip.language_only", ("lang", language)),
            _ => $"{l["common.auto"]} · {language}",
        };
        AudioChips.Add(new() { Command = OpenSettingsCommand, Text = mode, Page = "norm" });
        var video = Caps.Supports(s.Video) || Caps == EncoderCapabilities.None ? s.Video : VideoModes.Copy;
        VideoChips.Add(new() { Command = OpenSettingsCommand, Text = l.F("chip.video", ("mode", video.Replace("HEVC ", string.Empty))), Page = "video" });
        var format = FileNaming.IsExplicitFormat(s.OutputFormat) ? "." + s.OutputFormat : l["common.auto"];
        VideoChips.Add(new() { Command = OpenSettingsCommand, Text = $"{l["video.format"]}: {format}", Page = "video" });
        var jobs = s.JobsManual?.ToString(inv) ?? l["common.auto"];
        VideoChips.Add(new() { Command = OpenSettingsCommand, Text = l.F("chip.jobs", ("jobs", jobs)), Page = "jobs" });
        JobChips.Add(new() { Command = OpenSettingsCommand, Text = l.F("chip.resume", ("state", s.ResumeJobs ? l["common.on"] : l["common.off"])), Page = "jobs" });
        if (s.OverwriteOriginal) JobChips.Add(new() { Command = OpenSettingsCommand, Text = "⚠ " + l["chip.overwrite"], Page = "jobs", IsWarning = true });
        JobChips.Add(new()
        {
            Text = l[s.AnalyzeOnly ? "chip.analyze_only" : "mode.normalize"],
            IsWarning = s.AnalyzeOnly,
            IsNormalizeMode = !s.AnalyzeOnly,
            Command = ToggleAnalysisModeCommand,
            TooltipKey = "tooltip.toggle_mode",
        });
        ProfileText = string.IsNullOrEmpty(s.ActiveProfile) ? string.Empty : l.F("summary.profile", ("name", s.ActiveProfile));
    }

    // ===================================================================
    // Dateiliste
    // ===================================================================
    [RelayCommand]
    private async Task AddFiles()
    {
        if (IsRunning) return;
        var files = await Host.PickFilesAsync(Loc.Instance["picker.video_files"]);
        AddPaths(files);
    }

    [RelayCommand]
    private async Task AddFolder()
    {
        if (IsRunning) return;
        var folder = await Host.PickFolderAsync(Loc.Instance["picker.folder"], Settings.Input);
        if (folder == null) return;
        Settings.Input = folder;
        SaveSettings();
        AddPaths(new[] { folder });
    }

    /// <summary>Dateien und/oder Ordner hinzufuegen (Dialog oder Drag &amp; Drop).</summary>
    public void AddPaths(IEnumerable<string> paths)
    {
        var list = paths.ToList();
        if (IsRunning || list.Count == 0) return;
        paths = list;
        var added = 0;
        var dupes = 0;
        var fromFolder = false;
        HashSet<string>? knownOutputs = null;
        foreach (var p in paths)
        {
            if (Directory.Exists(p))
            {
                fromFolder = true;
                try { knownOutputs ??= new ResumeStore().KnownOutputs(); }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { knownOutputs = new(); }
                foreach (var f in FileNaming.CollectVideosFromFolder(p))
                {
                    if (knownOutputs.Contains(ResumeStore.Key(f))) continue;
                    if (AddFile(f, p)) added++;
                    else dupes++;
                }
            }
            else if (File.Exists(p) && FileNaming.IsVideoFile(p))
            {
                if (AddFile(p, Path.GetDirectoryName(p) ?? p)) added++;
                else dupes++;
            }
        }

        if (added == 0 && dupes == 0)
            AddLog(LogLevel.Warning, Loc.Instance["files.none_found"]);
        else if (added > 0)
            AddLog(LogLevel.Info, Loc.Instance.F(fromFolder ? "files.added_folder" : "files.added", ("count", added)));
        if (dupes > 0)
            AddLog(LogLevel.Muted, Loc.Instance.F("files.duplicates", ("count", dupes)));

        _lastSummary = null;
        SelectedFile ??= Files.FirstOrDefault();
        RefreshStatus();
    }

    private bool AddFile(string path, string root)
    {
        var full = Path.GetFullPath(path);
        var key = ResumeStore.Key(full);
        if (_fileIndex.ContainsKey(key)) return false;
        var item = new FileItemViewModel(full, root);
        item.RefreshTexts();
        _fileIndex[key] = item;
        Files.Add(item);
        _ = ProbeFileAsync(item);
        return true;
    }

    private async Task ProbeFileAsync(FileItemViewModel item)
    {
        if (FfprobePath == null)
        {
            item.InfoLoaded = true;
            item.RefreshTexts();
            return;
        }

        await _probeGate.WaitAsync();
        try
        {
            var info = await Task.Run(() => new MediaProbe(FfprobePath).ProbeAsync(item.Path));
            item.Info = info;
        }
        catch
        {
            item.Info = null;
        }
        finally
        {
            _probeGate.Release();
            item.InfoLoaded = true;
        }

        item.RefreshTexts();
        if (item == SelectedFile) await RefreshTracksAsync();
    }

    [RelayCommand(CanExecute = nameof(CanRemove))]
    private void RemoveSelected(IList? selection)
    {
        if (IsRunning) return;
        var items = selection?.OfType<FileItemViewModel>().ToList() ?? new List<FileItemViewModel>();
        if (items.Count == 0 && SelectedFile != null) items.Add(SelectedFile);
        foreach (var i in items)
        {
            Files.Remove(i);
            _fileIndex.Remove(ResumeStore.Key(i.Path));
        }
        SelectedFile = Files.FirstOrDefault();
        RefreshStatus();
    }

    private bool CanRemove(IList? selection) => !IsRunning && (SelectedFile != null || selection is { Count: > 0 });

    [RelayCommand]
    private void ClearList()
    {
        if (IsRunning) return;
        Files.Clear();
        _fileIndex.Clear();
        SelectedFile = null;
        _lastSummary = null;
        Tracks.Clear();
        OnPropertyChanged(nameof(HasTracks));
        RefreshStatus();
    }

    [RelayCommand]
    private void RevealFile(FileItemViewModel? item)
    {
        item ??= SelectedFile;
        if (item == null) return;
        var target = item.Result != null && File.Exists(item.Result.Output) ? item.Result.Output : item.Path;
        PlatformHelpers.RevealFile(target);
    }

    // ===================================================================
    // Audiospur-Vorschau
    // ===================================================================
    private async Task RefreshTracksAsync()
    {
        var item = SelectedFile;
        Tracks.Clear();
        OnPropertyChanged(nameof(HasTracks));
        if (item == null)
        {
            TracksFile = string.Empty;
            TracksSummary = Loc.Instance["tracks.none"];
            return;
        }

        TracksFile = item.Name;
        if (!item.InfoLoaded)
        {
            TracksLoading = true;
            TracksSummary = Loc.Instance["tracks.loading"];
            return; // ProbeFileAsync ruft erneut auf
        }

        TracksLoading = false;
        var streams = item.Info?.AudioStreams ?? Array.Empty<AudioStreamInfo>();
        if (streams.Count == 0)
        {
            TracksSummary = Loc.Instance["tracks.unavailable"];
            return;
        }

        var l = Loc.Instance;
        var sel = new AudioTrackSelector(Settings.TrackMode, Settings.PreferredLanguage, Settings.PreferFirst);
        var selected = sel.SelectedIndices(streams).ToHashSet();
        var order = sel.OutputOrder(streams);
        var def = sel.DefaultOutputIndex(streams, order);

        for (var o = 0; o < order.Count; o++)
        {
            var s = streams[order[o]];
            var flags = new List<string>();
            if (s.IsDefault) flags.Add(l["tracks.flag.orig_default"]);
            if (s.IsForced) flags.Add(l["tracks.flag.forced"]);
            if (def == o) flags.Add(l["tracks.flag.new_default"]);
            Tracks.Add(new AudioTrackRowViewModel
            {
                Out = (o + 1).ToString(CultureInfo.InvariantCulture),
                Language = string.IsNullOrWhiteSpace(s.Language) ? "-" : s.Language,
                Title = string.IsNullOrWhiteSpace(s.Title) ? "-" : s.Title,
                Format = s.FormatDescription(),
                Flags = flags.Count > 0 ? string.Join(", ", flags) : "-",
                Action = selected.Contains(order[o]) ? l["tracks.action.normalize"] : l["tracks.action.copy"],
                Normalize = selected.Contains(order[o]),
                IsNewDefault = def == o,
            });
        }
        OnPropertyChanged(nameof(HasTracks));
        TracksSummary = l.F("tracks.summary", ("selected", selected.Count), ("total", streams.Count));
        await Task.CompletedTask;
    }

    // ===================================================================
    // Ausgabe
    // ===================================================================
    [RelayCommand]
    private async Task BrowseOutput()
    {
        var folder = await Host.PickFolderAsync(Loc.Instance["picker.output"], OutputFolder);
        if (folder != null) OutputFolder = folder;
    }

    [RelayCommand]
    private async Task OpenOutput()
    {
        var path = OverwriteActive ? Files.FirstOrDefault()?.Root ?? OutputFolder : OutputFolder;
        if (string.IsNullOrWhiteSpace(path))
        {
            await BrowseOutput();
            return;
        }
        if (!PlatformHelpers.OpenFolder(path))
            await DialogWindow.ShowMessageAsync(Host.Window, Loc.Instance["dlg.error"], Loc.Instance.F("error.open_folder", ("path", path)), DialogKind.Error);
    }

    // ===================================================================
    // Verarbeitung
    // ===================================================================
    [RelayCommand]
    private async Task Start()
    {
        if (IsRunning) return;
        var l = Loc.Instance;
        ResolveTools();

        if (FfmpegPath == null)
        {
            await DialogWindow.ShowMessageAsync(Host.Window, l["dlg.error"], l["error.ffmpeg_missing"], DialogKind.Error);
            return;
        }
        if (FfprobePath == null)
        {
            await DialogWindow.ShowMessageAsync(Host.Window, l["dlg.error"], l["error.ffprobe_missing"], DialogKind.Error);
            return;
        }
        if (Files.Count == 0)
        {
            await DialogWindow.ShowMessageAsync(Host.Window, l["dlg.info"], l["error.no_files"]);
            return;
        }
        if (!Settings.OverwriteOriginal && !Settings.AnalyzeOnly && string.IsNullOrWhiteSpace(OutputFolder))
        {
            await BrowseOutput();
            if (string.IsNullOrWhiteSpace(OutputFolder))
            {
                await DialogWindow.ShowMessageAsync(Host.Window, l["dlg.info"], l["error.no_output"]);
                return;
            }
        }
        if (Settings.AfterCompletion == "shutdown" && !Settings.AnalyzeOnly &&
            !await DialogWindow.ConfirmAsync(Host.Window, l["dlg.confirm"], l["confirm.shutdown"]))
        {
            return;
        }

        if (Caps.FfmpegVersion.Length == 0)
        {
            Caps = await EncoderDetector.DetectAsync(FfmpegPath);
            RefreshEncoderChips();
        }

        ProcessingOptions options;
        try
        {
            options = ProcessingOptions.FromSettings(Settings, FfmpegPath, FfprobePath, Caps, OutputFolder.Trim());
        }
        catch (Exception ex)
        {
            await DialogWindow.ShowMessageAsync(Host.Window, l["dlg.error"], l.F("error.start", ("error", ex.Message)), DialogKind.Error);
            return;
        }

        // Zuruecksetzen
        _lastSummary = null;
        Log.Clear();
        ActiveJobs.Clear();
        NotifyJobs();
        foreach (var f in Files)
        {
            f.Result = null;
            f.State = FileState.Queued;
            f.RefreshTexts();
        }
        OverallPercent = 0;
        PercentText = "0 %";
        FilesText = $"0 / {Files.Count}";
        EtaText = "--:--";
        ElapsedText = "00:00";
        SetState("progress.state.running");
        IsRunning = true;
        IsCancelling = false;
        StatusText = l.F("status.processing", ("done", 0), ("total", Files.Count));
        _runStarted = DateTime.UtcNow;
        _elapsedTimer.Start();
        TaskbarProgress.Set(Host.NativeHandle, 0);

        _cts = new CancellationTokenSource();
        var sources = Files.Select(f => new SourceFile(f.Path, f.Root)).ToList();
        var observer = new UiObserver(this);
        BatchSummary? summary = null;
        try
        {
            var processor = new BatchProcessor(options, observer);
            summary = await Task.Run(() => processor.RunAsync(sources, _cts.Token));
        }
        catch (Exception ex)
        {
            AppPaths.WriteCrashLog(ex.ToString());
            AddLog(LogLevel.Error, ex.Message);
            await DialogWindow.ShowMessageAsync(Host.Window, l["dlg.error"], l.F("error.start", ("error", ex.Message)), DialogKind.Error);
        }
        finally
        {
            _elapsedTimer.Stop();
            IsRunning = false;
            IsCancelling = false;
            ActiveJobs.Clear();
            NotifyJobs();
            _cts.Dispose();
            _cts = null;
        }

        // Alle noch ausstehenden Log-/Status-Meldungen der Worker zuerst anzeigen
        await Dispatcher.UIThread.InvokeAsync(() => { }, DispatcherPriority.Background);
        if (summary != null) Finish(summary);
        else
        {
            SetState("progress.state.ready");
            TaskbarProgress.Clear(Host.NativeHandle);
            RefreshStatus();
        }
    }

    private void Finish(BatchSummary s)
    {
        var l = Loc.Instance;
        _lastSummary = s;
        var elapsed = FormatElapsed(s.Elapsed);
        ElapsedText = elapsed;

        if (s.Cancelled)
        {
            AddLog(LogLevel.Warning, l["log.aborted"]);
            SetState("progress.state.cancelled");
            TaskbarProgress.Set(Host.NativeHandle, OverallPercent / 100, TaskbarProgress.State.Paused);
            Host.Notify(l["notify.cancelled"], string.Empty, NotificationType.Warning);
            foreach (var f in Files.Where(f => f.State is FileState.Analyzing or FileState.Encoding)) f.State = FileState.Cancelled;
            RefreshStatus();
            return;
        }

        OverallPercent = 100;
        PercentText = "100 %";
        EtaText = "00:00";
        SetState("progress.state.completed");
        AddLog(LogLevel.Accent, l.F("log.summary", ("ok", s.Ok), ("skipped", s.Skipped + s.ResumeSkipped), ("errors", s.Errors), ("elapsed", elapsed)));
        if (s.RunLogPath != null) AddLog(LogLevel.Muted, l.F("log.runlog", ("path", s.RunLogPath)));
        RefreshStatus();

        TaskbarProgress.Set(Host.NativeHandle, 1, s.Errors > 0 ? TaskbarProgress.State.Error : TaskbarProgress.State.Normal);
        TaskbarProgress.Flash(Host.NativeHandle);
        if (Settings.NotifyOnFinish)
        {
            Host.Notify(l["notify.done.title"],
                l.F("notify.done.text", ("ok", s.Ok), ("skipped", s.Skipped + s.ResumeSkipped), ("errors", s.Errors), ("elapsed", elapsed)),
                s.Errors > 0 ? NotificationType.Warning : NotificationType.Success);
        }

        if (!Settings.AnalyzeOnly)
        {
            switch (Settings.AfterCompletion)
            {
                case "sleep":
                    AddLog(LogLevel.Warning, l["log.power_sleep"]);
                    PowerActions.Sleep();
                    break;
                case "shutdown":
                    AddLog(LogLevel.Warning, l["log.power_shutdown"]);
                    PowerActions.Shutdown();
                    break;
            }
        }
    }

    [RelayCommand]
    private void Cancel()
    {
        if (!IsRunning || _cts == null || IsCancelling) return;
        IsCancelling = true;
        SetState("progress.state.cancelling");
        StatusText = Loc.Instance["status.cancelling"];
        AddLog(LogLevel.Warning, Loc.Instance["log.cancel_requested"]);
        _cts.Cancel();
    }

    /// <summary>Vor dem Schliessen: laufende Verarbeitung abbrechen?</summary>
    public async Task<bool> ConfirmCloseAsync()
    {
        if (!IsRunning) return true;
        if (!await DialogWindow.ConfirmAsync(Host.Window, Loc.Instance["dlg.confirm"], Loc.Instance["confirm.close_running"]))
            return false;
        _cts?.Cancel();
        // kurz warten, damit ffmpeg-Prozesse beendet werden
        for (var i = 0; i < 50 && IsRunning; i++) await Task.Delay(100);
        return true;
    }

    public void SaveWindowState(double width, double height, int? x, int? y, bool maximized)
    {
        Settings.WindowWidth = width;
        Settings.WindowHeight = height;
        Settings.WindowX = x;
        Settings.WindowY = y;
        Settings.WindowMaximized = maximized;
        SaveSettings();
    }

    // ===================================================================
    // Beobachter (Hintergrund-Thread -> UI)
    // ===================================================================
    private sealed class UiObserver : IBatchObserver
    {
        private readonly MainWindowViewModel _vm;
        public UiObserver(MainWindowViewModel vm) => _vm = vm;

        private static void Ui(Action a) => Dispatcher.UIThread.Post(a);

        public void Log(LogLevel level, string text) => Ui(() => _vm.AddLog(level, text));

        public void JobStarted(Guid jobId, string displayName, string phase) => Ui(() =>
        {
            var used = _vm.ActiveJobs.Select(j => j.Number).ToHashSet();
            var n = 1;
            while (used.Contains(n)) n++;
            var job = new ActiveJobViewModel(jobId, n, displayName, phase);
            var idx = 0;
            while (idx < _vm.ActiveJobs.Count && _vm.ActiveJobs[idx].Number < n) idx++;
            _vm.ActiveJobs.Insert(idx, job);
            _vm.NotifyJobs();
        });

        public void JobProgress(Guid jobId, string phase, int percent) => Ui(() =>
        {
            var job = _vm.ActiveJobs.FirstOrDefault(j => j.Id == jobId);
            if (job == null) return;
            if (job.Phase != phase)
            {
                job.Phase = phase;
                job.Percent = 0;
            }
            job.Percent = percent;
        });

        public void JobEnded(Guid jobId) => Ui(() =>
        {
            var job = _vm.ActiveJobs.FirstOrDefault(j => j.Id == jobId);
            if (job != null) _vm.ActiveJobs.Remove(job);
            _vm.NotifyJobs();
        });

        public void FileStateChanged(string file, FileState state, FileResult? result) => Ui(() =>
        {
            if (!_vm._fileIndex.TryGetValue(ResumeStore.Key(file), out var item)) return;
            if (result != null) item.Result = result;
            item.State = state;
            item.RefreshTexts();
        });

        public void OverallProgress(double fraction, int completed, int total, TimeSpan? eta) => Ui(() =>
        {
            _vm.OverallPercent = fraction * 100;
            _vm.PercentText = $"{(int)(fraction * 100)} %";
            _vm.FilesText = $"{completed} / {total}";
            _vm.EtaText = fraction >= 0.999 ? "00:00" : eta.HasValue ? FormatElapsed(eta.Value) : "--:--";
            if (_vm.IsRunning && !_vm.IsCancelling)
                _vm.StatusText = Loc.Instance.F("status.processing", ("done", completed), ("total", total));
            if (_vm._host != null) TaskbarProgress.Set(_vm._host.NativeHandle, fraction);
        });
    }

    private void NotifyJobs()
    {
        OnPropertyChanged(nameof(HasActiveJobs));
        OnPropertyChanged(nameof(HasNoActiveJobs));
    }

    // ===================================================================
    // Log
    // ===================================================================
    public void AddLog(LogLevel level, string text)
    {
        foreach (var line in text.Replace("\r", string.Empty).Split('\n'))
            Log.Add(new LogEntryViewModel(level, line));
        while (Log.Count > 5000) Log.RemoveAt(0);
        if (LogAutoScroll) _host?.ScrollLogToEnd();
    }

    private string LogText() => string.Join(Environment.NewLine, Log.Select(e => e.ToString()));

    [RelayCommand]
    private async Task CopyLog() => await Host.SetClipboardAsync(LogText());

    [RelayCommand]
    private async Task SaveLog()
    {
        var path = await Host.SaveTextFileAsync(Loc.Instance["log.save"], $"loudnorm_log_{DateTime.Now:yyyyMMdd_HHmmss}.txt");
        if (path == null) return;
        await File.WriteAllTextAsync(path, LogText(), new UTF8Encoding(false));
        AddLog(LogLevel.Muted, Loc.Instance.F("log.saved", ("path", path)));
    }

    [RelayCommand]
    private void ClearLog() => Log.Clear();

    // ===================================================================
    // Einstellungen
    // ===================================================================
    [RelayCommand]
    private async Task OpenSettings(string? page)
    {
        var vm = new SettingsViewModel(Settings, Caps, FfmpegPath, FfprobePath);
        vm.SelectPage(page ?? "norm");
        if (!await Host.ShowSettingsAsync(vm)) return;

        var toolsChanged = vm.FfmpegPath.Trim() != Settings.FfmpegPath || vm.FfprobePath.Trim() != Settings.FfprobePath;
        vm.ApplyTo(Settings);
        OutputFolder = Settings.Output;
        SaveSettings();
        if (toolsChanged || ToolsMissing)
        {
            ResolveTools();
            Caps = await EncoderDetector.DetectAsync(FfmpegPath);
            RefreshEncoderChips();
            foreach (var f in Files.Where(f => f.Info == null))
            {
                f.InfoLoaded = false;
                _ = ProbeFileAsync(f);
            }
        }
        RefreshAllTexts();
        Host.RebuildMenu();
    }

    public void SaveSettings() => SettingsStore.Save(Settings);

    // ----- Sprache -----
    public string CurrentLanguage => Loc.Instance.Catalog.CurrentCode;

    public void SetLanguage(string code)
    {
        Settings.UiLanguage = code;
        SaveSettings();
        Loc.Instance.Catalog.SetLanguage(code);
        Host.RebuildMenu();
    }

    [RelayCommand]
    private void OpenLanguageFolder() => PlatformHelpers.OpenFolder(AppPaths.UserLanguagesDir);

    [RelayCommand]
    private async Task CreateLanguageTemplate()
    {
        var l = Loc.Instance;
        var code = await DialogWindow.PromptAsync(Host.Window, l["language.template.title"], l["language.template.prompt"]);
        if (code == null) return;
        var path = l.Catalog.WriteTemplate(code, code);
        PlatformHelpers.RevealFile(path);
        await DialogWindow.ShowMessageAsync(Host.Window, l["language.template.title"], l.F("language.template.created", ("path", path)));
    }

    [RelayCommand]
    private void ReloadLanguages()
    {
        var catalog = LanguageCatalog.LoadDefault(typeof(App).Assembly);
        Loc.Instance.Attach(catalog);
        catalog.SetLanguage(Settings.UiLanguage);
        Loc.Instance.Raise();
        AddLog(LogLevel.Info, Loc.Instance.F("language.reloaded", ("count", catalog.Languages.Count)));
        if (catalog.LoadErrors.Count > 0)
            AddLog(LogLevel.Warning, Loc.Instance.F("log.languages_errors", ("errors", string.Join("; ", catalog.LoadErrors))));
        Host.RebuildMenu();
    }

    // ----- Design -----
    public string CurrentTheme => Settings.UiTheme;

    public void ApplyTheme(string theme, bool save = true)
    {
        Settings.UiTheme = theme;
        if (Application.Current != null)
        {
            Application.Current.RequestedThemeVariant = theme switch
            {
                "light" => ThemeVariant.Light,
                "system" => ThemeVariant.Default,
                _ => ThemeVariant.Dark,
            };
        }
        if (save)
        {
            SaveSettings();
            _host?.RebuildMenu();
        }
    }

    // ----- Profile -----
    [RelayCommand]
    private async Task SaveProfile()
    {
        var l = Loc.Instance;
        var name = await DialogWindow.PromptAsync(Host.Window, l["profile.save.title"], l["profile.save.prompt"], Settings.ActiveProfile);
        if (name == null) return;
        Settings.Profiles.RemoveAll(p => string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase));
        Settings.Profiles.Add(ProcessingProfile.From(Settings, name));
        Settings.Profiles.Sort((a, b) => string.Compare(a.Name, b.Name, StringComparison.CurrentCultureIgnoreCase));
        Settings.ActiveProfile = name;
        SaveSettings();
        RefreshChips();
        Host.RebuildMenu();
        AddLog(LogLevel.Success, l.F("profile.saved", ("name", name)));
    }

    public void ApplyProfile(string name)
    {
        var p = Settings.Profiles.FirstOrDefault(x => x.Name == name);
        if (p == null) return;
        p.ApplyTo(Settings);
        SaveSettings();
        RefreshAllTexts();
        Host.RebuildMenu();
        AddLog(LogLevel.Info, Loc.Instance.F("profile.applied", ("name", name)));
    }

    public void DeleteProfile(string name)
    {
        Settings.Profiles.RemoveAll(x => x.Name == name);
        if (Settings.ActiveProfile == name) Settings.ActiveProfile = string.Empty;
        SaveSettings();
        RefreshChips();
        Host.RebuildMenu();
        AddLog(LogLevel.Info, Loc.Instance.F("profile.deleted", ("name", name)));
    }

    // ===================================================================
    // Info-Menue
    // ===================================================================
    [RelayCommand]
    private async Task ShowBuildInfo()
    {
        var l = Loc.Instance;
        var rows = BuildInfoRows();
        var d = new DialogWindow(l["build.title"], 640);
        d.AddKeyValues(rows);
        d.AddLeftButton(l["common.copy"], async () =>
        {
            await Host.SetClipboardAsync(string.Join(Environment.NewLine, rows.Select(r => $"{r.Key}: {r.Value}")));
            Host.Notify(l["build.title"], l["build.copied"], NotificationType.Information);
        });
        d.AddButton(l["common.close"], null, primary: true, isDefault: true, isCancel: true);
        await d.ShowDialog(Host.Window);
    }

    private List<(string Key, string Value)> BuildInfoRows()
    {
        var l = Loc.Instance;
        var enc = string.Join(", ", Caps.AvailableModes());
        return new()
        {
            (l["build.version"], AppInfo.Version),
            (l["build.date"], AppInfo.BuildDate),
            (l["build.runtime"], AppInfo.RuntimeDescription),
            (l["build.platform"], PlatformHelpers.PlatformName),
            (l["build.ffmpeg"], FfmpegPath ?? l["build.not_found"]),
            (l["build.ffprobe"], FfprobePath ?? l["build.not_found"]),
            (l["build.ffmpeg_version"], Caps.FfmpegVersion.Length > 0 ? Caps.FfmpegVersion : "-"),
            (l["build.encoders"], enc),
            (l["build.gpu"], Caps.GpuVendor.Length > 0 ? Caps.GpuVendor : "-"),
            (l["build.languages"], string.Join(", ", l.Catalog.Languages.Select(x => $"{x.Name} ({x.Code})"))),
            (l["build.data"], AppPaths.DataDir),
            (l["build.repo"], AppInfo.RepoUrl),
            (l["build.license"], AppInfo.License),
        };
    }

    [RelayCommand]
    private Task CheckUpdates() => CheckUpdatesInternalAsync(manual: true);

    private async Task CheckUpdatesInternalAsync(bool manual)
    {
        var l = Loc.Instance;
        if (manual) AddLog(LogLevel.Muted, l["update.checking"]);
        ReleaseInfo release;
        try
        {
            release = await UpdateChecker.GetLatestAsync();
        }
        catch (Exception ex)
        {
            if (manual)
                await DialogWindow.ShowMessageAsync(Host.Window, l["update.title"], l.F("update.failed", ("error", ex.Message)), DialogKind.Warning);
            return;
        }

        if (!UpdateChecker.IsNewer(release.Version, AppInfo.Version))
        {
            if (manual)
                await DialogWindow.ShowMessageAsync(Host.Window, l["update.title"], l.F("update.uptodate", ("version", AppInfo.Version)));
            return;
        }

        if (!manual && IsRunning) return;

        var d = new DialogWindow(l["update.title"], 600);
        d.AddText(l.F("update.available", ("version", release.Version)), "h3");
        d.AddText(l.F("update.available.hint", ("current", AppInfo.Version)), "muted");
        if (!string.IsNullOrWhiteSpace(release.Body))
        {
            d.AddText(l["update.notes"], "label");
            d.AddControl(new Avalonia.Controls.Border { Classes = { "inset" }, Padding = new Thickness(12), Child = DialogWindow.Markdownish(release.Body) });
        }
        d.AddButton(l["common.close"], false, isCancel: true);
        d.AddButton(l["update.download"], true, primary: true, isDefault: true);
        var r = await d.ShowDialog<object?>(Host.Window);
        if (r is true) PlatformHelpers.OpenUrl(release.HtmlUrl);
    }

    [RelayCommand]
    private async Task ShowChangelog()
    {
        var text = ReadEmbedded("CHANGELOG.md") ?? "-";
        var d = new DialogWindow(Loc.Instance["changelog.title"], 640);
        d.AddControl(DialogWindow.Markdownish(text));
        d.AddButton(Loc.Instance["common.close"], null, primary: true, isDefault: true, isCancel: true);
        await d.ShowDialog(Host.Window);
    }

    private static string? ReadEmbedded(string name)
    {
        var asm = Assembly.GetExecutingAssembly();
        using var s = asm.GetManifestResourceStream(name);
        if (s == null) return null;
        using var r = new StreamReader(s);
        return r.ReadToEnd();
    }

    [RelayCommand]
    private void OpenRepo() => PlatformHelpers.OpenUrl(AppInfo.RepoUrl);

    [RelayCommand]
    private void OpenDataFolder() => PlatformHelpers.OpenFolder(AppPaths.DataDir);

    [RelayCommand]
    private async Task ShowDiagnostics()
    {
        var l = Loc.Instance;
        var d = new DialogWindow(l["diag.title"], 640);
        var status = new Avalonia.Controls.TextBlock { Text = l["diag.running"], Classes = { "muted" } };
        d.AddControl(status);
        var rows = new Avalonia.Controls.StackPanel();
        d.AddControl(rows);

        async Task Run()
        {
            status.Text = l["diag.running"];
            rows.Children.Clear();
            ResolveTools();
            var rep = await Task.Run(() => FfmpegDiagnostics.RunAsync(FfmpegPath, FfprobePath));
            rows.Children.Add(DialogWindow.BuildKeyValues(new List<(string, string)>
            {
                (l["build.ffmpeg"], rep.FfmpegPath ?? l["build.not_found"]),
                (l["build.ffprobe"], rep.FfprobePath ?? l["build.not_found"]),
                (l["build.ffmpeg_version"], rep.FfmpegVersion),
                (l["build.encoders"], string.Join(", ", rep.Encoders.AvailableModes())),
                (l["build.gpu"], rep.Encoders.GpuVendor.Length > 0 ? rep.Encoders.GpuVendor : "-"),
                (l["diag.test"], (rep.LoudnormTestPassed ? "✓ " + l["diag.passed"] : "✗ " + l["diag.failed"]) + Environment.NewLine + rep.LoudnormTestDetails),
                (l["diag.duration"], $"{rep.TestDuration.TotalSeconds:0.0} s"),
            }));
            status.Text = rep.LoudnormTestPassed ? "✓ " + l["diag.passed"] : "✗ " + l["diag.failed"];
            status.Classes.Set("ok", rep.LoudnormTestPassed);
            status.Classes.Set("err", !rep.LoudnormTestPassed);
            Caps = rep.Encoders;
            RefreshEncoderChips();
        }

        d.AddLeftButton(l["diag.rerun"], () => _ = Run());
        d.AddButton(l["common.close"], null, primary: true, isDefault: true, isCancel: true);
        d.Opened += (_, _) => _ = Run();
        await d.ShowDialog(Host.Window);
    }

    [RelayCommand]
    private async Task ShowAbout()
    {
        var l = Loc.Instance;
        var d = new DialogWindow(l["about.title"], 480);
        d.AddText($"Loudnorm PRO {AppInfo.Version}", "h3");
        d.AddText(l["about.text"]);
        d.AddText($"{AppInfo.License} · {l["about.credits"]}", "muted");
        d.AddLeftButton("GitHub", () => PlatformHelpers.OpenUrl(AppInfo.RepoUrl), "link");
        d.AddButton(l["common.close"], null, primary: true, isDefault: true, isCancel: true);
        await d.ShowDialog(Host.Window);
    }

    [RelayCommand]
    private void Exit() => Host.Window.Close();

    [RelayCommand]
    private void SelectTab(string? tab) => IsLogTab = tab != "tracks";

    // ===================================================================
    // Hilfsfunktionen
    // ===================================================================
    public static string FormatElapsed(TimeSpan t) =>
        t.TotalHours >= 1 ? $"{(int)t.TotalHours}:{t.Minutes:00}:{t.Seconds:00}" : $"{t.Minutes:00}:{t.Seconds:00}";
}
