using System.Collections.ObjectModel;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using LoudnormPro.App.Localization;
using LoudnormPro.Core.Platform;
using LoudnormPro.Core.Media;
using LoudnormPro.Core.Processing;
using LoudnormPro.Core.Settings;

namespace LoudnormPro.App.ViewModels;

/// <summary>Auswahl-Eintrag fuer ComboBoxen (Schluessel + uebersetzte Anzeige).</summary>
public sealed record OptionItem(string Key, string Display)
{
    public override string ToString() => Display;
}

public sealed record SettingsPage(string Key, string Title, string IconKey);

/// <summary>
/// Bearbeitbare Kopie der Einstellungen fuer den Einstellungsdialog.
/// Erst mit OK (<see cref="ApplyTo"/>) werden die Werte uebernommen.
/// </summary>
public sealed partial class SettingsViewModel : ObservableObject
{
    private readonly EncoderCapabilities _caps;
    private bool _syncingProfile;

    public SettingsViewModel(AppSettings source, EncoderCapabilities caps, string? resolvedFfmpeg, string? resolvedFfprobe)
    {
        _caps = caps;
        var l = Loc.Instance;

        Pages = new()
        {
            new("norm", l["settings.norm.title"], "IconWave"),
            new("audio", l["settings.audio.title"], "IconMusic"),
            new("video", l["settings.video.title"], "IconVideo"),
            new("jobs", l["settings.jobs.title"], "IconCpu"),
            new("tools", l["settings.tools.title"], "IconTool"),
        };
        _selectedPage = Pages[0];

        ProfileOptions = LoudnessPresets.Keys.Select(k => new OptionItem(k, l["profile." + k])).ToList();
        TrackModeOptions = new()
        {
            new("auto", l["trackmode.auto"]),
            new("all", l["trackmode.all"]),
            new("preferred_only", l["trackmode.preferred_only"]),
        };
        PreferredLanguageOptions = AudioTrackSelector.SelectableLanguages.Select(k => new OptionItem(k, l["lang." + k])).ToList();
        JobOptions = AppSettings.JobOptions.Select(j => new OptionItem(j, j == "Auto" ? l["common.auto"] : j)).ToList();
        AfterOptions = new()
        {
            new("none", l["after.none"]),
            new("sleep", l["after.sleep"]),
            new("shutdown", l["after.shutdown"]),
        };

        EncoderChips = new()
        {
            new() { Name = VideoModes.AvcNvenc, Available = caps.AvcNvenc },
            new() { Name = VideoModes.X264, Available = caps.X264 },
            new() { Name = "HEVC NVENC", Available = caps.Nvenc },
            new() { Name = "HEVC AMF", Available = caps.Amf },
            new() { Name = "HEVC VAAPI", Available = caps.Vaapi },
            new() { Name = "VideoToolbox", Available = caps.VideoToolbox },
            new() { Name = "x265 (CPU)", Available = caps.X265 },
        };
        VideoModeOptions = caps.AvailableModes().Select(m => new OptionItem(m, m)).ToList();

        ResolvedFfmpeg = resolvedFfmpeg;
        ResolvedFfprobe = resolvedFfprobe;
        DataDir = AppPaths.DataDir;

        LoadFrom(source);
    }

    // ----- Seiten -----
    public List<SettingsPage> Pages { get; }
    [ObservableProperty] private SettingsPage _selectedPage;
    public bool IsNorm => SelectedPage.Key == "norm";
    public bool IsAudio => SelectedPage.Key == "audio";
    public bool IsVideo => SelectedPage.Key == "video";
    public bool IsJobs => SelectedPage.Key == "jobs";
    public bool IsTools => SelectedPage.Key == "tools";

    partial void OnSelectedPageChanged(SettingsPage value)
    {
        OnPropertyChanged(nameof(IsNorm));
        OnPropertyChanged(nameof(IsAudio));
        OnPropertyChanged(nameof(IsVideo));
        OnPropertyChanged(nameof(IsJobs));
        OnPropertyChanged(nameof(IsTools));
    }

    public void SelectPage(string key)
    {
        var p = Pages.FirstOrDefault(x => x.Key == key);
        if (p != null) SelectedPage = p;
    }

    // ----- Normalisierung -----
    public List<OptionItem> ProfileOptions { get; }
    [ObservableProperty] private OptionItem? _selectedProfile;
    [ObservableProperty] private decimal? _targetI;
    [ObservableProperty] private decimal? _targetTp;
    [ObservableProperty] private decimal? _targetLra;

    public List<OptionItem> TrackModeOptions { get; }
    [ObservableProperty] private OptionItem? _selectedTrackMode;
    public List<OptionItem> PreferredLanguageOptions { get; }
    [ObservableProperty] private OptionItem? _selectedPreferredLanguage;
    [ObservableProperty] private bool _preferFirst;
    [ObservableProperty] private bool _analyzeOnly;

    public bool PreferFirstEnabled => SelectedTrackMode?.Key == "preferred_only";

    public string TrackModeHint
    {
        get
        {
            var lang = SelectedPreferredLanguage?.Display ?? string.Empty;
            return SelectedTrackMode?.Key switch
            {
                "all" => Loc.Instance["trackmode.all.hint"],
                "preferred_only" => Loc.Instance.F("trackmode.preferred_only.hint", ("lang", lang)),
                _ => Loc.Instance.F("trackmode.auto.hint", ("lang", lang)),
            };
        }
    }

    partial void OnSelectedTrackModeChanged(OptionItem? value)
    {
        OnPropertyChanged(nameof(PreferFirstEnabled));
        OnPropertyChanged(nameof(TrackModeHint));
    }

    partial void OnSelectedPreferredLanguageChanged(OptionItem? value) => OnPropertyChanged(nameof(TrackModeHint));

    partial void OnSelectedProfileChanged(OptionItem? value)
    {
        if (_syncingProfile || value == null) return;
        if (LoudnessPresets.All.TryGetValue(value.Key, out var t))
        {
            _syncingProfile = true;
            TargetI = (decimal)t.Integrated;
            TargetTp = (decimal)t.TruePeak;
            TargetLra = (decimal)t.Lra;
            _syncingProfile = false;
        }
    }

    partial void OnTargetIChanged(decimal? value) => SyncProfileFromValues();
    partial void OnTargetTpChanged(decimal? value) => SyncProfileFromValues();
    partial void OnTargetLraChanged(decimal? value) => SyncProfileFromValues();

    private void SyncProfileFromValues()
    {
        if (_syncingProfile) return;
        var key = LoudnessPresets.Detect(CurrentTarget());
        _syncingProfile = true;
        SelectedProfile = ProfileOptions.First(p => p.Key == key);
        _syncingProfile = false;
    }

    private LoudnessTarget CurrentTarget() =>
        new((double)(TargetI ?? -16), (double)(TargetTp ?? -1.5m), (double)(TargetLra ?? 11));

    public decimal MinI => (decimal)LoudnessTarget.MinI;
    public decimal MaxI => (decimal)LoudnessTarget.MaxI;
    public decimal MinTp => (decimal)LoudnessTarget.MinTp;
    public decimal MaxTp => (decimal)LoudnessTarget.MaxTp;
    public decimal MinLra => (decimal)LoudnessTarget.MinLra;
    public decimal MaxLra => (decimal)LoudnessTarget.MaxLra;

    // ----- Audio -----
    public string[] AudioCodecs => AppSettings.AudioCodecs;
    [ObservableProperty] private string _selectedCodec = "AAC";
    public IReadOnlyList<string> BitrateOptions => AppSettings.BitratesFor(SelectedCodec);
    public bool ShowAudioBitrate => BitrateOptions.Count > 0;
    [ObservableProperty] private string? _selectedBitrate;

    partial void OnSelectedCodecChanged(string value)
    {
        var previous = SelectedBitrate;
        OnPropertyChanged(nameof(BitrateOptions));
        OnPropertyChanged(nameof(ShowAudioBitrate));
        SelectedBitrate = previous != null && BitrateOptions.Contains(previous) ? previous : AppSettings.DefaultAudioBitrate(value);
        RefreshCompatibility();
    }

    // ----- Video -----
    public List<EncoderChipViewModel> EncoderChips { get; }
    public List<OptionItem> VideoModeOptions { get; }
    [ObservableProperty] private OptionItem? _selectedVideoMode;
    private static OptionItem VideoOption(string key, string label, string? hintKey = null) =>
        new(key, hintKey == null ? label : $"{label} – {Loc.Instance[hintKey]}");

    public List<OptionItem> PresetOptions { get; } = VideoModes.Presets
        .Select(p => VideoOption(p, p, p is "P7" or "P4" or "P1" ? "video.preset." + p : null)).ToList();
    [ObservableProperty] private OptionItem? _selectedPreset;
    public List<OptionItem> AmfPresets { get; } = new()
    {
        VideoOption("quality", "Quality", "video.preset.P7"),
        VideoOption("balanced", "Balanced", "video.preset.P4"),
        VideoOption("speed", "Speed", "video.preset.P1"),
    };
    [ObservableProperty] private OptionItem? _selectedAmfPreset;
    public bool ShowAmfPreset => SelectedVideoMode?.Key == VideoModes.Amf;
    public List<OptionItem> RateModes { get; } = new() { new("QP", "QP"), new("bitrate", Loc.Instance["video.bitrate"]) };
    public List<OptionItem> Formats { get; } = new() { new("auto", Loc.Instance["common.auto"]), new("mkv", "Matroska (.mkv)"), new("mp4", "MP4 (.mp4)"), new("mov", "QuickTime (.mov)"), new("ts", "MPEG-TS (.ts)"), new("webm", "WebM (.webm)") };
    public List<OptionItem> VideoRates { get; } = VideoModes.Bitrates.Select(n => VideoOption(n, $"{n} Mbit/s", "video.rate." + n))
        .Append(new OptionItem("custom", Loc.Instance["video.custom"])).ToList();
    public List<OptionItem> QpOptions { get; } = Enumerable.Range(14, 14)
        .Select(q => q.ToString(System.Globalization.CultureInfo.InvariantCulture))
        .Select(q => VideoOption(q, q, q is "14" or "21" or "27" ? "video.qp." + q : null)).ToList();
    [ObservableProperty] private OptionItem? _selectedQp;
    [ObservableProperty] private OptionItem? _selectedRateMode;
    [ObservableProperty] private OptionItem? _selectedVideoRate;
    [ObservableProperty] private OptionItem? _selectedFormat;
    [ObservableProperty] private decimal? _customMbps = 8;
    public bool IsCopyMode => SelectedVideoMode?.Key == VideoModes.Copy;
    public bool ShowPreset => SelectedVideoMode?.Key is VideoModes.Nvenc or VideoModes.AvcNvenc;
    public bool ShowRateMode => VideoModes.SupportsQp(SelectedVideoMode?.Key);
    public bool ShowQp => ShowRateMode && SelectedRateMode?.Key == "QP";
    public bool ShowBitrate => !IsCopyMode && !ShowQp;
    public bool ShowCustom => ShowBitrate && SelectedVideoRate?.Key == "custom";
    public List<OptionItem> TagModes { get; } = new[] { "default", "none", "custom" }
        .Select(k => new OptionItem(k, Loc.Instance["tag." + k])).ToList();
    [ObservableProperty] private OptionItem? _selectedTagMode;
    [ObservableProperty] private string _customTag = "_norm";
    public bool ShowCustomTag => SelectedTagMode?.Key == "custom";
    public bool InvalidTag => ShowCustomTag && !FileNaming.ValidTag(CustomTag);
    public bool IncompatibleFormat => !AnalyzeOnly && SelectedFormat is { Key: not "auto" } format &&
        (!FileNaming.SupportsAudio("." + format.Key, AppSettings.EncoderFor(SelectedCodec)) ||
         (format.Key == "webm" && !IsCopyMode));
    public bool CanApply => !InvalidTag && !IncompatibleFormat && (!ShowCustom || CustomMbps is >= 0.001m and <= 1000m);

    partial void OnSelectedTagModeChanged(OptionItem? value)
    {
        OnPropertyChanged(nameof(ShowCustomTag));
        OnCustomTagChanged(CustomTag);
    }
    partial void OnCustomTagChanged(string value)
    {
        OnPropertyChanged(nameof(InvalidTag));
        OnPropertyChanged(nameof(CanApply));
    }
    partial void OnSelectedFormatChanged(OptionItem? value) => RefreshCompatibility();
    partial void OnAnalyzeOnlyChanged(bool value) => RefreshCompatibility();
    private void RefreshCompatibility()
    {
        OnPropertyChanged(nameof(IncompatibleFormat));
        OnPropertyChanged(nameof(CanApply));
    }

    partial void OnSelectedVideoModeChanged(OptionItem? value)
    {
        RefreshCompatibility();
        OnPropertyChanged(nameof(ShowPreset));
        OnPropertyChanged(nameof(ShowAmfPreset));
        OnPropertyChanged(nameof(IsCopyMode));
        OnPropertyChanged(nameof(ShowRateMode));
        OnPropertyChanged(nameof(ParallelHint));
        RefreshRateVisibility();
    }

    partial void OnSelectedRateModeChanged(OptionItem? value) => RefreshRateVisibility();
    partial void OnSelectedVideoRateChanged(OptionItem? value)
        => RefreshRateVisibility();
    partial void OnCustomMbpsChanged(decimal? value) => OnPropertyChanged(nameof(CanApply));
    private void RefreshRateVisibility()
    {
        OnPropertyChanged(nameof(ShowQp));
        OnPropertyChanged(nameof(ShowBitrate));
        OnPropertyChanged(nameof(ShowCustom));
        OnPropertyChanged(nameof(CanApply));
    }

    // ----- Jobs -----
    public List<OptionItem> JobOptions { get; }
    [ObservableProperty] private OptionItem? _selectedJobs;
    [ObservableProperty] private bool _resume;
    [ObservableProperty] private bool _overwrite;
    [ObservableProperty] private bool _preserveTimestamp;
    public List<OptionItem> AfterOptions { get; }
    [ObservableProperty] private OptionItem? _selectedAfter;
    [ObservableProperty] private bool _notify;
    [ObservableProperty] private bool _checkUpdates;

    partial void OnSelectedJobsChanged(OptionItem? value) => OnPropertyChanged(nameof(ParallelHint));

    public string ParallelHint
    {
        get
        {
            var mode = SelectedVideoMode?.Key ?? VideoModes.Copy;
            int? manual = int.TryParse(SelectedJobs?.Key, out var n) ? n : null;
            var o = new ProcessingOptions
            {
                FfmpegPath = "", FfprobePath = "", OutputRoot = "",
                Target = LoudnessTarget.Default, AudioCodec = "aac", AudioBitrate = "384k",
                Tracks = new AudioTrackSelector(AudioTrackMode.Auto, "de", false),
                Video = new VideoPlan(mode, mode, Array.Empty<string>(), "", "", "", null),
                ManualJobs = manual,
            };
            return manual.HasValue
                ? Loc.Instance.F("settings.jobs.parallel.hint_manual", ("jobs", manual.Value))
                : Loc.Instance.F("settings.jobs.parallel.hint_auto", ("analysis", o.SplitPipeline ? o.AnalysisJobs : o.EncodeJobs), ("encode", o.EncodeJobs));
        }
    }

    // ----- Pfade -----
    [ObservableProperty] private string _ffmpegPath = string.Empty;
    [ObservableProperty] private string _ffprobePath = string.Empty;
    [ObservableProperty] private string _tempWorkDir = string.Empty;
    [ObservableProperty] private string _outputFolder = string.Empty;
    public string? ResolvedFfmpeg { get; private set; }
    public string? ResolvedFfprobe { get; private set; }
    public string DataDir { get; }
    [ObservableProperty] private string _infoMessage = string.Empty;

    public string FfmpegStatus => ResolveStatus(FfmpegPath, "ffmpeg", out var p) ? $"{Loc.Instance["settings.tools.found"]} · {p}" : Loc.Instance["settings.tools.not_found"];
    public string FfprobeStatus => ResolveStatus(FfprobePath, "ffprobe", out var p) ? $"{Loc.Instance["settings.tools.found"]} · {p}" : Loc.Instance["settings.tools.not_found"];
    public bool FfmpegFound => ResolveStatus(FfmpegPath, "ffmpeg", out _);
    public bool FfprobeFound => ResolveStatus(FfprobePath, "ffprobe", out _);

    partial void OnFfmpegPathChanged(string value)
    {
        OnPropertyChanged(nameof(FfmpegStatus));
        OnPropertyChanged(nameof(FfmpegFound));
    }

    partial void OnFfprobePathChanged(string value)
    {
        OnPropertyChanged(nameof(FfprobeStatus));
        OnPropertyChanged(nameof(FfprobeFound));
    }

    private static bool ResolveStatus(string overridePath, string tool, out string path)
    {
        path = ToolLocator.Resolve(tool, overridePath) ?? string.Empty;
        return path.Length > 0;
    }

    [RelayCommand]
    private void ClearResume()
    {
        new ResumeStore().Clear();
        InfoMessage = Loc.Instance["settings.tools.resume_cleared"];
    }

    [RelayCommand]
    private void OpenDataFolder() => Services.PlatformHelpers.OpenFolder(DataDir);

    [RelayCommand]
    private void RestoreDefaults() => LoadFrom(new AppSettings { UiLanguage = "x", UiTheme = "dark" }, keepPaths: true);

    // ----- Laden / Speichern -----
    private void LoadFrom(AppSettings s, bool keepPaths = false)
    {
        _syncingProfile = true;
        TargetI = (decimal)s.TargetI;
        TargetTp = (decimal)s.TargetTp;
        TargetLra = (decimal)s.TargetLra;
        SelectedProfile = ProfileOptions.FirstOrDefault(p => p.Key == s.LoudnessProfile) ?? ProfileOptions.Last();
        _syncingProfile = false;
        if (SelectedProfile.Key != LoudnessPresets.Custom) OnSelectedProfileChanged(SelectedProfile);

        SelectedTrackMode = TrackModeOptions.FirstOrDefault(o => o.Key == s.AudioTrackMode) ?? TrackModeOptions[0];
        SelectedPreferredLanguage = PreferredLanguageOptions.FirstOrDefault(o => o.Key == s.PreferredLanguage) ?? PreferredLanguageOptions[0];
        PreferFirst = s.PreferFirst;
        AnalyzeOnly = s.AnalyzeOnly;

        SelectedCodec = s.Audio;
        SelectedBitrate = s.AudioBitrate;
        SelectedTagMode = TagModes.FirstOrDefault(x => x.Key == s.TagMode) ?? TagModes[0];
        CustomTag = s.CustomTag;

        SelectedVideoMode = VideoModeOptions.FirstOrDefault(o => o.Key == s.Video) ?? VideoModeOptions[0];
        SelectedPreset = PresetOptions.First(o => o.Key == VideoModes.NormalizePreset(s.VideoPreset));
        SelectedAmfPreset = AmfPresets.First(x => x.Key == (s.Video == VideoModes.Amf ? VideoModes.AmfPreset(s.VideoPreset) : "balanced"));
        SelectedRateMode = RateModes.FirstOrDefault(x => x.Key == s.VideoRateMode) ?? RateModes[0];
        SelectedVideoRate = VideoRates.FirstOrDefault(x => x.Key == s.VideoBitrate) ?? VideoRates.First(x => x.Key == "8");
        SelectedQp = QpOptions[Math.Clamp(s.VideoQp, 14, 27) - 14];
        CustomMbps = s.CustomVideoMbps;
        SelectedFormat = Formats.FirstOrDefault(x => x.Key == s.OutputFormat) ?? Formats[0];

        SelectedJobs = JobOptions.FirstOrDefault(o => o.Key == s.Jobs) ?? JobOptions[0];
        Resume = s.ResumeJobs;
        Overwrite = s.OverwriteOriginal;
        PreserveTimestamp = s.PreserveTimestamp;
        SelectedAfter = AfterOptions.FirstOrDefault(o => o.Key == s.AfterCompletion) ?? AfterOptions[0];
        Notify = s.NotifyOnFinish;
        CheckUpdates = s.CheckUpdatesOnStart;

        if (!keepPaths)
        {
            FfmpegPath = s.FfmpegPath;
            FfprobePath = s.FfprobePath;
            TempWorkDir = s.TempWorkDir;
            OutputFolder = s.Output;
        }
    }

    public void ApplyTo(AppSettings s)
    {
        var t = CurrentTarget().Clamp();
        s.LoudnessProfile = SelectedProfile?.Key ?? LoudnessPresets.Custom;
        s.TargetI = t.Integrated;
        s.TargetTp = t.TruePeak;
        s.TargetLra = t.Lra;
        s.AudioTrackMode = SelectedTrackMode?.Key ?? "auto";
        s.PreferredLanguage = SelectedPreferredLanguage?.Key ?? "de";
        s.PreferFirst = PreferFirst;
        s.AnalyzeOnly = AnalyzeOnly;
        s.Audio = SelectedCodec;
        s.TagMode = SelectedTagMode?.Key ?? "default";
        s.CustomTag = CustomTag;
        s.AudioBitrate = SelectedBitrate ?? AppSettings.DefaultAudioBitrate(SelectedCodec);
        s.Video = SelectedVideoMode?.Key ?? VideoModes.Copy;
        s.VideoPreset = SelectedPreset?.Key ?? VideoModes.DefaultPreset;
        if (s.Video == VideoModes.Amf && SelectedAmfPreset?.Key != VideoModes.AmfPreset(s.VideoPreset))
            s.VideoPreset = SelectedAmfPreset?.Key switch { "quality" => "P7", "speed" => "P3", _ => "P5" };
        s.VideoRateMode = SelectedRateMode?.Key ?? "QP";
        s.VideoQp = int.TryParse(SelectedQp?.Key, out var qp) ? qp : 21;
        s.CustomVideoMbps = CustomMbps ?? 8;
        s.VideoBitrate = SelectedVideoRate?.Key ?? "8";
        s.OutputFormat = SelectedFormat?.Key ?? "auto";
        s.Jobs = SelectedJobs?.Key ?? "Auto";
        s.ResumeJobs = Resume;
        s.OverwriteOriginal = Overwrite;
        s.PreserveTimestamp = PreserveTimestamp;
        s.AfterCompletion = SelectedAfter?.Key ?? "none";
        s.NotifyOnFinish = Notify;
        s.CheckUpdatesOnStart = CheckUpdates;
        s.FfmpegPath = FfmpegPath.Trim();
        s.FfprobePath = FfprobePath.Trim();
        s.TempWorkDir = TempWorkDir.Trim();
        s.Output = OutputFolder.Trim();
        s.Normalize();
        if (!string.IsNullOrEmpty(s.ActiveProfile) &&
            !s.Profiles.Contains(ProcessingProfile.From(s, s.ActiveProfile)))
            s.ActiveProfile = string.Empty;
    }
}
