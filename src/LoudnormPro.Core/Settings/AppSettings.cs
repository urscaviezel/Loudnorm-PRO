using System.Text.Json.Serialization;
using LoudnormPro.Core.Processing;

namespace LoudnormPro.Core.Settings;

/// <summary>
/// Alle Einstellungen. Die snake_case-Namen der v1-Felder bleiben erhalten,
/// damit eine bestehende loudnorm_settings.json weiter gelesen wird.
/// </summary>
public sealed class AppSettings
{
    // ---------- aus v1 ----------
    [JsonPropertyName("input")] public string Input { get; set; } = string.Empty;
    [JsonPropertyName("output")] public string Output { get; set; } = string.Empty;
    [JsonPropertyName("temp_work_dir")] public string TempWorkDir { get; set; } = string.Empty;
    [JsonPropertyName("audio")] public string Audio { get; set; } = "AAC";
    [JsonPropertyName("audio_bitrate")] public string AudioBitrate { get; set; } = "384k";
    [JsonPropertyName("audio_track_mode")] public string AudioTrackMode { get; set; } = "auto";
    [JsonPropertyName("preferred_language")] public string PreferredLanguage { get; set; } = "de";
    [JsonPropertyName("prefer_first")] public bool PreferFirst { get; set; } = true;
    [JsonPropertyName("resume_jobs")] public bool ResumeJobs { get; set; } = true;
    [JsonPropertyName("video")] public string Video { get; set; } = VideoModes.Copy;
    [JsonPropertyName("video_preset")] public string VideoPreset { get; set; } = VideoModes.DefaultPreset;
    [JsonPropertyName("video_bitrate")] public string VideoBitrate { get; set; } = string.Empty;
    [JsonPropertyName("video_rate_mode")] public string? VideoRateMode { get; set; }
    [JsonPropertyName("video_qp")] public int VideoQp { get; set; } = 21;
    [JsonPropertyName("custom_video_mbps")] public decimal CustomVideoMbps { get; set; } = 8;
    [JsonPropertyName("tag_mode")] public string TagMode { get; set; } = "default";
    [JsonPropertyName("custom_tag")] public string CustomTag { get; set; } = "_norm";
    [JsonPropertyName("output_format")] public string OutputFormat { get; set; } = "auto";
    [JsonPropertyName("jobs")] public string Jobs { get; set; } = "Auto";
    /// <summary>v1: "Deutsch"/"English" - wird nur noch zur Migration gelesen.</summary>
    [JsonPropertyName("language")] public string? LegacyLanguage { get; set; }
    /// <summary>v1: "Dunkel"/"Hell" - wird nur noch zur Migration gelesen.</summary>
    [JsonPropertyName("theme")] public string? LegacyTheme { get; set; }
    [JsonPropertyName("preview_visible")] public bool PreviewVisible { get; set; }

    // ---------- neu in v2 ----------
    [JsonPropertyName("ui_language")] public string? UiLanguage { get; set; }
    /// <summary>"dark", "light" oder "system".</summary>
    [JsonPropertyName("quick_tips")] public bool QuickTips { get; set; } = true;
    [JsonPropertyName("ui_theme")] public string UiTheme { get; set; } = string.Empty;
    [JsonPropertyName("loudness_profile")] public string LoudnessProfile { get; set; } = LoudnessPresets.Streaming;
    [JsonPropertyName("target_i")] public double TargetI { get; set; } = -16;
    [JsonPropertyName("target_tp")] public double TargetTp { get; set; } = -1.5;
    [JsonPropertyName("target_lra")] public double TargetLra { get; set; } = 11;
    [JsonPropertyName("overwrite_original")] public bool OverwriteOriginal { get; set; }
    [JsonPropertyName("preserve_timestamp")] public bool PreserveTimestamp { get; set; } = true;
    [JsonPropertyName("ffmpeg_path")] public string FfmpegPath { get; set; } = string.Empty;
    [JsonPropertyName("ffprobe_path")] public string FfprobePath { get; set; } = string.Empty;
    [JsonPropertyName("check_updates_on_start")] public bool CheckUpdatesOnStart { get; set; } = true;
    /// <summary>"none", "sleep" oder "shutdown".</summary>
    [JsonPropertyName("after_completion")] public string AfterCompletion { get; set; } = "none";
    [JsonPropertyName("notify_on_finish")] public bool NotifyOnFinish { get; set; } = true;
    [JsonPropertyName("analyze_only")] public bool AnalyzeOnly { get; set; }
    [JsonPropertyName("log_autoscroll")] public bool LogAutoScroll { get; set; } = true;
    [JsonPropertyName("window_width")] public double WindowWidth { get; set; } = 1280;
    [JsonPropertyName("window_height")] public double WindowHeight { get; set; } = 820;
    [JsonPropertyName("window_x")] public int? WindowX { get; set; }
    [JsonPropertyName("window_y")] public int? WindowY { get; set; }
    [JsonPropertyName("window_maximized")] public bool WindowMaximized { get; set; }
    [JsonPropertyName("active_profile")] public string ActiveProfile { get; set; } = string.Empty;
    [JsonPropertyName("profiles")] public List<ProcessingProfile> Profiles { get; set; } = new();
    [JsonPropertyName("recent_files")] public List<string> RecentFiles { get; set; } = new();

    [JsonIgnore]
    public LoudnessTarget Target
    {
        get => LoudnessPresets.Resolve(LoudnessProfile, new LoudnessTarget(TargetI, TargetTp, TargetLra)).Clamp();
        set
        {
            TargetI = value.Integrated;
            TargetTp = value.TruePeak;
            TargetLra = value.Lra;
        }
    }

    [JsonIgnore]
    public Processing.AudioTrackMode TrackMode
    {
        get => AudioTrackMode switch
        {
            "all" => Processing.AudioTrackMode.All,
            "preferred_only" => Processing.AudioTrackMode.PreferredOnly,
            _ => Processing.AudioTrackMode.Auto,
        };
        set => AudioTrackMode = value switch
        {
            Processing.AudioTrackMode.All => "all",
            Processing.AudioTrackMode.PreferredOnly => "preferred_only",
            _ => "auto",
        };
    }

    /// <summary>"Auto" oder 1..8.</summary>
    [JsonIgnore]
    public int? JobsManual => int.TryParse(Jobs, out var n) ? Math.Clamp(n, 1, 8) : null;

    public static readonly string[] AacBitrates = { "128k", "192k", "256k", "320k", "384k" };
    public static readonly string[] Eac3Bitrates = { "192k", "256k", "384k", "448k", "640k" };
    public static readonly string[] JobOptions = { "Auto", "1", "2", "3", "4", "5", "6", "7", "8" };

    public static readonly string[] AudioCodecs = { "AAC", "E-AC3", "Opus", "PCM 24-bit" };
    private static readonly string[] OpusBitrates = { "64k", "96k", "128k", "160k", "192k", "256k" };
    public static string EncoderFor(string codec) => codec switch { "E-AC3" => "eac3", "Opus" => "libopus", "PCM 24-bit" => "pcm_s24le", _ => "aac" };
    public static string DefaultAudioBitrate(string codec) => codec switch
    {
        "E-AC3" => "640k", "Opus" => "192k", "PCM 24-bit" => "", _ => "384k",
    };
    public static IReadOnlyList<string> BitratesFor(string codec) => codec switch
    {
        "E-AC3" => Eac3Bitrates, "Opus" => OpusBitrates, "PCM 24-bit" => Array.Empty<string>(), _ => AacBitrates,
    };

    /// <summary>Korrigiert ungueltige Werte (z. B. aus einer von Hand bearbeiteten Datei).</summary>
    public void Normalize()
    {
        if (!AudioCodecs.Contains(Audio)) Audio = "AAC";
        if (!BitratesFor(Audio).Contains(AudioBitrate)) AudioBitrate = DefaultAudioBitrate(Audio);
        if (!Media.FileNaming.ValidTag(CustomTag)) CustomTag = "_norm";
        if (TagMode is not ("custom" or "none")) TagMode = "default";
        if (AudioTrackMode is not ("auto" or "all" or "preferred_only")) AudioTrackMode = "auto";
        if (string.IsNullOrWhiteSpace(PreferredLanguage)) PreferredLanguage = "de";
        if (!JobOptions.Contains(Jobs)) Jobs = "Auto";
        VideoPreset = VideoModes.NormalizePreset(VideoPreset);
        if (VideoRateMode is not ("QP" or "bitrate"))
        {
            if (VideoBitrate == VideoModes.CqQuality) VideoQp = 19;
            VideoRateMode = string.IsNullOrEmpty(VideoBitrate) || VideoBitrate == VideoModes.CqQuality ? "QP" : "bitrate";
        }
        VideoQp = Math.Clamp(VideoQp, 14, 27);
        if (CustomVideoMbps is < 0.001m or > 1000m) CustomVideoMbps = 8;
        if (!Media.FileNaming.IsExplicitFormat(OutputFormat)) OutputFormat = "auto";
        VideoBitrate = VideoBitrate?.Split(' ')[0] ?? "8";
        if (VideoBitrate != "custom" && !VideoModes.Bitrates.Contains(VideoBitrate)) VideoBitrate = "8";
        if (!LoudnessPresets.Keys.Contains(LoudnessProfile)) LoudnessProfile = LoudnessPresets.Custom;
        if (AfterCompletion is not ("none" or "sleep" or "shutdown")) AfterCompletion = "none";
        if (UiTheme is not ("dark" or "light" or "system")) UiTheme = "dark";
        var t = new LoudnessTarget(TargetI, TargetTp, TargetLra).Clamp();
        TargetI = t.Integrated; TargetTp = t.TruePeak; TargetLra = t.Lra;
        Profiles ??= new();
        RecentFiles ??= new();
        if (!double.IsFinite(WindowWidth) || WindowWidth <= 0) WindowWidth = 1280;
        if (!double.IsFinite(WindowHeight) || WindowHeight <= 0) WindowHeight = 820;
    }

    public AppSettings Clone()
    {
        var json = System.Text.Json.JsonSerializer.Serialize(this, SettingsJsonContext.Default.AppSettings);
        return System.Text.Json.JsonSerializer.Deserialize(json, SettingsJsonContext.Default.AppSettings)!;
    }
}

/// <summary>Benannte Sammlung von Verarbeitungs-Einstellungen ("Profil").</summary>
public sealed record ProcessingProfile
{
    [JsonPropertyName("name")] public string Name { get; set; } = string.Empty;
    [JsonPropertyName("loudness_profile")] public string LoudnessProfile { get; set; } = LoudnessPresets.Streaming;
    [JsonPropertyName("target_i")] public double TargetI { get; set; } = -16;
    [JsonPropertyName("target_tp")] public double TargetTp { get; set; } = -1.5;
    [JsonPropertyName("target_lra")] public double TargetLra { get; set; } = 11;
    [JsonPropertyName("audio")] public string Audio { get; set; } = "AAC";
    [JsonPropertyName("audio_bitrate")] public string AudioBitrate { get; set; } = "384k";
    [JsonPropertyName("audio_track_mode")] public string AudioTrackMode { get; set; } = "auto";
    [JsonPropertyName("preferred_language")] public string PreferredLanguage { get; set; } = "de";
    [JsonPropertyName("prefer_first")] public bool PreferFirst { get; set; } = true;
    [JsonPropertyName("video")] public string Video { get; set; } = VideoModes.Copy;
    [JsonPropertyName("video_preset")] public string VideoPreset { get; set; } = VideoModes.DefaultPreset;
    [JsonPropertyName("video_bitrate")] public string VideoBitrate { get; set; } = string.Empty;
    [JsonPropertyName("video_rate_mode")] public string? VideoRateMode { get; set; }
    [JsonPropertyName("video_qp")] public int VideoQp { get; set; } = 21;
    [JsonPropertyName("custom_video_mbps")] public decimal CustomVideoMbps { get; set; } = 8;
    [JsonPropertyName("tag_mode")] public string TagMode { get; set; } = "default";
    [JsonPropertyName("custom_tag")] public string CustomTag { get; set; } = "_norm";
    [JsonPropertyName("output_format")] public string OutputFormat { get; set; } = "auto";
    [JsonPropertyName("jobs")] public string Jobs { get; set; } = "Auto";

    public static ProcessingProfile From(AppSettings s, string name) => new()
    {
        Name = name,
        LoudnessProfile = s.LoudnessProfile,
        TargetI = s.TargetI,
        TargetTp = s.TargetTp,
        TargetLra = s.TargetLra,
        Audio = s.Audio,
        AudioBitrate = s.AudioBitrate,
        AudioTrackMode = s.AudioTrackMode,
        PreferredLanguage = s.PreferredLanguage,
        PreferFirst = s.PreferFirst,
        Video = s.Video,
        VideoPreset = s.VideoPreset,
        VideoBitrate = s.VideoBitrate,
        VideoRateMode = s.VideoRateMode,
        VideoQp = s.VideoQp,
        CustomVideoMbps = s.CustomVideoMbps,
        TagMode = s.TagMode,
        CustomTag = s.CustomTag,
        OutputFormat = s.OutputFormat,
        Jobs = s.Jobs,
    };

    public void ApplyTo(AppSettings s)
    {
        s.LoudnessProfile = LoudnessProfile;
        s.TargetI = TargetI;
        s.TargetTp = TargetTp;
        s.TargetLra = TargetLra;
        s.Audio = Audio;
        s.AudioBitrate = AudioBitrate;
        s.AudioTrackMode = AudioTrackMode;
        s.PreferredLanguage = PreferredLanguage;
        s.PreferFirst = PreferFirst;
        s.Video = Video;
        s.VideoPreset = VideoPreset;
        s.VideoBitrate = VideoBitrate;
        s.VideoRateMode = VideoRateMode;
        s.VideoQp = VideoQp;
        s.CustomVideoMbps = CustomVideoMbps;
        s.TagMode = TagMode;
        s.CustomTag = CustomTag;
        s.OutputFormat = OutputFormat;
        s.Jobs = Jobs;
        s.ActiveProfile = Name;
        s.Normalize();
    }
}

[JsonSourceGenerationOptions(WriteIndented = true, PropertyNameCaseInsensitive = true, ReadCommentHandling = System.Text.Json.JsonCommentHandling.Skip, AllowTrailingCommas = true)]
[JsonSerializable(typeof(AppSettings))]
internal partial class SettingsJsonContext : JsonSerializerContext
{
}
