using LoudnormPro.Core.Settings;

namespace LoudnormPro.Core.Processing;

/// <summary>Unveraenderliche Momentaufnahme aller Einstellungen fuer einen Durchlauf.</summary>
public sealed record ProcessingOptions
{
    public required string FfmpegPath { get; init; }
    public required string FfprobePath { get; init; }
    public required LoudnessTarget Target { get; init; }
    public required string AudioCodec { get; init; }      // "aac" | "eac3"
    public required string AudioBitrate { get; init; }
    public required AudioTrackSelector Tracks { get; init; }
    public required VideoPlan Video { get; init; }
    public required string OutputRoot { get; init; }
    public string TagMode { get; init; } = "default";
    public string CustomTag { get; init; } = "_norm";
    public string OutputFormat { get; init; } = "auto";
    public string TempWorkDir { get; init; } = string.Empty;
    public bool OverwriteOriginal { get; init; }
    public bool PreserveTimestamp { get; init; } = true;
    public bool Resume { get; init; } = true;
    public bool AnalyzeOnly { get; init; }
    /// <summary>null = automatisch.</summary>
    public int? ManualJobs { get; init; }

    public int CpuCount { get; init; } = Environment.ProcessorCount;

    /// <summary>Anzahl paralleler Encode-Jobs (Logik aus v1).</summary>
    public int EncodeJobs
    {
        get
        {
            if (ManualJobs is { } m) return Math.Clamp(m, 1, 8);
            return Video.Mode is VideoModes.Nvenc or VideoModes.AvcNvenc or VideoModes.Amf or VideoModes.VideoToolbox
                ? Math.Max(2, Math.Min(4, CpuCount / 4 == 0 ? 2 : CpuCount / 4))
                : Math.Max(2, Math.Min(8, CpuCount / 2 == 0 ? 2 : CpuCount / 2));
        }
    }

    /// <summary>Anzahl paralleler Analyse-Jobs (Pass 1).</summary>
    public int AnalysisJobs
    {
        get
        {
            if (ManualJobs is { } m) return Math.Clamp(m, 1, 8);
            var half = CpuCount / 2;
            return Video.Mode is VideoModes.Nvenc or VideoModes.AvcNvenc
                ? Math.Max(3, Math.Min(8, half == 0 ? 3 : half))
                : Math.Max(2, Math.Min(8, half == 0 ? 2 : half));
        }
    }

    /// <summary>Getrennte Limits fuer Analyse und Encode (v1: nur NVENC + Auto; v2: alle HW-Encoder + Auto).</summary>
    public bool SplitPipeline => ManualJobs == null && VideoModes.IsHardware(Video.Mode);

    public static ProcessingOptions FromSettings(AppSettings s, string ffmpeg, string ffprobe, EncoderCapabilities caps, string outputRoot) => new()
    {
        FfmpegPath = ffmpeg,
        FfprobePath = ffprobe,
        Target = s.Target,
        AudioCodec = AppSettings.EncoderFor(s.Audio),
        AudioBitrate = string.IsNullOrWhiteSpace(s.AudioBitrate) ? AppSettings.DefaultAudioBitrate(s.Audio) : s.AudioBitrate,
        Tracks = new AudioTrackSelector(s.TrackMode, s.PreferredLanguage, s.PreferFirst),
        Video = VideoPlanBuilder.Build(s.Video, s.VideoPreset, s.VideoRateMode != "bitrate", s.VideoQp,
            s.VideoBitrate == "custom" ? s.CustomVideoMbps : int.TryParse(s.VideoBitrate, out var rate) ? rate : 8, caps),
        OutputRoot = outputRoot,
        TagMode = s.TagMode,
        CustomTag = s.CustomTag,
        OutputFormat = s.OutputFormat,
        TempWorkDir = s.TempWorkDir,
        OverwriteOriginal = s.OverwriteOriginal,
        PreserveTimestamp = s.PreserveTimestamp,
        Resume = s.ResumeJobs,
        AnalyzeOnly = s.AnalyzeOnly,
        ManualJobs = s.JobsManual,
    };
}

/// <summary>Eine Quelldatei mit ihrem Basisordner (fuer die Unterordner-Struktur in der Ausgabe).</summary>
public sealed record SourceFile(string Path, string Root);
