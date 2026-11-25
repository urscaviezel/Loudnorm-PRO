namespace LoudnormPro.Core.Processing;

/// <summary>Video-Modi. Die String-Werte sind kompatibel zu v1 (Einstellungsdatei).</summary>
public static class VideoModes
{
    public const string Copy = "COPY";
    public const string Nvenc = "HEVC NVENC";
    public const string Amf = "HEVC AMF";
    public const string Vaapi = "HEVC VAAPI";
    public const string VideoToolbox = "HEVC VideoToolbox";
    public const string AvcNvenc = "H.264 NVENC";
    public const string X264 = "H.264 x264 (CPU)";
    public const string X265 = "HEVC x265 (CPU)";

    public static readonly string[] Presets = { "P7", "P6", "P5", "P4", "P3", "P2", "P1" };
    public const string DefaultPreset = "P4";
    public const string CqQuality = "CQ 19 (quality)"; // legacy settings
    public static readonly string[] Bitrates = { "4", "6", "8", "12", "20", "30", "50" };
    public static string NormalizePreset(string? preset)
    {
        var key = preset?.Trim().Split(' ')[0].ToUpperInvariant();
        return key != null && Presets.Contains(key) ? key : DefaultPreset;
    }

    public static string AmfPreset(string preset) => NormalizePreset(preset) switch
    {
        "P7" or "P6" => "quality",
        "P5" => "balanced",
        _ => "speed",
    };

    public static bool SupportsQp(string? mode) => mode is Nvenc or AvcNvenc or Amf or Vaapi;
    public static bool IsHardware(string mode) => mode is Nvenc or AvcNvenc or Amf or Vaapi or VideoToolbox;
}

/// <summary>Fertig aufgeloeste Video-Einstellungen fuer einen Durchlauf.</summary>
public sealed record VideoPlan(
    string Mode,
    string ShortName,
    IReadOnlyList<string> Args,
    string Tag,
    string PresetLabel,
    string QualityLabel,
    string? FallbackNotice)
{
    public bool IsHevc => Mode is VideoModes.Nvenc or VideoModes.Amf or VideoModes.Vaapi or VideoModes.X265 or VideoModes.VideoToolbox;
    public bool IsReencode => Mode != VideoModes.Copy;
}

public static class VideoPlanBuilder
{
    public static VideoPlan Build(string requestedMode, string preset, bool useQp, int qp, decimal mbps, EncoderCapabilities caps)
    {
        var inv = System.Globalization.CultureInfo.InvariantCulture;
        var rate = mbps.ToString("0.###", inv) + "M";
        var peak = (2 * mbps).ToString("0.###", inv) + "M";
        var q = qp.ToString(inv);
        var label = useQp && VideoModes.SupportsQp(requestedMode) ? $"QP {qp}" : $"{mbps} Mbit/s";
        preset = VideoModes.NormalizePreset(preset);
        switch (requestedMode)
        {
            case VideoModes.AvcNvenc when caps.AvcNvenc:
            case VideoModes.Nvenc when caps.Nvenc:
            {
                var avc = requestedMode == VideoModes.AvcNvenc;
                var args = new List<string> { "-c:v", avc ? "h264_nvenc" : "hevc_nvenc", "-preset", preset.ToLowerInvariant() };
                args.AddRange(useQp ? new[] { "-rc", "constqp", "-qp", q }
                    : new[] { "-rc", "vbr", "-b:v", rate, "-maxrate", peak, "-bufsize", peak });
                return new(requestedMode, avc ? "H.264 NVENC" : "NVENC", args, avc ? "_loudnorm_h264_nvenc" : "_loudnorm_nvenc", preset, label, null);
            }
            case VideoModes.Amf when caps.Amf:
            {
                var args = new List<string> { "-c:v", "hevc_amf", "-quality", VideoModes.AmfPreset(preset) };
                args.AddRange(useQp ? new[] { "-rc", "cqp", "-qp_i", q, "-qp_p", q }
                    : new[] { "-rc", "vbr_peak", "-b:v", rate, "-maxrate", peak });
                return new(VideoModes.Amf, "AMF", args, "_loudnorm_amf", VideoModes.AmfPreset(preset), label, null);
            }
            case VideoModes.Vaapi when caps.Vaapi && caps.VaapiDevice != null:
            {
                var args = new List<string> { "-vaapi_device", caps.VaapiDevice, "-filter:v:0", "format=nv12,hwupload",
                    "-c:v:0", "hevc_vaapi", "-profile:v:0", "main", "-bf:v:0", "0" };
                args.AddRange(useQp ? new[] { "-rc_mode:v:0", "CQP", "-qp:v:0", q }
                    : new[] { "-rc_mode:v:0", "VBR", "-b:v:0", rate, "-maxrate:v:0", peak });
                return new(VideoModes.Vaapi, "VAAPI", args, "_loudnorm_vaapi", "-", label, null);
            }
            case VideoModes.VideoToolbox when caps.VideoToolbox:
                return new(VideoModes.VideoToolbox, "VideoToolbox",
                    new[] { "-c:v", "hevc_videotoolbox", "-allow_sw", "1", "-b:v", rate },
                    "_loudnorm_vt", "-", label, null);
            case VideoModes.X264 when caps.X264:
                return new(VideoModes.X264, "x264", new[] { "-c:v:0", "libx264", "-pix_fmt:v:0", "yuv420p",
                    "-preset:v:0", "medium", "-b:v:0", rate }, "_loudnorm_x264", "medium", label, null);
            case VideoModes.X265 when caps.X265:
                return new(VideoModes.X265, "x265",
                    new[] { "-c:v:0", "libx265", "-preset:v:0", "medium", "-b:v:0", rate },
                    "_loudnorm_x265", "medium", label, null);
        }
        var notice = requestedMode != VideoModes.Copy ? requestedMode : null;
        return new(VideoModes.Copy, "COPY", new[] { "-c:v", "copy" }, "_loudnorm", "-", "-", notice);
    }
}
