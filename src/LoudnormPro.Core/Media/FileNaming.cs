using System.Text.RegularExpressions;

namespace LoudnormPro.Core.Media;

/// <summary>Dateinamen-Regeln (aus v1 uebernommen, um _vt fuer VideoToolbox erweitert).</summary>
public static partial class FileNaming
{
    public static readonly IReadOnlySet<string> VideoExtensions =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase) { ".mkv", ".mp4", ".avi", ".mov", ".m4v", ".ts", ".webm" };

    /// <summary>Alle moeglichen Ausgabe-Tags.</summary>
    public static readonly string[] AllTags =
        { "_loudnorm", "_loudnorm_nvenc", "_loudnorm_amf", "_loudnorm_vaapi", "_loudnorm_vt", "_loudnorm_x265", "_loudnorm_x264", "_loudnorm_h264_nvenc" };

    [GeneratedRegex(@"(?:_loudnorm(?:_(?:nvenc|vaapi|x265|x264|h264_nvenc|amf|vt))?)$", RegexOptions.IgnoreCase)]
    private static partial Regex LoudnormSuffixRegex();

    [GeneratedRegex(@"_loudnorm($|_)", RegexOptions.IgnoreCase)]
    private static partial Regex LoudnormMarkerRegex();

    [GeneratedRegex(@"[^a-z0-9]")]
    private static partial Regex NonAlnumRegex();

    public static bool IsVideoFile(string path) => VideoExtensions.Contains(Path.GetExtension(path));

    public static bool IsLoudnormOutput(string path) =>
        LoudnormMarkerRegex().IsMatch(Path.GetFileNameWithoutExtension(path));

    public static string NormalizeName(string name) => NonAlnumRegex().Replace(name.ToLowerInvariant(), string.Empty);

    public static string CanonicalOutputStem(string stem) => LoudnormSuffixRegex().Replace(stem, string.Empty);

    /// <summary>
    /// Ausgabe-Container: WebM kann AAC/E-AC3/HEVC nicht aufnehmen -> MKV.
    /// Bei HEVC-Neukodierung: M4V (iPod-Muxer) -> MP4, AVI -> MKV.
    /// </summary>
    public static string OutputExtensionFor(string inputPath, bool hevcReencode = false, string outputFormat = "auto", string audioCodec = "aac")
    {
        if (IsExplicitFormat(outputFormat)) return "." + outputFormat;
        var ext = Path.GetExtension(inputPath).ToLowerInvariant();
        if (ext == ".webm") return audioCodec == "libopus" && !hevcReencode ? ".webm" : ".mkv";
        if (audioCodec == "libopus" && ext is ".mov" or ".avi" or ".m4v") return ".mkv";
        if (audioCodec == "pcm_s24le" && ext is ".ts" or ".m4v") return ".mkv";
        if (hevcReencode && ext == ".m4v") return ".mp4";
        if (hevcReencode && ext == ".avi") return ".mkv";
        return ext;
    }

    public static bool ValidTag(string? tag) =>
        !string.IsNullOrWhiteSpace(tag) && !tag.Any(c => char.IsControl(c) || "<>:\"/\\|?*".Contains(c));

    public static bool SupportsAudio(string ext, string codec) => ext.ToLowerInvariant() switch
    {
        ".webm" => codec == "libopus",
        ".mov" or ".avi" => codec != "libopus",
        ".ts" => codec != "pcm_s24le",
        ".m4v" => codec == "aac",
        _ => true,
    };

    public static bool IsExplicitFormat(string? format) => format is "mkv" or "mp4" or "mov" or "ts" or "webm";

    public static bool IsMp4Family(string path) =>
        Path.GetExtension(path).ToLowerInvariant() is ".mp4" or ".mov" or ".m4v";

    public static bool IsWebm(string path) => string.Equals(Path.GetExtension(path), ".webm", StringComparison.OrdinalIgnoreCase);

    /// <summary>Alle Videodateien eines Ordners (rekursiv), bereits normalisierte Ausgaben werden ignoriert.</summary>
    public static List<string> CollectVideosFromFolder(string folder)
    {
        var result = new List<string>();
        if (!Directory.Exists(folder))
            return result;

        var options = new EnumerationOptions
        {
            RecurseSubdirectories = true,
            IgnoreInaccessible = true,
            AttributesToSkip = FileAttributes.Hidden | FileAttributes.System,
        };
        foreach (var f in Directory.EnumerateFiles(folder, "*", options))
        {
            if (IsVideoFile(f) && !IsLoudnormOutput(f) && !IsTempArtifact(f))
                result.Add(f);
        }
        result.Sort(StringComparer.OrdinalIgnoreCase);
        return result;
    }

    /// <summary>Temporaere Dateien aus dem Ueberschreib-Modus.</summary>
    public static bool IsTempArtifact(string path)
    {
        var name = Path.GetFileName(path);
        return name.Contains(".__ln_tmp", StringComparison.OrdinalIgnoreCase)
               || name.StartsWith(".__replace_tmp__", StringComparison.OrdinalIgnoreCase);
    }
}
