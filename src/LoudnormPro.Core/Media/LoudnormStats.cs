using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace LoudnormPro.Core.Media;

/// <summary>Messwerte aus Pass 1 (loudnorm print_format=json).</summary>
public sealed record LoudnormStats(string InputI, string InputTp, string InputLra, string InputThresh, string TargetOffset)
{
    public double? InputIntegrated =>
        double.TryParse(InputI, NumberStyles.Float, CultureInfo.InvariantCulture, out var v) ? v : null;

    public double? InputTruePeak =>
        double.TryParse(InputTp, NumberStyles.Float, CultureInfo.InvariantCulture, out var v) ? v : null;

    /// <summary>Sucht den JSON-Block in der ffmpeg-Ausgabe (wie v1: erstes '{' bis letztes '}').</summary>
    public static LoudnormStats? FromFfmpegOutput(string? text)
    {
        if (string.IsNullOrEmpty(text)) return null;
        var start = text.LastIndexOf("[Parsed_loudnorm", StringComparison.Ordinal);
        var from = start >= 0 ? text.IndexOf('{', start) : text.IndexOf('{');
        var end = text.LastIndexOf('}');
        if (from < 0 || end <= from) return null;

        try
        {
            using var doc = JsonDocument.Parse(text.AsMemory(from, end - from + 1));
            var r = doc.RootElement;
            string Get(string n) => r.TryGetProperty(n, out var v) ? (v.ValueKind == JsonValueKind.String ? v.GetString() ?? "" : v.GetRawText()).Trim() : "";
            var s = new LoudnormStats(Get("input_i"), Get("input_tp"), Get("input_lra"), Get("input_thresh"), Get("target_offset"));
            return s.IsComplete ? s : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    public bool IsComplete =>
        !string.IsNullOrEmpty(InputI) && !string.IsNullOrEmpty(InputTp) && !string.IsNullOrEmpty(InputLra)
        && !string.IsNullOrEmpty(InputThresh) && !string.IsNullOrEmpty(TargetOffset);
}

/// <summary>Liest die Ausgangslautheit aus Pass 2 (print_format=summary).</summary>
public static partial class LoudnormSummary
{
    [GeneratedRegex(@"Output Integrated:\s*(-?\d+(?:\.\d+)?)\s*LUFS", RegexOptions.IgnoreCase)]
    private static partial Regex OutputIntegratedRegex();

    /// <summary>Durchschnitt aller "Output Integrated"-Werte (bei mehreren Spuren).</summary>
    public static double? ParseOutputIntegrated(string? text)
    {
        if (string.IsNullOrEmpty(text)) return null;
        var values = OutputIntegratedRegex().Matches(text)
            .Select(m => double.TryParse(m.Groups[1].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out var v) ? (double?)v : null)
            .Where(v => v.HasValue && !double.IsInfinity(v.Value))
            .Select(v => v!.Value)
            .ToList();
        return values.Count > 0 ? Math.Round(values.Average(), 1) : null;
    }
}
