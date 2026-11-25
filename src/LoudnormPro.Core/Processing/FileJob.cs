using LoudnormPro.Core.Media;

namespace LoudnormPro.Core.Processing;

public static class JobStatus
{
    public const string Ok = "OK";
    public const string Analyzed = "ANALYZED";
    public const string SkipExists = "SKIP_EXISTS";
    public const string ErrorNoAudio = "ERROR_NO_AUDIO";
    public const string ErrorNoMatchingAudio = "ERROR_NO_MATCHING_AUDIO";
    public const string ErrorPass1 = "ERROR_PASS1";
    public const string ErrorPass2 = "ERROR_PASS2";
    public const string ErrorReplace = "ERROR_REPLACE";
    public const string ErrorPrepare = "ERROR_PREPARE";
    public const string Cancelled = "CANCELLED";

    public static bool IsError(string s) => s.StartsWith("ERROR", StringComparison.Ordinal);
    public static bool IsDone(string s) => s is Ok or SkipExists;
}

/// <summary>Arbeitskontext einer Datei (entspricht dem ctx-Dictionary aus v1).</summary>
public sealed class FileJob
{
    public required string File { get; init; }
    public required string DisplayName { get; init; }
    /// <summary>Endgueltige Ausgabedatei (im Ueberschreib-Modus = Original).</summary>
    public required string Output { get; init; }
    /// <summary>Datei, in die ffmpeg schreibt (im Ueberschreib-Modus temporaer).</summary>
    public required string EncodeOutput { get; init; }
    public bool OverwriteOriginal { get; init; }
    public double DurationSeconds { get; set; } = 1;
    public IReadOnlyList<AudioStreamInfo> AudioStreams { get; set; } = Array.Empty<AudioStreamInfo>();
    public List<int> SelectedIndices { get; set; } = new();
    public List<int> OutputOrder { get; set; } = new();
    public int? DefaultOutputIndex { get; set; }
    public Dictionary<int, LoudnormStats> Stats { get; } = new();
    public DateTime Started { get; init; } = DateTime.UtcNow;
}

/// <summary>Ergebnis einer Datei (Zeile in Resume-Datei und Log-CSV).</summary>
public sealed record FileResult
{
    public required string File { get; init; }
    public required string Output { get; init; }
    public required string Status { get; init; }
    public string Phase { get; init; } = string.Empty;
    public string Details { get; init; } = string.Empty;
    public required string DisplayName { get; init; }
    public TimeSpan Elapsed { get; init; }
    /// <summary>Gemessene Lautheit vorher (LUFS, erste normalisierte Spur).</summary>
    public double? MeasuredBefore { get; init; }
    /// <summary>Lautheit nachher (LUFS, aus Pass 2).</summary>
    public double? MeasuredAfter { get; init; }
}
