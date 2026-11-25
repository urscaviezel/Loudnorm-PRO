namespace LoudnormPro.Core.Media;

/// <summary>Eine Audiospur aus ffprobe. <see cref="Position"/> ist der Index innerhalb der Audiospuren (0:a:N).</summary>
public sealed record AudioStreamInfo
{
    public int Position { get; init; }
    public int StreamIndex { get; init; }
    public string Language { get; init; } = string.Empty;
    public string Title { get; init; } = string.Empty;
    public bool IsDefault { get; init; }
    public bool IsForced { get; init; }
    public string Codec { get; init; } = string.Empty;
    public int? Channels { get; init; }
    public string ChannelLayout { get; init; } = string.Empty;
    public string SampleRate { get; init; } = string.Empty;

    public bool HasNumericSampleRate => SampleRate.Length > 0 && SampleRate.All(char.IsDigit);

    public string FormatDescription()
    {
        var parts = new List<string>();
        if (!string.IsNullOrWhiteSpace(Codec)) parts.Add(Codec);
        if (!string.IsNullOrWhiteSpace(ChannelLayout)) parts.Add(ChannelLayout);
        else if (Channels is > 0) parts.Add($"{Channels} ch");
        if (HasNumericSampleRate) parts.Add($"{SampleRate} Hz");
        return parts.Count > 0 ? string.Join(", ", parts) : "-";
    }
}

public sealed record MediaInfo(double DurationSeconds, IReadOnlyList<AudioStreamInfo> AudioStreams)
{
    public bool CanCopyOtherStreamsToWebm { get; init; } = true;
}
