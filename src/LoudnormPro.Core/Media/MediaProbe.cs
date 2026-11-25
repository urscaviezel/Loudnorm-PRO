using System.Globalization;
using System.Text.Json;
using LoudnormPro.Core.Platform;

namespace LoudnormPro.Core.Media;

/// <summary>Liest Dauer und Audiospuren per ffprobe.</summary>
public sealed class MediaProbe
{
    private readonly string _ffprobePath;

    public MediaProbe(string ffprobePath) => _ffprobePath = ffprobePath;

    public async Task<MediaInfo> ProbeAsync(string file, CancellationToken ct = default)
    {
        var args = new[]
        {
            "-v", "error",
            "-show_entries",
            "format=duration:stream=index,codec_type,codec_name,channels,channel_layout,sample_rate:stream_tags=language,title:stream_disposition=default,forced",
            "-of", "json",
            file,
        };
        var result = await ProcessRunner.RunAsync(_ffprobePath, args, TimeSpan.FromSeconds(60), ct).ConfigureAwait(false);
        return Parse(result.StdOut);
    }

    /// <summary>Parst die JSON-Ausgabe von ffprobe (oeffentlich fuer Tests).</summary>
    public static MediaInfo Parse(string json)
    {
        double duration = 1.0;
        var streams = new List<AudioStreamInfo>();
        var canCopyToWebm = true;
        if (string.IsNullOrWhiteSpace(json))
            return new MediaInfo(duration, streams);

        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            if (root.TryGetProperty("format", out var format) &&
                format.TryGetProperty("duration", out var d) &&
                double.TryParse(GetString(d), NumberStyles.Float, CultureInfo.InvariantCulture, out var dur) &&
                dur > 0)
            {
                duration = dur;
            }

            if (root.TryGetProperty("streams", out var arr) && arr.ValueKind == JsonValueKind.Array)
            {
                var pos = 0;
                foreach (var s in arr.EnumerateArray())
                {
                    var type = s.TryGetProperty("codec_type", out var ctp) ? GetString(ctp) : "audio";
                    if (!string.Equals(type, "audio", StringComparison.OrdinalIgnoreCase))
                    {
                        var codec = s.TryGetProperty("codec_name", out var name) ? GetString(name) : "";
                        canCopyToWebm &= type switch
                        {
                            "video" => codec is "vp8" or "vp9" or "av1",
                            "subtitle" => codec == "webvtt",
                            _ => false,
                        };
                        continue;
                    }

                    var tags = s.TryGetProperty("tags", out var t) ? t : default;
                    var disp = s.TryGetProperty("disposition", out var dp) ? dp : default;

                    streams.Add(new AudioStreamInfo
                    {
                        Position = pos++,
                        StreamIndex = s.TryGetProperty("index", out var idx) && idx.TryGetInt32(out var i) ? i : -1,
                        Language = GetTag(tags, "language"),
                        Title = GetTag(tags, "title"),
                        IsDefault = GetFlag(disp, "default"),
                        IsForced = GetFlag(disp, "forced"),
                        Codec = s.TryGetProperty("codec_name", out var cn) ? GetString(cn) : string.Empty,
                        Channels = s.TryGetProperty("channels", out var ch) && ch.TryGetInt32(out var c) ? c : null,
                        ChannelLayout = s.TryGetProperty("channel_layout", out var cl) ? GetString(cl) : string.Empty,
                        SampleRate = s.TryGetProperty("sample_rate", out var sr) ? GetString(sr) : string.Empty,
                    });
                }
            }
        }
        catch (JsonException)
        {
            // unvollstaendige Ausgabe -> leere Info
        }

        return new MediaInfo(duration, streams) { CanCopyOtherStreamsToWebm = canCopyToWebm };
    }

    private static string GetString(JsonElement e) => e.ValueKind switch
    {
        JsonValueKind.String => e.GetString() ?? string.Empty,
        JsonValueKind.Number => e.GetRawText(),
        _ => string.Empty,
    };

    private static string GetTag(JsonElement tags, string name)
    {
        if (tags.ValueKind != JsonValueKind.Object) return string.Empty;
        foreach (var p in tags.EnumerateObject())
        {
            if (string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase))
                return GetString(p.Value).Trim();
        }
        return string.Empty;
    }

    private static bool GetFlag(JsonElement disp, string name)
    {
        if (disp.ValueKind != JsonValueKind.Object || !disp.TryGetProperty(name, out var v)) return false;
        return v.ValueKind == JsonValueKind.Number && v.TryGetInt32(out var i) && i != 0;
    }
}
