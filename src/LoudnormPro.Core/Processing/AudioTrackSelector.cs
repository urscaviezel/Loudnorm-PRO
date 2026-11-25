using LoudnormPro.Core.Media;

namespace LoudnormPro.Core.Processing;

public enum AudioTrackMode
{
    /// <summary>Erste Spur in bevorzugter Sprache, sonst Spur 1 (schnellste Option).</summary>
    Auto,
    /// <summary>Alle Spuren normalisieren.</summary>
    All,
    /// <summary>Nur Spuren in bevorzugter Sprache.</summary>
    PreferredOnly,
}

/// <summary>
/// Spurauswahl und Ausgabe-Reihenfolge (1:1 aus v1, um Franzoesisch, Italienisch, Spanisch u. a. erweitert).
/// Bevorzugte Sprache: ISO-Code ("de", "en", "fr", ...), "orig" (jede Spur mit Sprach-Tag) oder "first".
/// </summary>
public sealed class AudioTrackSelector
{
    public const string FirstTrack = "first";
    public const string Original = "orig";

    private static readonly Dictionary<string, string[]> Aliases = new(StringComparer.OrdinalIgnoreCase)
    {
        ["de"] = new[] { "de", "deu", "ger" },
        ["en"] = new[] { "en", "eng" },
        ["fr"] = new[] { "fr", "fra", "fre" },
        ["it"] = new[] { "it", "ita" },
        ["es"] = new[] { "es", "spa" },
        ["pt"] = new[] { "pt", "por" },
        ["nl"] = new[] { "nl", "nld", "dut" },
        ["ja"] = new[] { "ja", "jpn" },
        ["rm"] = new[] { "rm", "roh" },
    };

    private static readonly Dictionary<string, string[]> TitleKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        ["de"] = new[] { "deutsch", "german", "allemand", "tedesco" },
        ["en"] = new[] { "english", "englisch", "anglais", "inglese" },
        ["fr"] = new[] { "français", "francais", "french", "französisch", "franzoesisch", "francese" },
        ["it"] = new[] { "italiano", "italian", "italienisch", "italien" },
        ["es"] = new[] { "español", "espanol", "spanish", "spanisch", "espagnol" },
        ["pt"] = new[] { "português", "portugues", "portuguese" },
        ["nl"] = new[] { "nederlands", "dutch" },
        ["ja"] = new[] { "japanese", "日本語" },
        ["rm"] = new[] { "rumantsch", "romansh", "rätoromanisch" },
    };

    /// <summary>Waehlbare Sprachen in den Einstellungen (Code -> Sprach-Key fuer die Uebersetzung).</summary>
    public static readonly string[] SelectableLanguages = { "de", "en", "fr", "it", "es", "pt", "nl", "ja", Original, FirstTrack };

    public AudioTrackMode Mode { get; }
    public string PreferredLanguage { get; }
    public bool PreferFirst { get; }

    public AudioTrackSelector(AudioTrackMode mode, string preferredLanguage, bool preferFirst)
    {
        Mode = mode;
        PreferredLanguage = string.IsNullOrWhiteSpace(preferredLanguage) ? "de" : preferredLanguage.Trim().ToLowerInvariant();
        PreferFirst = preferFirst;
    }

    public static string NormalizeLanguageCode(string? value)
    {
        var v = (value ?? string.Empty).Trim().ToLowerInvariant();
        foreach (var (key, aliases) in Aliases)
        {
            if (aliases.Contains(v)) return key;
        }
        return v;
    }

    public bool Matches(AudioStreamInfo info)
    {
        if (PreferredLanguage == FirstTrack) return false;

        var lang = NormalizeLanguageCode(info.Language);
        if (PreferredLanguage == Original) return lang.Length > 0;

        if (lang == PreferredLanguage) return true;
        if (Aliases.TryGetValue(PreferredLanguage, out var al) && al.Contains(lang)) return true;

        var title = info.Title.Trim().ToLowerInvariant();
        return title.Length > 0
               && TitleKeywords.TryGetValue(PreferredLanguage, out var kws)
               && kws.Any(k => title.Contains(k, StringComparison.OrdinalIgnoreCase));
    }

    public List<int> PreferredIndices(IReadOnlyList<AudioStreamInfo> streams) =>
        streams.Select((s, i) => (s, i)).Where(x => Matches(x.s)).Select(x => x.i).ToList();

    /// <summary>Welche Spuren (Position) werden normalisiert?</summary>
    public List<int> SelectedIndices(IReadOnlyList<AudioStreamInfo> streams)
    {
        if (streams.Count == 0) return new List<int>();
        if (Mode == AudioTrackMode.All) return Enumerable.Range(0, streams.Count).ToList();

        var preferred = PreferredIndices(streams);
        if (Mode == AudioTrackMode.PreferredOnly)
        {
            // "Erste Spur" ohne Sprachbezug -> Spur 1
            if (PreferredLanguage == FirstTrack) return new List<int> { 0 };
            return preferred;
        }

        return preferred.Count > 0 ? new List<int> { preferred[0] } : new List<int> { 0 };
    }

    /// <summary>Reihenfolge der Audiospuren in der Ausgabedatei (Input-Positionen).</summary>
    public List<int> OutputOrder(IReadOnlyList<AudioStreamInfo> streams)
    {
        var baseOrder = Enumerable.Range(0, streams.Count).ToList();
        if (Mode != AudioTrackMode.PreferredOnly || !PreferFirst) return baseOrder;

        var preferred = PreferredIndices(streams);
        if (preferred.Count == 0) return baseOrder;

        var set = preferred.ToHashSet();
        return preferred.Concat(baseOrder.Where(i => !set.Contains(i))).ToList();
    }

    /// <summary>Index (in der Ausgabe) der neuen Default-Spur.</summary>
    public int? DefaultOutputIndex(IReadOnlyList<AudioStreamInfo> streams, IReadOnlyList<int> outputOrder)
    {
        if (outputOrder.Count == 0) return null;

        if (Mode is AudioTrackMode.Auto or AudioTrackMode.PreferredOnly)
        {
            var preferred = PreferredIndices(streams);
            if (preferred.Count > 0)
            {
                for (var o = 0; o < outputOrder.Count; o++)
                    if (outputOrder[o] == preferred[0]) return o;
            }
        }

        for (var o = 0; o < outputOrder.Count; o++)
            if (streams[outputOrder[o]].IsDefault) return o;

        return 0;
    }
}
