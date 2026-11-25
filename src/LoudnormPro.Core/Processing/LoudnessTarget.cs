using System.Globalization;

namespace LoudnormPro.Core.Processing;

/// <summary>Zielwerte fuer loudnorm (EBU R128).</summary>
public sealed record LoudnessTarget(double Integrated, double TruePeak, double Lra)
{
    public const double MinI = -70, MaxI = -5;
    public const double MinTp = -9, MaxTp = 0;
    public const double MinLra = 1, MaxLra = 20;

    public static readonly LoudnessTarget Default = new(-16, -1.5, 11);

    public LoudnessTarget Clamp() => new(
        Math.Clamp(Integrated, MinI, MaxI),
        Math.Clamp(TruePeak, MinTp, MaxTp),
        Math.Clamp(Lra, MinLra, MaxLra));

    /// <summary>"I=-16:TP=-1.5:LRA=11" (immer mit Punkt als Dezimaltrenner).</summary>
    public string ToFilterArgs()
    {
        var c = Clamp();
        return string.Create(CultureInfo.InvariantCulture, $"I={c.Integrated:0.##}:TP={c.TruePeak:0.##}:LRA={c.Lra:0.##}");
    }

    public string ToDisplay() =>
        string.Create(CultureInfo.InvariantCulture, $"I={Integrated:0.##}  TP={TruePeak:0.##}  LRA={Lra:0.##}");
}

/// <summary>Vordefinierte Lautheits-Profile.</summary>
public static class LoudnessPresets
{
    public const string Streaming = "streaming";
    public const string Ebu = "ebu_r128";
    public const string Platform = "platform";
    public const string Custom = "custom";

    public static readonly IReadOnlyDictionary<string, LoudnessTarget> All = new Dictionary<string, LoudnessTarget>
    {
        [Streaming] = new(-16, -1.5, 11),
        [Ebu] = new(-23, -1.0, 7),
        [Platform] = new(-14, -1.0, 11),
    };

    public static readonly string[] Keys = { Streaming, Ebu, Platform, Custom };

    public static LoudnessTarget Resolve(string? key, LoudnessTarget custom) =>
        key != null && All.TryGetValue(key, out var t) ? t : custom;

    /// <summary>Welches Profil entspricht diesen Werten? Sonst "custom".</summary>
    public static string Detect(LoudnessTarget t)
    {
        foreach (var (k, v) in All)
        {
            if (Math.Abs(v.Integrated - t.Integrated) < 0.01 && Math.Abs(v.TruePeak - t.TruePeak) < 0.01 && Math.Abs(v.Lra - t.Lra) < 0.01)
                return k;
        }
        return Custom;
    }
}
