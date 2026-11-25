using System.Net.Http.Headers;
using System.Text.Json;

namespace LoudnormPro.Core.Services;

public sealed record ReleaseAsset(string Name, string DownloadUrl, long Size);

public sealed record ReleaseInfo(string Tag, string Name, string Body, string HtmlUrl, IReadOnlyList<ReleaseAsset> Assets)
{
    public string Version => string.IsNullOrWhiteSpace(Tag) ? Name : Tag;
}

/// <summary>Prueft GitHub Releases auf eine neuere Version.</summary>
public static class UpdateChecker
{
    private static readonly HttpClient Http = CreateClient();

    private static HttpClient CreateClient()
    {
        var c = new HttpClient { Timeout = TimeSpan.FromSeconds(12) };
        c.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("Loudnorm-PRO", AppInfo.Version));
        c.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/vnd.github+json"));
        return c;
    }

    public static async Task<ReleaseInfo> GetLatestAsync(CancellationToken ct = default)
    {
        var json = await Http.GetStringAsync(AppInfo.ApiLatestRelease, ct).ConfigureAwait(false);
        return Parse(json);
    }

    public static ReleaseInfo Parse(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var r = doc.RootElement;
        string S(JsonElement e, string n) => e.TryGetProperty(n, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() ?? "" : "";

        var assets = new List<ReleaseAsset>();
        if (r.TryGetProperty("assets", out var arr) && arr.ValueKind == JsonValueKind.Array)
        {
            foreach (var a in arr.EnumerateArray())
            {
                var size = a.TryGetProperty("size", out var sz) && sz.TryGetInt64(out var l) ? l : 0;
                assets.Add(new ReleaseAsset(S(a, "name"), S(a, "browser_download_url"), size));
            }
        }

        var html = S(r, "html_url");
        return new ReleaseInfo(S(r, "tag_name"), S(r, "name"), S(r, "body"), string.IsNullOrEmpty(html) ? AppInfo.ReleasesUrl : html, assets);
    }

    /// <summary>Vergleicht Versionsnummern numerisch ("v2.1.0" &gt; "2.0.9").</summary>
    public static bool IsNewer(string candidate, string current) => Compare(ParseVersion(candidate), ParseVersion(current)) > 0;

    internal static int[] ParseVersion(string value)
    {
        var cleaned = new string((value ?? "").Trim().TakeWhile(c => char.IsDigit(c) || c == '.' || c == 'v' || c == 'V').Where(c => char.IsDigit(c) || c == '.').ToArray());
        var parts = cleaned.Split('.', StringSplitOptions.RemoveEmptyEntries).Select(p => int.TryParse(p, out var n) ? n : 0).ToArray();
        return parts.Length > 0 ? parts : new[] { 0 };
    }

    private static int Compare(int[] a, int[] b)
    {
        for (var i = 0; i < Math.Max(a.Length, b.Length); i++)
        {
            var x = i < a.Length ? a[i] : 0;
            var y = i < b.Length ? b[i] : 0;
            if (x != y) return x.CompareTo(y);
        }
        return 0;
    }

    /// <summary>Passendes Download-Paket fuer dieses Betriebssystem (oder null).</summary>
    public static ReleaseAsset? PickAssetForPlatform(ReleaseInfo release)
    {
        bool Has(ReleaseAsset a, params string[] s) => s.Any(x => a.Name.Contains(x, StringComparison.OrdinalIgnoreCase));

        if (OperatingSystem.IsWindows())
            return release.Assets.FirstOrDefault(a => Has(a, "setup") && a.Name.EndsWith(".exe", StringComparison.OrdinalIgnoreCase))
                   ?? release.Assets.FirstOrDefault(a => a.Name.EndsWith(".exe", StringComparison.OrdinalIgnoreCase))
                   ?? release.Assets.FirstOrDefault(a => Has(a, "win") && a.Name.EndsWith(".zip", StringComparison.OrdinalIgnoreCase));
        if (OperatingSystem.IsMacOS())
            return release.Assets.FirstOrDefault(a => a.Name.EndsWith(".dmg", StringComparison.OrdinalIgnoreCase))
                   ?? release.Assets.FirstOrDefault(a => Has(a, "mac", "osx") && a.Name.EndsWith(".zip", StringComparison.OrdinalIgnoreCase));
        return release.Assets.FirstOrDefault(a => Has(a, "linux") && (a.Name.EndsWith(".tar.gz", StringComparison.OrdinalIgnoreCase) || a.Name.EndsWith(".AppImage", StringComparison.OrdinalIgnoreCase)));
    }
}
