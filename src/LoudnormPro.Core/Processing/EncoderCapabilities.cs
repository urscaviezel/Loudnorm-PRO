using LoudnormPro.Core.Platform;

namespace LoudnormPro.Core.Processing;

/// <summary>Welche HEVC-Encoder sind auf diesem System nutzbar?</summary>
public sealed record EncoderCapabilities
{
    public bool Nvenc { get; init; }
    public bool Amf { get; init; }
    public bool Vaapi { get; init; }
    public bool VideoToolbox { get; init; }
    public bool AvcNvenc { get; init; }
    public bool X264 { get; init; }
    public bool X265 { get; init; }
    public string? VaapiDevice { get; init; }
    public string GpuVendor { get; init; } = string.Empty;
    public string FfmpegVersion { get; init; } = string.Empty;

    public static readonly EncoderCapabilities None = new();

    public IEnumerable<string> AvailableModes()
    {
        yield return VideoModes.Copy;
        if (AvcNvenc) yield return VideoModes.AvcNvenc;
        if (X264) yield return VideoModes.X264;
        if (Nvenc) yield return VideoModes.Nvenc;
        if (Amf) yield return VideoModes.Amf;
        if (Vaapi) yield return VideoModes.Vaapi;
        if (VideoToolbox) yield return VideoModes.VideoToolbox;
        if (X265) yield return VideoModes.X265;
    }

    public bool Supports(string mode) => AvailableModes().Contains(mode);
}

public static class EncoderDetector
{
    public static async Task<EncoderCapabilities> DetectAsync(string? ffmpegPath, CancellationToken ct = default)
    {
        if (string.IsNullOrEmpty(ffmpegPath))
            return EncoderCapabilities.None;

        string encoders = string.Empty, version = string.Empty;
        try
        {
            var r = await ProcessRunner.RunAsync(ffmpegPath, new[] { "-hide_banner", "-encoders" }, TimeSpan.FromSeconds(10), ct);
            encoders = (r.StdOut + "\n" + r.StdErr).ToLowerInvariant();
            var v = await ProcessRunner.RunAsync(ffmpegPath, new[] { "-version" }, TimeSpan.FromSeconds(10), ct);
            version = ParseVersion(v.StdOut);
        }
        catch (OperationCanceledException) { throw; }
        catch
        {
            return EncoderCapabilities.None;
        }

        var encoderNames = encoders.Split('\n').Select(line => line.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries))
            .Where(parts => parts.Length >= 2 && parts[0].Length == 6)
            .Select(parts => parts[1]).ToHashSet(StringComparer.Ordinal);
        bool Has(string name) => encoderNames.Contains(name);

        var vendor = OperatingSystem.IsWindows() ? await DetectWindowsGpuVendorAsync(ct) : string.Empty;
        var caps = new EncoderCapabilities
        {
            X264 = Has("libx264"),
            X265 = Has("libx265"),
            FfmpegVersion = version,
            GpuVendor = vendor,
        };

        if (OperatingSystem.IsWindows())
        {
            return caps with
            {
                AvcNvenc = Has("h264_nvenc") && vendor == "nvidia",
                Nvenc = Has("hevc_nvenc") && vendor == "nvidia",
                Amf = Has("hevc_amf") && vendor == "amd",
            };
        }

        if (OperatingSystem.IsMacOS())
        {
            return caps with { VideoToolbox = Has("hevc_videotoolbox"), GpuVendor = "apple" };
        }

        var vaapiDevice = FindVaapiDevice();
        return caps with
        {
            AvcNvenc = Has("h264_nvenc") && HasNvidiaRuntimeLinux(),
            Nvenc = Has("hevc_nvenc") && HasNvidiaRuntimeLinux(),
            Vaapi = Has("hevc_vaapi") && vaapiDevice != null,
            VaapiDevice = vaapiDevice,
            GpuVendor = HasNvidiaRuntimeLinux() ? "nvidia" : vaapiDevice != null ? "vaapi" : string.Empty,
        };
    }

    public static string ParseVersion(string versionOutput)
    {
        var first = versionOutput.Split('\n').FirstOrDefault()?.Trim() ?? string.Empty;
        // "ffmpeg version 7.1-full_build-www.gyan.dev Copyright ..."
        const string marker = "ffmpeg version ";
        if (first.StartsWith(marker, StringComparison.OrdinalIgnoreCase))
        {
            var rest = first[marker.Length..];
            var space = rest.IndexOf(' ');
            return space > 0 ? rest[..space] : rest;
        }
        return first;
    }

    private static async Task<string> DetectWindowsGpuVendorAsync(CancellationToken ct)
    {
        try
        {
            var r = await ProcessRunner.RunAsync("powershell",
                new[] { "-NoProfile", "-NonInteractive", "-Command", "(Get-CimInstance Win32_VideoController | Select-Object -ExpandProperty Name) -join \"`n\"" },
                TimeSpan.FromSeconds(10), ct);
            var info = (r.StdOut + "\n" + r.StdErr).ToLowerInvariant();
            if (info.Contains("nvidia")) return "nvidia";
            if (info.Contains("amd") || info.Contains("radeon") || info.Contains("advanced micro devices")) return "amd";
            if (info.Contains("intel")) return "intel";
        }
        catch (OperationCanceledException) { throw; }
        catch
        {
            // PowerShell nicht verfuegbar
        }
        return string.Empty;
    }

    private static bool HasNvidiaRuntimeLinux() =>
        File.Exists("/dev/nvidia0") || File.Exists("/dev/nvidiactl") || File.Exists("/proc/driver/nvidia/version");

    private static string? FindVaapiDevice()
    {
        const string dri = "/dev/dri";
        try
        {
            if (!Directory.Exists(dri)) return null;
            var preferred = Path.Combine(dri, "renderD128");
            if (File.Exists(preferred)) return preferred;
            return Directory.GetFiles(dri, "renderD*").OrderBy(x => x, StringComparer.Ordinal).FirstOrDefault();
        }
        catch
        {
            return null;
        }
    }
}
