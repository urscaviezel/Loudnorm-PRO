using LoudnormPro.Core.Media;
using LoudnormPro.Core.Processing;

namespace LoudnormPro.Core.Services;

public sealed record DiagnosticsReport(
    string? FfmpegPath,
    string? FfprobePath,
    string FfmpegVersion,
    EncoderCapabilities Encoders,
    bool LoudnormTestPassed,
    string LoudnormTestDetails,
    TimeSpan TestDuration);

/// <summary>Prueft ffmpeg: Pfade, Version, Encoder und einen kurzen loudnorm-Testlauf mit einem Sinuston.</summary>
public static class FfmpegDiagnostics
{
    public static async Task<DiagnosticsReport> RunAsync(string? ffmpeg, string? ffprobe, CancellationToken ct = default)
    {
        var caps = await EncoderDetector.DetectAsync(ffmpeg, ct);
        if (string.IsNullOrEmpty(ffmpeg))
            return new DiagnosticsReport(ffmpeg, ffprobe, "-", caps, false, "ffmpeg not found", TimeSpan.Zero);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var args = new[]
        {
            "-loglevel", "info",
            "-f", "lavfi", "-i", "sine=frequency=1000:duration=3:sample_rate=48000",
            "-af", "loudnorm=" + LoudnessTarget.Default.ToFilterArgs() + ":print_format=json",
            "-f", "null", "-",
        };
        var r = await FfmpegRunner.RunAsync(ffmpeg, args, 3, null, ct);
        sw.Stop();
        var stats = LoudnormStats.FromFfmpegOutput(r.StdErr);
        var details = stats != null
            ? $"input_i={stats.InputI} LUFS, input_tp={stats.InputTp} dBTP, input_lra={stats.InputLra}"
            : string.Join(" | ", r.StdErr.Split('\n', StringSplitOptions.RemoveEmptyEntries).TakeLast(6).Select(x => x.Trim()));

        return new DiagnosticsReport(ffmpeg, ffprobe, caps.FfmpegVersion, caps, stats != null && r.ExitCode == 0, details, sw.Elapsed);
    }
}
