using System.Diagnostics;
using System.Globalization;
using LoudnormPro.Core.Platform;

namespace LoudnormPro.Core.Processing;

public sealed record FfmpegRunResult(int ExitCode, string StdErr, bool Cancelled, bool PrivateDataWarning);

/// <summary>
/// Fuehrt ffmpeg aus und meldet den Fortschritt.
/// v2: Fortschritt ueber "-progress pipe:1" statt Temp-Datei (plattformunabhaengig).
/// </summary>
public static class FfmpegRunner
{
    private const int MaxStdErrLines = 600;

    public static async Task<FfmpegRunResult> RunAsync(
        string ffmpegPath,
        IReadOnlyList<string> args,
        double durationSeconds,
        Action<int>? onPercent,
        CancellationToken ct)
    {
        var fullArgs = new List<string> { "-hide_banner", "-nostats", "-progress", "pipe:1" };
        fullArgs.AddRange(args);

        using var proc = new Process { StartInfo = ProcessRunner.CreateStartInfo(ffmpegPath, fullArgs), EnableRaisingEvents = true };
        var stderr = new Queue<string>();
        var privateDataWarning = false;
        var lastPct = -1;

        proc.ErrorDataReceived += (_, e) =>
        {
            if (e.Data == null) return;
            lock (stderr)
            {
                privateDataWarning |= e.Data.Contains("may not be recognized upon reading", StringComparison.OrdinalIgnoreCase);
                stderr.Enqueue(e.Data);
                while (stderr.Count > MaxStdErrLines) stderr.Dequeue();
            }
        };
        proc.OutputDataReceived += (_, e) =>
        {
            var line = e.Data;
            if (line == null || durationSeconds <= 0) return;
            // out_time_us (neu) bzw. out_time_ms (alt, ebenfalls Mikrosekunden)
            if (!line.StartsWith("out_time_us=", StringComparison.Ordinal) && !line.StartsWith("out_time_ms=", StringComparison.Ordinal))
                return;
            if (!double.TryParse(line.AsSpan(line.IndexOf('=') + 1), NumberStyles.Float, CultureInfo.InvariantCulture, out var us))
                return;
            var pct = (int)Math.Clamp(us / 1_000_000.0 / durationSeconds * 100.0, 0, 100);
            if (pct > lastPct)
            {
                lastPct = pct;
                onPercent?.Invoke(pct);
            }
        };

        proc.Start();
        proc.BeginErrorReadLine();
        proc.BeginOutputReadLine();

        var cancelled = false;
        try
        {
            await proc.WaitForExitAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            cancelled = true;
            ProcessRunner.TryKill(proc);
            try { await proc.WaitForExitAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false); }
            catch { /* ignoriert */ }
        }

        // Restliche Ausgabe einsammeln
        try { proc.WaitForExit(2000); } catch { /* ignoriert */ }

        string err;
        lock (stderr) err = string.Join(Environment.NewLine, stderr);
        int exit;
        try { exit = proc.ExitCode; } catch { exit = -1; }
        return new FfmpegRunResult(exit, err, cancelled, privateDataWarning);
    }
}
