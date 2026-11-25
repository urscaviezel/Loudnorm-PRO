using System.Diagnostics;
using System.Text;

namespace LoudnormPro.Core.Platform;

public sealed record ProcessResult(int ExitCode, string StdOut, string StdErr, bool TimedOut);

/// <summary>Startet externe Programme ohne Konsolenfenster und liest die Ausgabe ein.</summary>
public static class ProcessRunner
{
    public static ProcessStartInfo CreateStartInfo(string fileName, IEnumerable<string> args)
    {
        var psi = new ProcessStartInfo
        {
            FileName = fileName,
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            RedirectStandardInput = false,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8,
        };
        foreach (var a in args)
            psi.ArgumentList.Add(a);
        return psi;
    }

    public static async Task<ProcessResult> RunAsync(
        string fileName,
        IEnumerable<string> args,
        TimeSpan? timeout = null,
        CancellationToken ct = default)
    {
        using var proc = new Process { StartInfo = CreateStartInfo(fileName, args) };
        var stdout = new StringBuilder();
        var stderr = new StringBuilder();
        proc.OutputDataReceived += (_, e) => { if (e.Data != null) lock (stdout) stdout.AppendLine(e.Data); };
        proc.ErrorDataReceived += (_, e) => { if (e.Data != null) lock (stderr) stderr.AppendLine(e.Data); };

        proc.Start();
        proc.BeginOutputReadLine();
        proc.BeginErrorReadLine();

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        if (timeout.HasValue)
            cts.CancelAfter(timeout.Value);

        var timedOut = false;
        try
        {
            await proc.WaitForExitAsync(cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            timedOut = !ct.IsCancellationRequested;
            TryKill(proc);
            if (ct.IsCancellationRequested)
                throw;
        }

        string o, e2;
        lock (stdout) o = stdout.ToString();
        lock (stderr) e2 = stderr.ToString();
        return new ProcessResult(timedOut ? -1 : SafeExitCode(proc), o, e2, timedOut);
    }

    public static void TryKill(Process proc)
    {
        try
        {
            if (!proc.HasExited)
                proc.Kill(entireProcessTree: true);
        }
        catch
        {
            // Prozess bereits beendet
        }
    }

    private static int SafeExitCode(Process p)
    {
        try { return p.ExitCode; }
        catch { return -1; }
    }

    /// <summary>Startet ein Programm "fire and forget" (z. B. Energieoptionen).</summary>
    public static bool TryStartDetached(string fileName, params string[] args)
    {
        try
        {
            var psi = new ProcessStartInfo { FileName = fileName, UseShellExecute = false, CreateNoWindow = true };
            foreach (var a in args) psi.ArgumentList.Add(a);
            Process.Start(psi);
            return true;
        }
        catch
        {
            return false;
        }
    }
}
