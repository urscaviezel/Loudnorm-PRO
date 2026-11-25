using System.IO.Pipes;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;

namespace LoudnormPro.App.Services;

/// <summary>
/// Erlaubt nur eine laufende Instanz pro Benutzer.
/// Eine zweite Instanz uebergibt ihre Kommandozeilen-Dateien an die erste, die sich
/// wiederherstellt und in den Vordergrund holt; danach beendet sich die zweite sofort.
/// Funktioniert auf Windows, macOS und Linux (benannter Mutex + Named Pipe).
/// </summary>
public sealed class SingleInstance : IDisposable
{
    private const string ActivateCommand = "::activate::";
    private readonly Mutex _mutex;
    private readonly string _pipeName;
    private readonly CancellationTokenSource _cts = new();

    public bool IsFirstInstance { get; }

    /// <summary>Wird im UI-Thread-unabhaengigen Kontext aufgerufen: Liste der uebergebenen Pfade (kann leer sein).</summary>
    public event Action<IReadOnlyList<string>>? ActivationRequested;

    public SingleInstance()
    {
        var id = BuildId();
        _pipeName = "LoudnormPRO-" + id;
        _mutex = new Mutex(true, (OperatingSystem.IsWindows() ? @"Local\" : string.Empty) + "LoudnormPRO-" + id, out var created);
        IsFirstInstance = created;
    }

    /// <summary>Eindeutig pro Benutzer; Debug-Builds getrennt, damit sie eine installierte Version nicht blockieren.</summary>
    private static string BuildId()
    {
        var key = Environment.UserName + "|" + Environment.UserDomainName;
#if DEBUG
        key += "|debug";
#endif
        if (OperatingSystem.IsWindows() && LoudnormPro.Core.Platform.AppPaths.IsPortable)
            key += "|portable|" + Path.GetFullPath(AppContext.BaseDirectory).ToUpperInvariant();
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(key));
        return Convert.ToHexString(hash, 0, 8);
    }

    /// <summary>Zweite Instanz: Pfade an die erste senden.</summary>
    public bool SignalFirstInstance(IEnumerable<string> args)
    {
        try
        {
            if (OperatingSystem.IsWindows())
                AllowSetForegroundWindow(-1); // ASFW_ANY: die erste Instanz darf sich nach vorne holen

            using var client = new NamedPipeClientStream(".", _pipeName, PipeDirection.Out);
            client.Connect(3000);
            using var writer = new StreamWriter(client, new UTF8Encoding(false)) { AutoFlush = true };
            writer.WriteLine(ActivateCommand);
            foreach (var a in args)
            {
                var full = Path.GetFullPath(a);
                writer.WriteLine(full);
            }
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>Erste Instanz: auf Aktivierungswuensche weiterer Instanzen warten.</summary>
    public void StartListening()
    {
        if (!IsFirstInstance) return;
        _ = Task.Run(async () =>
        {
            while (!_cts.IsCancellationRequested)
            {
                try
                {
                    await using var server = new NamedPipeServerStream(_pipeName, PipeDirection.In, 1,
                        PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
                    await server.WaitForConnectionAsync(_cts.Token);
                    using var reader = new StreamReader(server, Encoding.UTF8);
                    var paths = new List<string>();
                    string? line;
                    while ((line = await reader.ReadLineAsync(_cts.Token)) != null)
                    {
                        if (line == ActivateCommand || string.IsNullOrWhiteSpace(line)) continue;
                        paths.Add(line);
                    }
                    ActivationRequested?.Invoke(paths);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch
                {
                    await Task.Delay(250);
                }
            }
        });
    }

    public void Dispose()
    {
        _cts.Cancel();
        if (IsFirstInstance)
        {
            try { _mutex.ReleaseMutex(); } catch { /* ignoriert */ }
        }
        _mutex.Dispose();
    }

    [DllImport("user32.dll")]
    private static extern bool AllowSetForegroundWindow(int dwProcessId);
}
