using System.Globalization;
using LoudnormPro.Core.Platform;

namespace LoudnormPro.Core.Processing;

/// <summary>Merkt sich erledigte Dateien, damit ein abgebrochener Lauf fortgesetzt werden kann (CSV-Format wie v1).</summary>
public sealed class ResumeStore
{
    private static readonly string[] Header = { "Timestamp", "File", "Output", "Status", "Phase", "Details" };
    private readonly string _path;
    private readonly object _gate = new();

    public ResumeStore(string? path = null) => _path = path ?? AppPaths.ResumeFile;

    public string FilePath => _path;

    public static string Key(string file)
    {
        var full = Path.GetFullPath(file.Trim());
        return OperatingSystem.IsLinux() ? full : full.ToLowerInvariant();
    }

    public Dictionary<string, Dictionary<string, string>> Load()
    {
        var state = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);
        lock (_gate)
        {
            if (!File.Exists(_path)) return state;
            foreach (var row in Csv.ParseWithHeader(File.ReadAllText(_path)))
            {
                var f = row.GetValueOrDefault("File")?.Trim();
                if (string.IsNullOrEmpty(f)) continue;
                try { state[Key(f)] = row; } catch { /* ungueltiger Pfad */ }
            }
        }
        return state;
    }

    public HashSet<string> KnownOutputs()
    {
        var outputs = new HashSet<string>(StringComparer.Ordinal);
        lock (_gate)
        {
            if (!File.Exists(_path)) return outputs;
            foreach (var row in Csv.ParseWithHeader(File.ReadAllText(_path)))
            {
                var source = row.GetValueOrDefault("File");
                var output = row.GetValueOrDefault("Output");
                if (row.GetValueOrDefault("Status") != JobStatus.Ok || string.IsNullOrWhiteSpace(source) || string.IsNullOrWhiteSpace(output)) continue;
                try
                {
                    if (!OutputPlanner.SamePath(source, output)) outputs.Add(Key(output));
                }
                catch (ArgumentException) { /* Invalid legacy entry. */ }
            }
        }
        return outputs;
    }

    public void Append(FileResult r)
    {
        lock (_gate)
        {
            var exists = File.Exists(_path) && new FileInfo(_path).Length > 0;
            using var w = new StreamWriter(_path, append: true, new System.Text.UTF8Encoding(false));
            if (!exists) w.WriteLine(Csv.Line(Header));
            w.WriteLine(Csv.Line(
                DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
                r.File, r.Output, r.Status, r.Phase, r.Details));
        }
    }

    /// <summary>Entfernt Dateien, die bereits erfolgreich verarbeitet wurden und deren Ausgabe noch existiert.</summary>
    public (List<SourceFile> Remaining, int Skipped) Filter(IReadOnlyList<SourceFile> files, Func<SourceFile, string>? expectedExtension = null, Func<SourceFile, string>? expectedPath = null)
    {
        var state = Load();
        if (state.Count == 0) return (files.ToList(), 0);

        var remaining = new List<SourceFile>();
        var skipped = 0;
        foreach (var f in files)
        {
            if (state.TryGetValue(Key(f.Path), out var row))
            {
                var status = (row.GetValueOrDefault("Status") ?? "").Trim().ToUpperInvariant();
                var output = (row.GetValueOrDefault("Output") ?? "").Trim();
                if (status is JobStatus.Ok or JobStatus.SkipExists && output.Length > 0 && File.Exists(output)
                    && (expectedPath == null || OutputPlanner.SamePath(output, expectedPath(f)))
                    && (expectedExtension == null || string.Equals(Path.GetExtension(output), expectedExtension(f), StringComparison.OrdinalIgnoreCase)))
                {
                    skipped++;
                    continue;
                }
            }
            remaining.Add(f);
        }
        return (remaining, skipped);
    }

    public void Clear()
    {
        lock (_gate)
        {
            try { if (File.Exists(_path)) File.Delete(_path); } catch { /* ignoriert */ }
        }
    }
}
