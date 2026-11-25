using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using LoudnormPro.Core.Media;
using LoudnormPro.Core.Platform;

namespace LoudnormPro.Core.Processing;

/// <summary>Stores a short fingerprint and five loudnorm measurements per audio track.</summary>
public sealed class AnalysisCache
{
    private static readonly object Gate = new();
    private readonly string _directory;
    private readonly string _key;
    private readonly string _source;
    private readonly string _fingerprint;
    private readonly Dictionary<int, LoudnormStats> _tracks;

    private AnalysisCache(string directory, string key, string source, string fingerprint)
    {
        _directory = directory;
        _key = key;
        _source = source;
        _fingerprint = fingerprint;
        lock (Gate)
        {
            var path = ResolvePath(directory, source, key);
            MigrateFile(directory, source, key, path);
            _tracks = Read(path);
        }
    }

    private static string CacheKey(string source, LoudnessTarget target, FileInfo ffmpeg)
    {
        var key = string.Join('\n', "loudnorm-1", ResumeStore.Key(source), target.ToFilterArgs(),
            ffmpeg.FullName, ffmpeg.Length.ToString(CultureInfo.InvariantCulture),
            ffmpeg.LastWriteTimeUtc.Ticks.ToString(CultureInfo.InvariantCulture));
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(key)));
    }

    // Keep the ordinary case directly in analysis-cache. Colliding names use a
    // separate directory, so the .txt filename always matches the source.
    private static string ResolvePath(string directory, string source, string key)
    {
        var name = Path.GetFileName(source);
        // Leave room for .txt on filesystems with a 255-byte component limit.
        while (Encoding.UTF8.GetByteCount(name) > 250)
            name = name[..(char.IsLowSurrogate(name[^1]) ? ^2 : ^1)];
        var primary = Path.Combine(directory, name + ".txt");
        var variant = Path.Combine(directory, "Varianten", key, name + ".txt");
        if (File.Exists(variant)) return variant;
        if (!File.Exists(primary)) return primary;
        using var reader = OpenReader(primary);
        return reader.ReadLine() == key ? primary : variant;
    }

    private static StreamReader OpenReader(string path) => new(
        new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete));

    private static void WriteAtomically(string path, IEnumerable<string> lines, bool replace)
    {
        var directory = Path.GetDirectoryName(path)!;
        Directory.CreateDirectory(directory);
        var temp = Path.Combine(directory, Guid.NewGuid().ToString("N") + ".tmp");
        try
        {
            File.WriteAllLines(temp, lines);
            File.Move(temp, path, overwrite: replace);
        }
        finally { SafeReplace.TryDelete(temp); }
    }

    private static void MigrateFile(string directory, string source, string key, string path)
    {
        var old = Path.Combine(directory, key + ".txt");
        if (!File.Exists(old)) return;
        if (!File.Exists(path))
            WriteAtomically(path, new[] { key, source }.Concat(File.ReadLines(old)), false);
        // Delete only after the new file has been committed.
        File.Delete(old);
    }

    public static void MigrateKnownFiles(string ffmpegPath, Settings.AppSettings settings)
    {
        try
        {
            var directory = Path.Combine(AppPaths.DataDir, "analysis-cache");
            if (!Directory.Exists(directory)) return;
            var legacy = Directory.EnumerateFiles(directory, "*.txt").Select(Path.GetFileNameWithoutExtension)
                .Where(n => n is { Length: 64 } && n.All(Uri.IsHexDigit)).ToHashSet();
            if (legacy.Count == 0) return;
            var ffmpeg = new FileInfo(ffmpegPath);
            var targets = LoudnessPresets.All.Values.Append(settings.Target)
                .Concat(settings.Profiles.Select(p => new LoudnessTarget(p.TargetI, p.TargetTp, p.TargetLra))).Distinct().ToArray();
            var files = settings.RecentFiles.Concat(new ResumeStore().Load().Values.Select(r => r["File"]));
            if (File.Exists(AppPaths.RunLogFile))
                files = files.Concat(Csv.ParseWithHeader(File.ReadAllText(AppPaths.RunLogFile)).Select(r => r.GetValueOrDefault("File") ?? ""));
            foreach (var source in files.Where(Path.IsPathFullyQualified).Distinct())
            {
                foreach (var target in targets)
                {
                    var key = CacheKey(source, target, ffmpeg);
                    if (!legacy.Remove(key)) continue;
                    lock (Gate) MigrateFile(directory, source, key, ResolvePath(directory, source, key));
                }
                if (legacy.Count == 0) break;
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException) { /* Retry on next use. */ }
    }

    public static AnalysisCache? Open(string source, ProcessingOptions options)
    {
        try
        {
            var key = CacheKey(source, options.Target, new FileInfo(options.FfmpegPath));
            return new AnalysisCache(Path.Combine(AppPaths.DataDir, "analysis-cache"), key, source, Fingerprint(source));
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { return null; }
    }

    private static string Fingerprint(string source)
    {
        using var file = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        var length = file.Length;
        var buffer = new byte[(int)Math.Min(4096L, length)];
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var offset in new[] { 0L, (length - buffer.Length) / 2, length - buffer.Length }.Distinct())
        {
            file.Position = offset;
            file.ReadExactly(buffer);
            hash.AppendData(buffer);
        }
        return FormattableString.Invariant($"{length} {File.GetLastWriteTimeUtc(source).Ticks} {Convert.ToHexString(hash.GetHashAndReset())}");
    }

    private Dictionary<int, LoudnormStats> Read(string path)
    {
        var tracks = new Dictionary<int, LoudnormStats>();
        if (!File.Exists(path)) return tracks;
        using var reader = OpenReader(path);
        if (reader.ReadLine() != _key) return tracks;
        reader.ReadLine(); // Original source path for identifying equally named files.
        if (reader.ReadLine() != _fingerprint) return tracks;
        while (reader.ReadLine() is string line)
        {
            var values = line.Split(' ');
            if (values.Length == 6 && int.TryParse(values[0], NumberStyles.None, CultureInfo.InvariantCulture, out var idx)
                && values.Skip(1).All(IsNumber))
                tracks[idx] = new(values[1], values[2], values[3], values[4], values[5]);
        }
        return tracks;
    }

    private static bool IsNumber(string value) =>
        double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var number) && double.IsFinite(number);

    public LoudnormStats? Get(int index) => _tracks.GetValueOrDefault(index);

    public void Save(IReadOnlyDictionary<int, LoudnormStats> measured)
    {
        try
        {
            lock (Gate)
            {
                if (Fingerprint(_source) != _fingerprint) return;
                var path = ResolvePath(_directory, _source, _key);
                var replace = File.Exists(path);
                var tracks = Read(path);
                foreach (var (index, stats) in measured) tracks[index] = stats;
                WriteAtomically(path, new[] { _key, _source, _fingerprint }.Concat(tracks.Select(t =>
                    FormattableString.Invariant($"{t.Key} {t.Value.InputI} {t.Value.InputTp} {t.Value.InputLra} {t.Value.InputThresh} {t.Value.TargetOffset}"))), replace);
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { /* Optional cache. */ }
    }
}
