using LoudnormPro.Core.Media;

namespace LoudnormPro.Core.Processing;

/// <summary>Bestimmt Ausgabe- und Temp-Pfade und erkennt bereits vorhandene Ergebnisse (aus v1).</summary>
public static class OutputPlanner
{
    public sealed record Plan(string FinalOutput, string EncodeOutput, bool OverwriteOriginal, string? ExistingOutput, string? Notice);

    public static Plan Create(SourceFile source, ProcessingOptions o)
    {
        var input = Path.GetFullPath(source.Path);
        var stem = Path.GetFileNameWithoutExtension(input);
        var ext = Path.GetExtension(input);
        var outExt = FileNaming.OutputExtensionFor(input, o.Video.IsReencode, o.OutputFormat, o.AudioCodec);
        var containerChanges = !string.Equals(outExt, ext, StringComparison.OrdinalIgnoreCase);

        // Muss der Container wechseln (WebM -> MKV, M4V+HEVC -> MP4), bleibt die Quelle erhalten.
        var overwrite = o.OverwriteOriginal && !containerChanges && !o.AnalyzeOnly;
        string? notice = o.OverwriteOriginal && containerChanges ? "webm_preserved" : null;

        if (overwrite)
        {
            string tempDir;
            string tmpName;
            if (o.Video.Mode == VideoModes.Amf || string.IsNullOrWhiteSpace(o.TempWorkDir))
            {
                // Neben der Quelle: gleiche Festplatte -> atomares Ersetzen (v1: nur AMF)
                tempDir = Path.GetDirectoryName(input)!;
                tmpName = o.Video.Mode == VideoModes.Amf ? $"{stem}.__ln_tmp_amf__{ext}" : $"{stem}.__ln_tmp__{ext}";
            }
            else
            {
                tempDir = EnsureWritableDir(o.TempWorkDir);
                tmpName = $"{stem}.__ln_tmp__{ext}";
            }
            return new Plan(input, Path.Combine(tempDir, tmpName), true, null, notice);
        }

        var final = FinalPath(source, o);
        var targetDir = Path.GetDirectoryName(final)!;
        var temp = Path.Combine(targetDir, $".__ln_tmp_{Guid.NewGuid():N}{outExt}");
        return new Plan(final, temp, false, File.Exists(final) ? final : null, notice);
    }

    public static string FinalPath(SourceFile source, ProcessingOptions o)
    {
        var input = Path.GetFullPath(source.Path);
        var outExt = FileNaming.OutputExtensionFor(input, o.Video.IsReencode, o.OutputFormat, o.AudioCodec);
        if (o.OverwriteOriginal && !o.AnalyzeOnly && string.Equals(outExt, Path.GetExtension(input), StringComparison.OrdinalIgnoreCase))
            return input;
        var root = Path.GetFullPath(source.Root);
        // Ohne Ausgabeordner (z. B. Ueberschreib-Modus mit Containerwechsel): neben die Quelle
        var outputRoot = string.IsNullOrWhiteSpace(o.OutputRoot) ? root : o.OutputRoot;
        var rel = Path.GetRelativePath(root, Path.GetDirectoryName(input)!);
        var targetDir = rel is "." or "" || rel.StartsWith("..", StringComparison.Ordinal)
            ? outputRoot
            : Path.Combine(outputRoot, rel);

        var tag = o.TagMode switch
        {
            "custom" when FileNaming.ValidTag(o.CustomTag) => o.CustomTag,
            "none" when !SamePath(Path.TrimEndingDirectorySeparator(targetDir), Path.GetDirectoryName(input)!) => "",
            _ => o.Video.Tag,
        };
        return Path.Combine(targetDir, Path.GetFileNameWithoutExtension(input) + tag + outExt);
    }

    public static bool SamePath(string a, string b)
    {
        var cmp = OperatingSystem.IsWindows() || OperatingSystem.IsMacOS() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        return string.Equals(Path.GetFullPath(a), Path.GetFullPath(b), cmp);
    }

    public static string EnsureWritableDir(string dir)
    {
        var full = Path.GetFullPath(dir.Trim().Trim('"'));
        Directory.CreateDirectory(full);
        var probe = Path.Combine(full, $".write_test_{Guid.NewGuid():N}.tmp");
        try
        {
            File.WriteAllText(probe, "ok");
        }
        catch (Exception ex)
        {
            throw new IOException($"Temp work dir is not writable: {full}", ex);
        }
        finally
        {
            try { File.Delete(probe); } catch { /* ignoriert */ }
        }
        return full;
    }
}
