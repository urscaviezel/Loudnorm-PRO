namespace LoudnormPro.Core.Processing;

/// <summary>
/// Ersetzt die Originaldatei erst nach erfolgreicher Verarbeitung (aus v1).
/// Gleiches Laufwerk: atomares Verschieben. Anderes Laufwerk: erst kopieren, dann tauschen.
/// </summary>
public static class SafeReplace
{
    public static void Replace(string tempOutput, string finalOutput, bool preserveTimestamps)
    {
        tempOutput = Path.GetFullPath(tempOutput);
        finalOutput = Path.GetFullPath(finalOutput);

        if (!File.Exists(tempOutput)) throw new FileNotFoundException(tempOutput);
        if (OutputPlanner.SamePath(tempOutput, finalOutput))
            throw new IOException("Temporary output path must differ from final output path.");

        var targetDir = Path.GetDirectoryName(finalOutput) ?? ".";
        Directory.CreateDirectory(targetDir);

        DateTime? created = null, accessed = null, modified = null;
        if (File.Exists(finalOutput))
        {
            try
            {
                created = File.GetCreationTimeUtc(finalOutput);
                accessed = File.GetLastAccessTimeUtc(finalOutput);
                modified = File.GetLastWriteTimeUtc(finalOutput);
            }
            catch
            {
                // Zeitstempel nicht lesbar
            }
        }

        var sameVolume = string.Equals(Path.GetPathRoot(tempOutput), Path.GetPathRoot(finalOutput), StringComparison.OrdinalIgnoreCase);
        if (sameVolume)
        {
            File.Move(tempOutput, finalOutput, overwrite: true);
        }
        else
        {
            var swap = Path.Combine(targetDir, $".__replace_tmp__{Environment.ProcessId}_{Guid.NewGuid():N}_{Path.GetFileName(finalOutput)}");
            try
            {
                File.Copy(tempOutput, swap, overwrite: true);
                File.Move(swap, finalOutput, overwrite: true);
            }
            catch
            {
                try { if (File.Exists(swap)) File.Delete(swap); } catch { /* ignoriert */ }
                throw;
            }
            finally
            {
                try { if (File.Exists(tempOutput)) File.Delete(tempOutput); } catch { /* ignoriert */ }
            }
        }

        try
        {
            var now = DateTime.UtcNow;
            if (preserveTimestamps && modified.HasValue)
            {
                if (created.HasValue) File.SetCreationTimeUtc(finalOutput, created.Value);
                File.SetLastWriteTimeUtc(finalOutput, modified.Value);
                if (accessed.HasValue) File.SetLastAccessTimeUtc(finalOutput, accessed.Value);
            }
            else
            {
                File.SetCreationTimeUtc(finalOutput, now);
                File.SetLastWriteTimeUtc(finalOutput, now);
                File.SetLastAccessTimeUtc(finalOutput, now);
            }
        }
        catch
        {
            // Zeitstempel sind "nice to have"
        }
    }

    public static void TryDelete(string? path)
    {
        if (string.IsNullOrEmpty(path)) return;
        try { if (File.Exists(path)) File.Delete(path); } catch { /* ignoriert */ }
    }
}
