namespace LoudnormPro.Core.Processing;

public enum LogLevel
{
    Info,
    Muted,
    Accent,
    Success,
    Warning,
    Error,
}

public enum FileState
{
    Queued,
    Analyzing,
    Encoding,
    Done,
    Skipped,
    Failed,
    Cancelled,
    Analyzed,
}

/// <summary>Rueckmeldungen des Batch-Laufs an die Oberflaeche. Aufrufe kommen aus Hintergrund-Threads.</summary>
public interface IBatchObserver
{
    void Log(LogLevel level, string text);

    void JobStarted(Guid jobId, string displayName, string phase);

    void JobProgress(Guid jobId, string phase, int percent);

    void JobEnded(Guid jobId);

    void FileStateChanged(string file, FileState state, FileResult? result);

    void OverallProgress(double fraction, int completed, int total, TimeSpan? eta);
}

/// <summary>Beobachter, der nichts tut (fuer Tests).</summary>
public sealed class NullBatchObserver : IBatchObserver
{
    public static readonly NullBatchObserver Instance = new();
    public void Log(LogLevel level, string text) { }
    public void JobStarted(Guid jobId, string displayName, string phase) { }
    public void JobProgress(Guid jobId, string phase, int percent) { }
    public void JobEnded(Guid jobId) { }
    public void FileStateChanged(string file, FileState state, FileResult? result) { }
    public void OverallProgress(double fraction, int completed, int total, TimeSpan? eta) { }
}

public sealed record BatchSummary(int Total, int Ok, int Skipped, int Errors, int ResumeSkipped, bool Cancelled, TimeSpan Elapsed, string? RunLogPath);
