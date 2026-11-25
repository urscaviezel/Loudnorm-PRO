using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using LoudnormPro.Core.Localization;
using LoudnormPro.Core.Platform;

namespace LoudnormPro.Core.Processing;

/// <summary>
/// Verarbeitet eine Liste von Dateien parallel.
/// Bei Hardware-Encodern + Auto-Jobs laufen Analyse und Encode mit getrennten Limits
/// als Pipeline (v1 machte erst alle Analysen, dann alle Encodes).
/// </summary>
public sealed class BatchProcessor
{
    private readonly ProcessingOptions _o;
    private readonly IBatchObserver _obs;
    private readonly ResumeStore _resume;

    public BatchProcessor(ProcessingOptions options, IBatchObserver observer, ResumeStore? resume = null)
    {
        _o = options;
        _obs = observer;
        _resume = resume ?? new ResumeStore();
    }

    public async Task<BatchSummary> RunAsync(IReadOnlyList<SourceFile> sources, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        var files = sources
            .GroupBy(s => ResumeStore.Key(s.Path))
            .Select(g => g.First())
            .OrderBy(s => s.Path, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var resumeSkipped = 0;
        if (_o.Resume && !_o.AnalyzeOnly)
        {
            var (remaining, skipped) = _resume.Filter(files, expectedPath: f => OutputPlanner.FinalPath(f, _o));
            foreach (var f in files.Except(remaining))
                _obs.FileStateChanged(f.Path, FileState.Skipped, null);
            files = remaining;
            resumeSkipped = skipped;
        }

        LogHeader(files.Count, resumeSkipped);

        var total = files.Count;
        if (total == 0)
        {
            _obs.OverallProgress(1, 0, 0, TimeSpan.Zero);
            return new BatchSummary(0, 0, 0, 0, resumeSkipped, false, sw.Elapsed, null);
        }

        if (!_o.OverwriteOriginal && !_o.AnalyzeOnly)
            Directory.CreateDirectory(_o.OutputRoot);

        var pipeline = new LoudnormPipeline(_o, _obs);
        var results = new ConcurrentBag<FileResult>();
        var progress = new ConcurrentDictionary<string, double>();
        var completed = 0;
        var analysisWeight = _o.AnalyzeOnly ? 1.0 : _o.Video.IsReencode ? 0.15 : 0.4;

        void Report()
        {
            var frac = Math.Clamp(progress.Values.Sum() / total, 0, 1);
            TimeSpan? eta = null;
            if (frac > 0.02 && sw.Elapsed.TotalSeconds > 5)
                eta = TimeSpan.FromSeconds(sw.Elapsed.TotalSeconds / frac * (1 - frac));
            _obs.OverallProgress(frac, Volatile.Read(ref completed), total, eta);
        }

        long lastReportTicks = 0;
        void ReportThrottled()
        {
            var now = sw.ElapsedMilliseconds;
            var last = Interlocked.Read(ref lastReportTicks);
            if (now - last < 300) return;
            if (Interlocked.CompareExchange(ref lastReportTicks, now, last) == last) Report();
        }

        Report();

        void Finish(SourceFile src, FileResult r)
        {
            results.Add(r);
            progress[src.Path] = 1.0;
            if (!_o.AnalyzeOnly)
            {
                try { _resume.Append(r); }
                catch (Exception ex) { _obs.Log(LogLevel.Warning, L.F("log.resume_write_failed", ("error", ex.Message))); }
            }
            var n = Interlocked.Increment(ref completed);
            var state = r.Status switch
            {
                JobStatus.Ok => FileState.Done,
                JobStatus.Analyzed => FileState.Analyzed,
                JobStatus.SkipExists => FileState.Skipped,
                _ => FileState.Failed,
            };
            _obs.FileStateChanged(src.Path, state, r);
            LogResult(n, total, r);
            Report();
        }

        using var analysisGate = new SemaphoreSlim(_o.SplitPipeline ? _o.AnalysisJobs : _o.EncodeJobs);
        using var encodeGate = new SemaphoreSlim(_o.EncodeJobs);

        async Task ProcessOne(SourceFile src)
        {
            var name = Path.GetFileName(src.Path);
            var jobId = Guid.NewGuid();
            var jobOpen = false;
            var holdsAnalysisGate = false;
            progress[src.Path] = 0;
            try
            {
                await analysisGate.WaitAsync(ct).ConfigureAwait(false);
                holdsAnalysisGate = true;
                FileJob? job;
                try
                {
                    _obs.JobStarted(jobId, name, L.T("phase.prepare"));
                    jobOpen = true;
                    _obs.FileStateChanged(src.Path, FileState.Analyzing, null);

                    var (j, early) = await pipeline.PrepareAsync(src, ct).ConfigureAwait(false);
                    if (early != null)
                    {
                        Finish(src, early);
                        return;
                    }
                    job = j!;

                    var analyzed = await pipeline.AnalyzeAsync(job, jobId,
                        f => { progress[src.Path] = f * analysisWeight; ReportThrottled(); }, ct).ConfigureAwait(false);
                    if (analyzed != null)
                    {
                        Finish(src, analyzed);
                        return;
                    }
                }
                finally
                {
                    if (_o.SplitPipeline)
                    {
                        analysisGate.Release();
                        holdsAnalysisGate = false;
                        if (jobOpen) { _obs.JobEnded(jobId); jobOpen = false; }
                    }
                }

                if (_o.SplitPipeline)
                {
                    await encodeGate.WaitAsync(ct).ConfigureAwait(false);
                    jobId = Guid.NewGuid();
                    _obs.JobStarted(jobId, name, L.T("phase.pass2"));
                    jobOpen = true;
                }

                try
                {
                    _obs.FileStateChanged(src.Path, FileState.Encoding, null);
                    var r = await pipeline.EncodeAsync(job, jobId,
                        f => { progress[src.Path] = analysisWeight + f * (1 - analysisWeight); ReportThrottled(); }, ct).ConfigureAwait(false);
                    Finish(src, r);
                }
                finally
                {
                    if (_o.SplitPipeline) encodeGate.Release();
                }
            }
            catch (OperationCanceledException)
            {
                _obs.FileStateChanged(src.Path, FileState.Cancelled, null);
            }
            catch (Exception ex)
            {
                Finish(src, new FileResult
                {
                    File = src.Path,
                    Output = string.Empty,
                    Status = JobStatus.ErrorPrepare,
                    Phase = L.T("phase.prepare"),
                    Details = ex.Message,
                    DisplayName = name,
                });
            }
            finally
            {
                // Im kombinierten Modus haelt der Job das Gate ueber alle Schritte
                if (holdsAnalysisGate) analysisGate.Release();
                if (jobOpen) _obs.JobEnded(jobId);
            }
        }

        await Task.WhenAll(files.Select(f => Task.Run(() => ProcessOne(f), CancellationToken.None))).ConfigureAwait(false);

        var cancelled = ct.IsCancellationRequested;
        var list = results.ToList();
        var ok = list.Count(r => r.Status is JobStatus.Ok or JobStatus.Analyzed);
        var skip = list.Count(r => r.Status == JobStatus.SkipExists);
        var err = list.Count(r => JobStatus.IsError(r.Status));

        string? runLog = null;
        if (!cancelled)
        {
            runLog = WriteRunLog(list);
            Report();
        }

        return new BatchSummary(total, ok, skip, err, resumeSkipped, cancelled, sw.Elapsed, runLog);
    }

    private void LogHeader(int count, int resumeSkipped)
    {
        var inv = CultureInfo.InvariantCulture;
        _obs.Log(LogLevel.Accent, $"Loudnorm PRO {AppInfo.Version}");
        _obs.Log(LogLevel.Info, $"{L.T("log.h.output"),-10}: {(_o.AnalyzeOnly ? "-" : _o.OverwriteOriginal ? L.T("log.overwrite_on") : _o.OutputRoot)}");
        _obs.Log(LogLevel.Info, $"{L.T("log.h.target"),-10}: {_o.Target.ToDisplay()}");
        _obs.Log(LogLevel.Info, $"{"Audio",-10}: {_o.AudioCodec} {_o.AudioBitrate} · {TrackModeText()}");
        _obs.Log(LogLevel.Info, $"{"Video",-10}: {_o.Video.ShortName}" + (_o.Video.IsReencode ? $" · {_o.Video.PresetLabel} / {_o.Video.QualityLabel}" : string.Empty));
        if (_o.Video.FallbackNotice != null)
            _obs.Log(LogLevel.Warning, L.F("log.encoder_fallback", ("mode", _o.Video.FallbackNotice)));
        _obs.Log(LogLevel.Info, _o.SplitPipeline
            ? $"{L.T("log.h.jobs"),-10}: {L.T("log.h.analysis")} {_o.AnalysisJobs} · Encode {_o.EncodeJobs}"
            : $"{L.T("log.h.jobs"),-10}: {_o.EncodeJobs.ToString(inv)}");
        _obs.Log(LogLevel.Info, $"{"ffmpeg",-10}: {_o.FfmpegPath}");
        _obs.Log(LogLevel.Info, $"{"Resume",-10}: {(_o.Resume ? L.T("common.on") : L.T("common.off"))}");
        if (_o.AnalyzeOnly) _obs.Log(LogLevel.Warning, L.T("log.analyze_only"));
        _obs.Log(LogLevel.Accent, L.F("log.total_files", ("count", count)));
        if (resumeSkipped > 0) _obs.Log(LogLevel.Muted, L.F("log.resume_skipped", ("count", resumeSkipped)));
    }

    private string TrackModeText()
    {
        var lang = L.T("lang." + _o.Tracks.PreferredLanguage);
        return _o.Tracks.Mode switch
        {
            AudioTrackMode.All => L.T("trackmode.all.log"),
            AudioTrackMode.PreferredOnly => L.F(_o.Tracks.PreferFirst ? "trackmode.preferred_first.log" : "trackmode.preferred.log", ("lang", lang)),
            _ => L.F("trackmode.auto.log", ("lang", lang)),
        };
    }

    private void LogResult(int n, int total, FileResult r)
    {
        var prefix = $"[{n}/{total}]";
        switch (r.Status)
        {
            case JobStatus.Ok:
                var lufs = r.MeasuredBefore.HasValue && r.MeasuredAfter.HasValue
                    ? string.Create(CultureInfo.InvariantCulture, $"  ({r.MeasuredBefore:0.0} → {r.MeasuredAfter:0.0} LUFS)")
                    : string.Empty;
                _obs.Log(LogLevel.Success, $"{prefix} ✓ {r.DisplayName}{lufs}");
                break;
            case JobStatus.Analyzed:
                _obs.Log(LogLevel.Success, $"{prefix} ✓ {r.DisplayName}: {r.Details}");
                break;
            case JobStatus.SkipExists:
                _obs.Log(LogLevel.Muted, $"{prefix} ↷ {r.DisplayName} — {L.T("job.skipped_exists")}");
                break;
            default:
                _obs.Log(LogLevel.Error, $"{prefix} ✗ {r.Status} — {r.DisplayName}");
                if (!string.IsNullOrWhiteSpace(r.Details))
                    _obs.Log(LogLevel.Muted, "    " + r.Details);
                break;
        }
    }

    private string? WriteRunLog(List<FileResult> results)
    {
        try
        {
            var path = AppPaths.RunLogFile;
            using var w = new StreamWriter(path, false, new System.Text.UTF8Encoding(false));
            w.WriteLine(Csv.Line("File", "Output", "Status", "Phase", "Details", "LUFS_Before", "LUFS_After", "Seconds"));
            foreach (var r in results.OrderBy(r => r.File, StringComparer.OrdinalIgnoreCase))
            {
                w.WriteLine(Csv.Line(r.File, r.Output, r.Status, r.Phase, r.Details,
                    r.MeasuredBefore?.ToString("0.0", CultureInfo.InvariantCulture),
                    r.MeasuredAfter?.ToString("0.0", CultureInfo.InvariantCulture),
                    r.Elapsed.TotalSeconds.ToString("0", CultureInfo.InvariantCulture)));
            }
            return path;
        }
        catch (Exception ex)
        {
            _obs.Log(LogLevel.Warning, ex.Message);
            return null;
        }
    }
}
