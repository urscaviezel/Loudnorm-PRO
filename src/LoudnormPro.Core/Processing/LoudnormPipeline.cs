using System.Globalization;
using LoudnormPro.Core.Localization;
using LoudnormPro.Core.Media;

namespace LoudnormPro.Core.Processing;

/// <summary>Die drei Schritte pro Datei: Vorbereiten, Pass 1 (Analyse), Pass 2 (Normalisierung).</summary>
public sealed class LoudnormPipeline
{
    private readonly ProcessingOptions _o;
    private readonly IBatchObserver _obs;
    private readonly MediaProbe _probe;

    /// <summary>Mindestgroesse einer gueltigen Ausgabe (v1: 1 MB; v2 kleiner, damit kurze Clips nicht scheitern).</summary>
    public const long MinValidOutputBytes = 16 * 1024;

    public LoudnormPipeline(ProcessingOptions options, IBatchObserver observer)
    {
        _o = options;
        _obs = observer;
        _probe = new MediaProbe(options.FfprobePath);
    }

    private FileResult Result(FileJob? job, SourceFile src, string status, string phase, string details, DateTime started, string? output = null) => new()
    {
        File = src.Path,
        Output = output ?? job?.Output ?? string.Empty,
        Status = status,
        Phase = phase,
        Details = details,
        DisplayName = Path.GetFileName(src.Path),
        Elapsed = DateTime.UtcNow - started,
        MeasuredBefore = job?.Stats.Values.FirstOrDefault()?.InputIntegrated,
    };

    /// <summary>Plant Ausgabe, prueft vorhandene Ergebnisse und liest die Audiospuren.</summary>
    public async Task<(FileJob? Job, FileResult? Early)> PrepareAsync(SourceFile src, CancellationToken ct)
    {
        var started = DateTime.UtcNow;
        var name = Path.GetFileName(src.Path);

        OutputPlanner.Plan plan;
        try
        {
            plan = OutputPlanner.Create(src, _o);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return (null, Result(null, src, JobStatus.ErrorPrepare, L.T("phase.prepare"), ex.Message, started));
        }

        if (plan.Notice == "webm_preserved")
            _obs.Log(LogLevel.Warning, L.F("log.webm_preserved", ("name", name), ("ext", Path.GetExtension(plan.FinalOutput).ToUpperInvariant().TrimStart('.'))));

        if (plan.ExistingOutput != null && !_o.AnalyzeOnly)
            return (null, Result(null, src, JobStatus.SkipExists, L.T("job.skipped"), string.Empty, started, plan.ExistingOutput));

        if (!_o.AnalyzeOnly && _o.AudioCodec == "eac3" &&
            string.Equals(Path.GetExtension(plan.FinalOutput), ".m4v", StringComparison.OrdinalIgnoreCase))
            return (null, Result(null, src, JobStatus.ErrorPrepare, L.T("phase.prepare"),
                L.T("error.m4v_eac3"), started, plan.FinalOutput));

        var extension = Path.GetExtension(plan.FinalOutput);
        if (!_o.AnalyzeOnly && (!FileNaming.SupportsAudio(extension, _o.AudioCodec) ||
            (extension.Equals(".webm", StringComparison.OrdinalIgnoreCase) && _o.Video.IsReencode)))
            return (null, Result(null, src, JobStatus.ErrorPrepare, L.T("phase.prepare"),
                L.T("error.codec_container"), started, plan.FinalOutput));

        if (plan.OverwriteOriginal)
            _obs.Log(LogLevel.Muted, L.F("log.overwrite_prepared", ("name", name), ("path", plan.EncodeOutput)));

        var info = await _probe.ProbeAsync(src.Path, ct).ConfigureAwait(false);
        var job = new FileJob
        {
            File = src.Path,
            DisplayName = name,
            Output = plan.FinalOutput,
            EncodeOutput = plan.EncodeOutput,
            OverwriteOriginal = plan.OverwriteOriginal,
            DurationSeconds = info.DurationSeconds,
            AudioStreams = info.AudioStreams,
            Started = started,
        };

        if (info.AudioStreams.Count == 0)
            return (null, Result(job, src, JobStatus.ErrorNoAudio, L.T("phase.prepare"), L.T("error.no_audio"), started));

        job.SelectedIndices = _o.Tracks.SelectedIndices(info.AudioStreams);
        job.OutputOrder = _o.Tracks.OutputOrder(info.AudioStreams);
        job.DefaultOutputIndex = _o.Tracks.DefaultOutputIndex(info.AudioStreams, job.OutputOrder);

        if (job.SelectedIndices.Count == 0)
        {
            var tags = info.AudioStreams.Select(s => string.IsNullOrWhiteSpace(s.Language)
                ? "UND" : s.Language.ToUpperInvariant()).Distinct();
            var details = L.F("error.no_matching_audio", ("lang", L.T("lang." + _o.Tracks.PreferredLanguage)),
                ("tags", string.Join(", ", tags)));
            return (null, Result(job, src, JobStatus.ErrorNoMatchingAudio, L.T("phase.prepare"), details, started));
        }

        if (!_o.AnalyzeOnly && FileNaming.IsWebm(plan.FinalOutput) &&
            (!info.CanCopyOtherStreamsToWebm || job.OutputOrder.Except(job.SelectedIndices)
                .Any(i => info.AudioStreams[i].Codec is not ("opus" or "vorbis"))))
            return (null, Result(job, src, JobStatus.ErrorPrepare, L.T("phase.prepare"), L.T("error.webm_tracks"), started));

        if (_o.Tracks.Mode == AudioTrackMode.Auto)
        {
            var pref = _o.Tracks.PreferredIndices(info.AudioStreams);
            _obs.Log(LogLevel.Muted, pref.Count > 0
                ? L.F("log.auto_uses_track", ("name", name), ("track", pref[0] + 1))
                : L.F("log.auto_no_preferred", ("name", name)));
        }

        return (job, null);
    }

    /// <summary>Pass 1: Lautheit jeder gewaehlten Spur messen.</summary>
    public async Task<FileResult?> AnalyzeAsync(FileJob job, Guid jobId, Action<double> fileProgress, CancellationToken ct)
    {
        var count = job.SelectedIndices.Count;
        var pos = 0;
        var cache = AnalysisCache.Open(job.File, _o);
        var reused = 0;
        foreach (var idx in job.SelectedIndices)
        {
            pos++;
            ct.ThrowIfCancellationRequested();
            if (cache?.Get(idx) is { } cached)
            {
                job.Stats[idx] = cached;
                reused++;
                fileProgress((double)pos / count);
                continue;
            }
            var phase = L.F("phase.pass1_track", ("current", pos), ("total", count));
            _obs.JobProgress(jobId, phase, 0);

            var args = new List<string>
            {
                "-loglevel", "info",
                "-i", job.File,
                "-map", $"0:a:{idx}",
                "-vn", "-sn", "-dn",
                "-af", $"loudnorm={_o.Target.ToFilterArgs()}:print_format=json",
                "-f", "null", "-",
            };

            var p = pos;
            var r = await FfmpegRunner.RunAsync(_o.FfmpegPath, args, job.DurationSeconds, pct =>
            {
                _obs.JobProgress(jobId, phase, pct);
                fileProgress(((p - 1) + pct / 100.0) / count);
            }, ct).ConfigureAwait(false);

            if (r.Cancelled) throw new OperationCanceledException(ct);

            var stats = LoudnormStats.FromFfmpegOutput(r.StdErr);
            if (r.ExitCode != 0 || stats == null)
            {
                var tail = string.Join(" | ", r.StdErr.Split('\n', StringSplitOptions.RemoveEmptyEntries).TakeLast(12).Select(s => s.Trim()));
                return new FileResult
                {
                    File = job.File,
                    Output = job.Output,
                    Status = JobStatus.ErrorPass1,
                    Phase = L.F("phase.pass1_track_n", ("track", idx + 1)),
                    Details = tail.Length > 0 ? tail : L.F("error.pass1_missing_stats", ("track", idx + 1)),
                    DisplayName = job.DisplayName,
                    Elapsed = DateTime.UtcNow - job.Started,
                };
            }
            job.Stats[idx] = stats;
        }

        if (reused > 0)
            _obs.Log(LogLevel.Info, L.F("log.analysis_reused", ("name", job.DisplayName), ("count", reused)));
        if (reused < count) cache?.Save(job.Stats);

        if (_o.AnalyzeOnly)
        {
            var details = string.Join("; ", job.Stats.Select(kv =>
                string.Create(CultureInfo.InvariantCulture, $"#{kv.Key + 1}: I={kv.Value.InputI} LUFS, TP={kv.Value.InputTp} dBTP, LRA={kv.Value.InputLra}")));
            return new FileResult
            {
                File = job.File,
                Output = string.Empty,
                Status = JobStatus.Analyzed,
                Phase = L.T("phase.analysis_done"),
                Details = details,
                DisplayName = job.DisplayName,
                Elapsed = DateTime.UtcNow - job.Started,
                MeasuredBefore = job.Stats.Values.FirstOrDefault()?.InputIntegrated,
            };
        }

        return null; // weiter mit Pass 2
    }

    /// <summary>Pass 2: normalisieren und schreiben.</summary>
    public async Task<FileResult> EncodeAsync(FileJob job, Guid jobId, Action<double> fileProgress, CancellationToken ct)
    {
        var phase = L.T("phase.pass2");
        _obs.JobProgress(jobId, phase, 0);

        var args = BuildPass2Args(job, _o);
        var outDir = Path.GetDirectoryName(job.EncodeOutput);
        if (!string.IsNullOrEmpty(outDir)) Directory.CreateDirectory(outDir);

        FfmpegRunResult r;
        try
        {
            r = await FfmpegRunner.RunAsync(_o.FfmpegPath, args, job.DurationSeconds, pct =>
            {
                _obs.JobProgress(jobId, phase, pct);
                fileProgress(pct / 100.0);
            }, ct).ConfigureAwait(false);
        }
        catch
        {
            SafeReplace.TryDelete(job.EncodeOutput);
            throw;
        }

        if (r.Cancelled)
        {
            // v2: auch im Normalmodus die halbfertige Datei entfernen, sonst wuerde sie beim
            // naechsten Lauf als "bereits vorhanden" uebersprungen.
            SafeReplace.TryDelete(job.EncodeOutput);
            throw new OperationCanceledException(ct);
        }

        var after = LoudnormSummary.ParseOutputIntegrated(r.StdErr);
        var before = job.Stats.Values.FirstOrDefault()?.InputIntegrated;
        var ok = r.ExitCode == 0 && File.Exists(job.EncodeOutput) && new FileInfo(job.EncodeOutput).Length >= MinValidOutputBytes;

        // MPEG-TS can accept incompatible tracks as private data and still return success.
        var privateDataWarning = _o.OutputFormat == "ts" && r.PrivateDataWarning;
        if (!ok || privateDataWarning)
        {
            SafeReplace.TryDelete(job.EncodeOutput);
            _obs.Log(LogLevel.Muted, "ffmpeg " + string.Join(" ", args.Select(QuoteArg)));
            var tail = string.Join(" | ", r.StdErr.Split('\n', StringSplitOptions.RemoveEmptyEntries).TakeLast(12).Select(s => s.Trim()));
            return new FileResult
            {
                File = job.File,
                Output = job.Output,
                Status = JobStatus.ErrorPass2,
                Phase = "Pass 2",
                Details = privateDataWarning ? L.T("error.ts_private_data") : tail.Length > 0 ? tail : L.T("error.pass2_failed"),
                DisplayName = job.DisplayName,
                Elapsed = DateTime.UtcNow - job.Started,
                MeasuredBefore = before,
            };
        }

        if (job.OverwriteOriginal)
        {
            try
            {
                SafeReplace.Replace(job.EncodeOutput, job.Output, _o.PreserveTimestamp);
                _obs.Log(LogLevel.Muted, L.F("log.overwrite_done", ("name", job.DisplayName)));
            }
            catch (Exception ex)
            {
                SafeReplace.TryDelete(job.EncodeOutput);
                return new FileResult
                {
                    File = job.File,
                    Output = job.Output,
                    Status = JobStatus.ErrorReplace,
                    Phase = L.T("job.done"),
                    Details = L.F("error.replace_failed", ("error", ex.Message)),
                    DisplayName = job.DisplayName,
                    Elapsed = DateTime.UtcNow - job.Started,
                    MeasuredBefore = before,
                    MeasuredAfter = after,
                };
            }
        }

        if (!job.OverwriteOriginal && !OutputPlanner.SamePath(job.EncodeOutput, job.Output))
        {
            try { File.Move(job.EncodeOutput, job.Output); }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                SafeReplace.TryDelete(job.EncodeOutput);
                return new FileResult { File = job.File, Output = job.Output, Status = JobStatus.ErrorPass2,
                    Phase = phase, Details = ex.Message, DisplayName = job.DisplayName, Elapsed = DateTime.UtcNow - job.Started };
            }
        }

        return new FileResult
        {
            File = job.File,
            Output = job.Output,
            Status = JobStatus.Ok,
            Phase = "Pass 2",
            DisplayName = job.DisplayName,
            Elapsed = DateTime.UtcNow - job.Started,
            MeasuredBefore = before,
            MeasuredAfter = after,
        };
    }

    private static string QuoteArg(string a) => a.Contains(' ') || a.Contains(';') ? $"\"{a}\"" : a;

    /// <summary>Argumente fuer Pass 2 (1:1 aus v1, Zielwerte jetzt einstellbar). Oeffentlich fuer Tests.</summary>
    public static List<string> BuildPass2Args(FileJob job, ProcessingOptions o)
    {
        var target = o.Target.ToFilterArgs();
        // loudnorm arbeitet intern mit 192 kHz. Das Zuruecksetzen auf die Original-Abtastrate passiert
        // direkt in der Filterkette (aresample). Ein "-ar" als Ausgabeoption fuehrt bei neueren
        // ffmpeg-Versionen (8.x) zu "Invalid argument" im Filtergraph.
        var filterParts = job.SelectedIndices.Select(idx =>
        {
            var s = job.Stats[idx];
            var info = job.AudioStreams[idx];
            var rate = o.AudioCodec != "libopus" && info.HasNumericSampleRate ? info.SampleRate : "48000";
            // Opus uses rear surround labels for five/six channels. Relabel the
            // side pair after measurement without mixing or dropping channels.
            var channelMap = o.AudioCodec == "libopus" ? info.ChannelLayout switch
            {
                "5.1(side)" => ",channelmap=map=FL-FL|FR-FR|FC-FC|LFE-LFE|SL-BL|SR-BR:channel_layout=5.1",
                "5.0(side)" => ",channelmap=map=FL-FL|FR-FR|FC-FC|SL-BL|SR-BR:channel_layout=5.0",
                _ => "",
            } : "";
            return $"[0:a:{idx}]loudnorm={target}:" +
                   $"measured_I={s.InputI}:measured_TP={s.InputTp}:measured_LRA={s.InputLra}:" +
                   $"measured_thresh={s.InputThresh}:offset={s.TargetOffset}:linear=true:print_format=summary," +
                   $"aresample={rate}{channelMap}[anorm{idx}]";
        });

        var args = new List<string>
        {
            "-loglevel", "info",
            "-y",
            "-i", job.File,
            "-filter_complex", string.Join(";", filterParts),
            "-map_metadata", "0",
            "-map_chapters", "0",
        };

        args.AddRange(o.Video.IsReencode ? new[] { "-map", "0:v:0?" } : new[] { "-map", "0:v?" });

        foreach (var inIdx in job.OutputOrder)
        {
            args.Add("-map");
            args.Add(job.Stats.ContainsKey(inIdx) ? $"[anorm{inIdx}]" : $"0:a:{inIdx}?");
        }

        args.AddRange(new[] { "-map", "0:s?", "-map", "0:d?", "-map", "0:t?" });
        args.AddRange(o.Video.Args);
        // Nicht bei VAAPI: hevc_vaapi (z. B. Mesa/radeonsi) schreibt im Stream andere SPS/PPS als in
        // den globalen Header. Bei "hvc1" verwirft der MP4-Muxer die In-Band-Parametersaetze, der
        // Decoder nutzt dann die falschen aus hvcC -> gruenes Bild/Datenmuell. "hev1" behaelt sie.
        if (o.Video.IsHevc && o.Video.Mode != VideoModes.Vaapi && FileNaming.IsMp4Family(job.EncodeOutput))
            args.AddRange(new[] { "-tag:v:0", "hvc1" });

        for (var outIdx = 0; outIdx < job.OutputOrder.Count; outIdx++)
        {
            var inIdx = job.OutputOrder[outIdx];
            if (job.Stats.ContainsKey(inIdx))
            {
                args.AddRange(new[] { $"-c:a:{outIdx}", o.AudioCodec });
                if (o.AudioCodec != "pcm_s24le") args.AddRange(new[] { $"-b:a:{outIdx}", o.AudioBitrate });
            }
            else
            {
                args.AddRange(new[] { $"-c:a:{outIdx}", "copy" });
            }
        }

        for (var outIdx = 0; outIdx < job.OutputOrder.Count; outIdx++)
        {
            var info = job.AudioStreams[job.OutputOrder[outIdx]];
            if (!string.IsNullOrWhiteSpace(info.Language))
                args.AddRange(new[] { $"-metadata:s:a:{outIdx}", $"language={info.Language.Trim()}" });
            if (!string.IsNullOrWhiteSpace(info.Title))
                args.AddRange(new[] { $"-metadata:s:a:{outIdx}", $"title={info.Title.Trim()}" });

            var disp = new List<string>();
            if (job.DefaultOutputIndex == outIdx) disp.Add("default");
            if (info.IsForced) disp.Add("forced");
            args.AddRange(new[] { $"-disposition:a:{outIdx}", disp.Count > 0 ? string.Join("+", disp) : "0" });
        }

        args.AddRange(new[] { "-c:s", "copy", "-c:d", "copy", "-c:t", "copy", job.EncodeOutput });
        return args;
    }
}
