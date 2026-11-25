using System.Globalization;
using CommunityToolkit.Mvvm.ComponentModel;
using LoudnormPro.App.Localization;
using LoudnormPro.Core.Media;
using LoudnormPro.Core.Processing;

namespace LoudnormPro.App.ViewModels;

/// <summary>Eine Datei in der Quellen-Liste.</summary>
public sealed partial class FileItemViewModel : ObservableObject
{
    public FileItemViewModel(string path, string root)
    {
        Path = path;
        Root = root;
        Name = System.IO.Path.GetFileName(path);
        Meta = Loc.Instance["file.meta_loading"];
    }

    public string Path { get; }
    public string Root { get; }
    public string Name { get; }

    [ObservableProperty] private string _meta;
    [ObservableProperty] private FileState _state = FileState.Queued;
    [ObservableProperty] private string _stateText = string.Empty;
    [ObservableProperty] private string _resultText = string.Empty;
    [ObservableProperty] private string _toolTipText = string.Empty;

    public MediaInfo? Info { get; set; }
    public bool InfoLoaded { get; set; }
    public FileResult? Result { get; set; }

    public bool IsRunning => State is FileState.Analyzing or FileState.Encoding;
    public bool IsDone => State is FileState.Done or FileState.Analyzed;
    public bool IsSkipped => State is FileState.Skipped or FileState.Cancelled;
    public bool IsFailed => State == FileState.Failed;
    public bool HasResult => ResultText.Length > 0;

    partial void OnStateChanged(FileState value)
    {
        RefreshTexts();
        OnPropertyChanged(nameof(IsRunning));
        OnPropertyChanged(nameof(IsDone));
        OnPropertyChanged(nameof(IsSkipped));
        OnPropertyChanged(nameof(IsFailed));
    }

    partial void OnResultTextChanged(string value) => OnPropertyChanged(nameof(HasResult));

    public void RefreshTexts()
    {
        var l = Loc.Instance;
        StateText = l["filestate." + State.ToString().ToLowerInvariant()];
        if (Info != null)
        {
            var langs = Info.AudioStreams.Select(a => string.IsNullOrWhiteSpace(a.Language) ? "und" : a.Language.ToUpperInvariant())
                .Distinct().Take(5);
            Meta = l.F("file.meta", ("duration", FormatDuration(Info.DurationSeconds)), ("tracks", Info.AudioStreams.Count), ("langs", string.Join("/", langs)));
        }
        else if (InfoLoaded)
        {
            Meta = l["file.meta_unknown"];
        }
        else
        {
            Meta = l["file.meta_loading"];
        }

        var r = Result;
        if (r?.MeasuredBefore is { } before && r.MeasuredAfter is { } after)
            ResultText = l.F("file.result", ("before", before.ToString("0.0", CultureInfo.InvariantCulture)), ("after", after.ToString("0.0", CultureInfo.InvariantCulture)));
        else if (r?.MeasuredBefore is { } b2)
            ResultText = l.F("file.measured", ("before", b2.ToString("0.0", CultureInfo.InvariantCulture)));
        else
            ResultText = string.Empty;

        ToolTipText = r != null && JobStatus.IsError(r.Status) ? $"{r.Status}\n{r.Details}" : Path;
    }

    public static string FormatDuration(double seconds)
    {
        var t = TimeSpan.FromSeconds(Math.Max(0, seconds));
        return t.TotalHours >= 1 ? $"{(int)t.TotalHours}:{t.Minutes:00}:{t.Seconds:00}" : $"{t.Minutes}:{t.Seconds:00}";
    }
}

/// <summary>Ein laufender Job (Analyse oder Encode) in der Fortschrittsanzeige.</summary>
public sealed partial class ActiveJobViewModel : ObservableObject
{
    public ActiveJobViewModel(Guid id, int number, string name, string phase)
    {
        Id = id;
        Number = number;
        Name = name;
        _phase = phase;
        Started = DateTime.UtcNow;
    }

    public Guid Id { get; }
    public string Name { get; }
    public DateTime Started { get; private set; }
    private DateTime _phaseStarted = DateTime.UtcNow;

    [ObservableProperty] private int _number;
    [ObservableProperty] private string _phase;
    [ObservableProperty] private int _percent;
    [ObservableProperty] private string _etaText = "ETA --";

    public string NumberText => $"Job {Number}";
    public string PercentText => $"{Percent} %";

    partial void OnNumberChanged(int value) => OnPropertyChanged(nameof(NumberText));

    partial void OnPhaseChanged(string? oldValue, string newValue)
    {
        if (oldValue != newValue) _phaseStarted = DateTime.UtcNow;
    }

    partial void OnPercentChanged(int value)
    {
        OnPropertyChanged(nameof(PercentText));
        var elapsed = (DateTime.UtcNow - _phaseStarted).TotalSeconds;
        if (value is > 0 and < 100 && elapsed > 2)
        {
            var remaining = elapsed / value * (100 - value);
            EtaText = "ETA " + FormatShort(remaining);
        }
        else if (value >= 100)
        {
            EtaText = "ETA 0s";
        }
    }

    public static string FormatShort(double seconds)
    {
        var s = (int)Math.Max(0, seconds);
        if (s < 60) return $"{s}s";
        var m = s / 60;
        if (m < 60) return $"{m}m {s % 60:00}s";
        return $"{m / 60}h {m % 60:00}m";
    }
}

/// <summary>Eine Log-Zeile.</summary>
public sealed class LogEntryViewModel
{
    public LogEntryViewModel(LogLevel level, string text)
    {
        Level = level;
        Text = text;
        Time = DateTime.Now.ToString("HH:mm:ss", CultureInfo.InvariantCulture);
    }

    public LogLevel Level { get; }
    public string Text { get; }
    public string Time { get; }

    public bool IsMuted => Level == LogLevel.Muted;
    public bool IsAccent => Level == LogLevel.Accent;
    public bool IsSuccess => Level == LogLevel.Success;
    public bool IsWarning => Level == LogLevel.Warning;
    public bool IsError => Level == LogLevel.Error;

    public override string ToString() => $"{Time}  {Text}";
}

/// <summary>Eine Zeile der Audiospur-Vorschau.</summary>
public sealed class AudioTrackRowViewModel
{
    public required string Out { get; init; }
    public required string Language { get; init; }
    public required string Title { get; init; }
    public required string Format { get; init; }
    public required string Flags { get; init; }
    public required string Action { get; init; }
    public bool Normalize { get; init; }
    public bool IsNewDefault { get; init; }
}

/// <summary>Kachel "Aktive Einstellungen".</summary>
public sealed class ChipViewModel
{
    public required string Text { get; init; }
    public string? Page { get; init; }
    public required System.Windows.Input.ICommand Command { get; init; }
    public string TooltipKey { get; init; } = "tooltip.chip";
    public string Tooltip => Loc.Instance[TooltipKey];
    public bool IsWarning { get; init; }
    public bool IsNormalizeMode { get; init; }
}

/// <summary>Encoder-Anzeige in der Menueleiste und im Einstellungsdialog.</summary>
public sealed class EncoderChipViewModel
{
    public required string Name { get; init; }
    public bool Available { get; init; }
    public string ToolTip => Available ? Loc.Instance["tooltip.encoder_ok"] : Loc.Instance["tooltip.encoder_missing"];
}
