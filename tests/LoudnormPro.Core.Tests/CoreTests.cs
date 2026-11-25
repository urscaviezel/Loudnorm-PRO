using LoudnormPro.Core.Localization;
using LoudnormPro.Core.Media;
using LoudnormPro.Core.Processing;
using LoudnormPro.Core.Services;
using LoudnormPro.Core.Settings;

namespace LoudnormPro.Core.Tests;

public class FileNamingTests
{
    [Theory]
    [InlineData("Film_loudnorm", "Film")]
    [InlineData("Film_loudnorm_nvenc", "Film")]
    [InlineData("Film_loudnorm_vt", "Film")]
    [InlineData("Film_LOUDNORM_X265", "Film")]
    [InlineData("Film", "Film")]
    public void CanonicalStem(string input, string expected) =>
        Assert.Equal(expected, FileNaming.CanonicalOutputStem(input));

    [Fact]
    public void WebmBecomesMkv() => Assert.Equal(".mkv", FileNaming.OutputExtensionFor(@"C:\a\clip.WEBM"));

    [Theory]
    [InlineData("a.m4v", true, ".mp4")]
    [InlineData("a.m4v", false, ".m4v")]
    [InlineData("a.avi", true, ".mkv")]
    [InlineData("a.mkv", true, ".mkv")]
    public void ContainerForHevc(string input, bool hevc, string expected) =>
        Assert.Equal(expected, FileNaming.OutputExtensionFor(input, hevc));

    [Theory]
    [InlineData("none", false, "Film.mkv")]
    [InlineData("none", true, "Film_loudnorm.mkv")]
    [InlineData("custom", true, "Film_final.mkv")]
    [InlineData("custom", false, "Film_final.mkv")]
    [InlineData("default", false, "Film_loudnorm.mkv")]
    public void NamingModes(string mode, bool sameFolder, string expected)
    {
        var root = Path.GetFullPath("inputs");
        var settings = new AppSettings { TagMode = mode, CustomTag = "_final" };
        var options = ProcessingOptions.FromSettings(settings, "", "", EncoderCapabilities.None, sameFolder ? root : Path.GetFullPath("outputs"));
        var plan = OutputPlanner.Create(new SourceFile(Path.Combine(root, "Film.mkv"), root), options);
        Assert.Equal(expected, Path.GetFileName(plan.FinalOutput));
        Assert.NotEqual(plan.FinalOutput, plan.EncodeOutput);
    }

    [Theory]
    [InlineData("clip.webm", "libopus", false, ".webm")]
    [InlineData("clip.webm", "libopus", true, ".mkv")]
    [InlineData("clip.mov", "libopus", false, ".mkv")]
    [InlineData("clip.ts", "pcm_s24le", false, ".mkv")]
    [InlineData("clip.mov", "pcm_s24le", false, ".mov")]
    public void AutoSelectsCompatibleAudioContainer(string source, string codec, bool encode, string expected) =>
        Assert.Equal(expected, FileNaming.OutputExtensionFor(source, encode, "auto", codec));

    [Fact]
    public void Mp4StaysMp4() => Assert.Equal(".mp4", FileNaming.OutputExtensionFor("/x/clip.mp4"));

    [Theory]
    [InlineData("a/b/Film_loudnorm.mkv", true)]
    [InlineData("a/b/Film_loudnorm_amf.mkv", true)]
    [InlineData("a/b/Film.mkv", false)]
    public void DetectsOutputs(string path, bool expected) => Assert.Equal(expected, FileNaming.IsLoudnormOutput(path));

    [Fact]
    public void NormalizeName() => Assert.Equal("bandaliveinzurich2025", FileNaming.NormalizeName("Band - Alive in Zurich (2025)"));
}

public class LoudnormStatsTests
{
    private const string Pass1 = """
        [Parsed_loudnorm_0 @ 000001]
        {
        	"input_i" : "-27.61",
        	"input_tp" : "-4.47",
        	"input_lra" : "18.06",
        	"input_thresh" : "-39.20",
        	"output_i" : "-16.58",
        	"output_tp" : "-1.50",
        	"output_lra" : "14.78",
        	"output_thresh" : "-27.71",
        	"normalization_type" : "dynamic",
        	"target_offset" : "0.58"
        }
        """;

    [Fact]
    public void ParsesJson()
    {
        var s = LoudnormStats.FromFfmpegOutput("Input #0, title {weird}\n" + Pass1);
        Assert.NotNull(s);
        Assert.Equal("-27.61", s!.InputI);
        Assert.Equal("0.58", s.TargetOffset);
        Assert.Equal(-27.61, s.InputIntegrated!.Value, 2);
    }

    [Fact]
    public void ReturnsNullOnGarbage() => Assert.Null(LoudnormStats.FromFfmpegOutput("nothing here"));

    [Fact]
    public void ParsesSummary()
    {
        const string text = "Output Integrated:   -16.0 LUFS\nfoo\nOutput Integrated:   -16.2 LUFS";
        Assert.Equal(-16.1, LoudnormSummary.ParseOutputIntegrated(text));
    }
}

public class AudioTrackSelectorTests
{
    private static List<AudioStreamInfo> Streams() => new()
    {
        new() { Position = 0, Language = "eng", Title = "English", IsDefault = true },
        new() { Position = 1, Language = "ger", Title = "Deutsch" },
        new() { Position = 2, Language = "fre", Title = "Français" },
        new() { Position = 3, Language = "", Title = "Kommentar Deutsch" },
    };

    [Fact]
    public void AutoPicksFirstPreferred()
    {
        var s = new AudioTrackSelector(AudioTrackMode.Auto, "de", true);
        Assert.Equal(new[] { 1 }, s.SelectedIndices(Streams()));
        Assert.Equal(new[] { 0, 1, 2, 3 }, s.OutputOrder(Streams()));
        Assert.Equal(1, s.DefaultOutputIndex(Streams(), s.OutputOrder(Streams())));
    }

    [Fact]
    public void AutoFallsBackToFirst()
    {
        var s = new AudioTrackSelector(AudioTrackMode.Auto, "ja", false);
        Assert.Equal(new[] { 0 }, s.SelectedIndices(Streams()));
        Assert.Equal(0, s.DefaultOutputIndex(Streams(), s.OutputOrder(Streams())));
    }

    [Fact]
    public void PreferredOnlyMovesFirst()
    {
        var s = new AudioTrackSelector(AudioTrackMode.PreferredOnly, "de", true);
        Assert.Equal(new[] { 1, 3 }, s.SelectedIndices(Streams()));
        Assert.Equal(new[] { 1, 3, 0, 2 }, s.OutputOrder(Streams()));
        Assert.Equal(0, s.DefaultOutputIndex(Streams(), s.OutputOrder(Streams())));
    }

    [Fact]
    public void FrenchIsRecognized()
    {
        var s = new AudioTrackSelector(AudioTrackMode.PreferredOnly, "fr", false);
        Assert.Equal(new[] { 2 }, s.SelectedIndices(Streams()));
    }

    [Fact]
    public void AllSelectsAll()
    {
        var s = new AudioTrackSelector(AudioTrackMode.All, "de", false);
        Assert.Equal(4, s.SelectedIndices(Streams()).Count);
    }
}

public class VideoPlanTests
{
    private static readonly EncoderCapabilities AllCaps = new()
    {
        Nvenc = true, Amf = true, Vaapi = true, VaapiDevice = "/dev/dri/renderD128", VideoToolbox = true, X265 = true,
    };

    [Fact]
    public void NvencConstantQp()
    {
        var p = VideoPlanBuilder.Build(VideoModes.Nvenc, "p7 slow", true, 19, 12, AllCaps);
        Assert.Equal(new[] { "-c:v", "hevc_nvenc", "-preset", "p7", "-rc", "constqp", "-qp", "19" }, p.Args);
        Assert.Equal("_loudnorm_nvenc", p.Tag);
    }

    [Theory]
    [InlineData(VideoModes.Nvenc, "-b:v")]
    [InlineData(VideoModes.X265, "-b:v:0")]
    [InlineData(VideoModes.Amf, "-b:v")]
    [InlineData(VideoModes.Vaapi, "-b:v:0")]
    [InlineData(VideoModes.VideoToolbox, "-b:v")]
    public void CustomBitrateSurvivesProfileRoundTrip(string mode, string bitrateOption)
    {
        var s = new AppSettings { Video = mode, VideoRateMode = "bitrate", VideoBitrate = "custom",
            CustomVideoMbps = 8.75m, OutputFormat = "mkv", VideoPreset = "P1" };
        var restored = new AppSettings();
        ProcessingProfile.From(s, "custom").ApplyTo(restored);
        var options = ProcessingOptions.FromSettings(restored, "", "", AllCaps, "");
        Assert.Contains(bitrateOption + " 8.75M", string.Join(" ", options.Video.Args));
        Assert.Equal(".mkv", FileNaming.OutputExtensionFor("sample.mp4", true, options.OutputFormat));
        Assert.Equal("P1", restored.VideoPreset);
    }

    [Fact]
    public void NvencBitrate()
    {
        var p = VideoPlanBuilder.Build(VideoModes.Nvenc, "p5 balanced", false, 19, 12, AllCaps);
        Assert.Contains("24M", p.Args);
    }

    [Fact]
    public void UnavailableFallsBackToCopy()
    {
        var p = VideoPlanBuilder.Build(VideoModes.Nvenc, "p5 balanced", true, 19, 12, EncoderCapabilities.None);
        Assert.Equal(VideoModes.Copy, p.Mode);
        Assert.Equal(VideoModes.Nvenc, p.FallbackNotice);
    }

    [Fact]
    public void VideoToolbox()
    {
        var p = VideoPlanBuilder.Build(VideoModes.VideoToolbox, "p5 balanced", true, 19, 12, AllCaps);
        Assert.Contains("hevc_videotoolbox", p.Args);
        Assert.Equal("_loudnorm_vt", p.Tag);
    }
}

public class LoudnessTargetTests
{
    [Fact]
    public void FilterArgsUseInvariantCulture()
    {
        var prev = System.Globalization.CultureInfo.CurrentCulture;
        try
        {
            System.Globalization.CultureInfo.CurrentCulture = new System.Globalization.CultureInfo("de-CH");
            Assert.Equal("I=-16:TP=-1.5:LRA=11", LoudnessTarget.Default.ToFilterArgs());
        }
        finally
        {
            System.Globalization.CultureInfo.CurrentCulture = prev;
        }
    }

    [Fact]
    public void Clamps() => Assert.Equal("I=-5:TP=0:LRA=20", new LoudnessTarget(3, 5, 99).ToFilterArgs());

    [Fact]
    public void DetectsPreset() => Assert.Equal(LoudnessPresets.Ebu, LoudnessPresets.Detect(new LoudnessTarget(-23, -1, 7)));
}

public class Pass2ArgsTests
{
    [Fact]
    public void BuildsLikeV1()
    {
        var job = new FileJob
        {
            File = "in.mkv",
            DisplayName = "in.mkv",
            Output = "out.mkv",
            EncodeOutput = "out.mkv",
            AudioStreams = new List<AudioStreamInfo>
            {
                new() { Position = 0, Language = "eng", SampleRate = "48000" },
                new() { Position = 1, Language = "ger", Title = "Deutsch", SampleRate = "48000", IsForced = true },
            },
            SelectedIndices = new() { 1 },
            OutputOrder = new() { 0, 1 },
            DefaultOutputIndex = 1,
        };
        job.Stats[1] = new LoudnormStats("-27.61", "-4.47", "18.06", "-39.20", "0.58");

        var o = new ProcessingOptions
        {
            FfmpegPath = "ffmpeg",
            FfprobePath = "ffprobe",
            Target = LoudnessTarget.Default,
            AudioCodec = "aac",
            AudioBitrate = "384k",
            Tracks = new AudioTrackSelector(AudioTrackMode.Auto, "de", true),
            Video = VideoPlanBuilder.Build(VideoModes.Copy, "", false, 19, 12, EncoderCapabilities.None),
            OutputRoot = "out",
        };

        var a = LoudnormPipeline.BuildPass2Args(job, o);
        var joined = string.Join(" ", a);
        Assert.Contains("[0:a:1]loudnorm=I=-16:TP=-1.5:LRA=11:measured_I=-27.61:", joined);
        Assert.Contains("-map 0:v?", joined);
        Assert.Contains("-map 0:a:0? -map [anorm1]", joined);
        Assert.Contains("-c:a:0 copy", joined);
        Assert.Contains("-c:a:1 aac -b:a:1 384k", joined);
        Assert.DoesNotContain("-ar:", joined);
        Assert.Contains("print_format=summary,aresample=48000[anorm1]", joined);
        Assert.Contains("-disposition:a:0 0", joined);
        Assert.Contains("-disposition:a:1 default+forced", joined);
        Assert.Equal("out.mkv", a[^1]);
    }
}

public class CsvAndResumeTests
{
    [Fact]
    public void CsvRoundTrip()
    {
        var line = Csv.Line("a,b", "say \"hi\"", "x");
        var rows = Csv.Parse(line + "\n");
        Assert.Equal(new[] { "a,b", "say \"hi\"", "x" }, rows[0]);
    }

    [Fact]
    public void ResumeSkipsFinished()
    {
        var dir = Directory.CreateTempSubdirectory("lnp_");
        try
        {
            var src = Path.Combine(dir.FullName, "a.mkv");
            var outp = Path.Combine(dir.FullName, "a_loudnorm.mkv");
            File.WriteAllText(src, "x");
            File.WriteAllText(outp, "y");
            var store = new ResumeStore(Path.Combine(dir.FullName, "resume.csv"));
            store.Append(new FileResult { File = src, Output = outp, Status = JobStatus.Ok, DisplayName = "a.mkv" });

            var (remaining, skipped) = store.Filter(new[] { new SourceFile(src, dir.FullName), new SourceFile(Path.Combine(dir.FullName, "b.mkv"), dir.FullName) });
            Assert.Equal(0, store.Filter(new[] { new SourceFile(src, dir.FullName) }, _ => ".mp4").Skipped);
            Assert.Equal(1, skipped);
            Assert.Single(remaining);
        }
        finally
        {
            dir.Delete(true);
        }
    }
}

public class SettingsTests
{
    [Fact]
    public void MigratesV1File()
    {
        var dir = Directory.CreateTempSubdirectory("lnp_");
        try
        {
            var path = Path.Combine(dir.FullName, "s.json");
            File.WriteAllText(path, """
                { "source_mode": "files", "output": "D:/out", "audio": "E-AC3", "audio_bitrate": "640k",
                  "audio_track_mode": "preferred_only", "preferred_language": "en", "video": "HEVC NVENC",
                  "jobs": "4", "language": "English", "theme": "Hell", "temp_work_dir": "H:/tmp" }
                """);
            var s = SettingsStore.Load(path);
            Assert.Equal("en", s.UiLanguage);
            Assert.Equal("light", s.UiTheme);
            Assert.Equal("E-AC3", s.Audio);
            Assert.Equal("640k", s.AudioBitrate);
            Assert.Equal(AudioTrackMode.PreferredOnly, s.TrackMode);
            Assert.Equal(4, s.JobsManual);
            Assert.Equal("H:/tmp", s.TempWorkDir);

            SettingsStore.Save(s, path);
            var again = SettingsStore.Load(path);
            Assert.Equal("D:/out", again.Output);
        }
        finally
        {
            dir.Delete(true);
        }
    }
}

public class UpdateTests
{
    [Theory]
    [InlineData("v2.1.0", "2.0.0", true)]
    [InlineData("2.0.0", "2.0.0", false)]
    [InlineData("v1.0.4", "2.0.0", false)]
    [InlineData("2.0.10", "2.0.9", true)]
    public void Compares(string a, string b, bool newer) => Assert.Equal(newer, UpdateChecker.IsNewer(a, b));
}

public class LanguageTests
{
    private static string LangDir => Path.Combine(AppContext.BaseDirectory, "Languages");

    [Fact]
    public void BundledLanguagesLoad()
    {
        var c = new LanguageCatalog();
        c.LoadFolder(LangDir, "test");
        Assert.Empty(c.LoadErrors);
        Assert.True(c.HasLanguage("de"));
        Assert.True(c.HasLanguage("en"));
        Assert.True(c.HasLanguage("fr"));
    }

    [Theory]
    [InlineData("de")]
    [InlineData("fr")]
    public void LanguagesAreComplete(string code)
    {
        var c = new LanguageCatalog();
        c.LoadFolder(LangDir, "test");
        Assert.Empty(c.MissingKeys(code));
    }

    [Fact]
    public void FallsBackToEnglishThenKey()
    {
        var c = new LanguageCatalog();
        c.LoadJson("""{ "meta": { "code": "en", "name": "English" }, "strings": { "a": "A", "b": "B {n}" } }""", "t", "en");
        c.LoadJson("""{ "meta": { "code": "xx", "name": "X" }, "strings": { "a": "Xa" } }""", "t", "xx");
        c.SetLanguage("xx");
        Assert.Equal("Xa", c.Get("a"));
        Assert.Equal("B 5", c.Format("b", ("n", 5)));
        Assert.Equal("missing.key", c.Get("missing.key"));
    }
}
