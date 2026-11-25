using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.Primitives;
using Avalonia.Layout;
using Avalonia.Media;
using LoudnormPro.App.Localization;
using LoudnormPro.Core.Media;
using LoudnormPro.Core.Settings;

namespace LoudnormPro.App.Views;

public sealed class GuideWindow : Window
{
    private readonly Loc _l = Loc.Instance;
    private readonly StackPanel _article = new() { Spacing = 14 };
    private readonly ScrollViewer _scroll = new();
    private readonly ComboBox _chapters = new() { HorizontalAlignment = HorizontalAlignment.Stretch };
    private readonly (string Title, string Body)[] _topics =
    {
        ("help.start", "help.workflow"), ("settings.norm.title", "help.targets"),
        ("settings.norm.track_mode", "help.tracks"), ("settings.norm.analyze_only", "help.cache"),
        ("settings.audio.title", "help.codecs"), ("video.format", "help.containers"),
        ("settings.video.title", "help.video"), ("settings.jobs.title", "help.jobs"),
        ("settings.tools.title", "help.paths"), ("menu.settings.profiles", "help.profiles"),
        ("menu.info.diagnostics", "help.diagnostics")
    };

    public GuideWindow()
    {
        Title = $"Loudnorm PRO · {_l["help.guide"]}";
        Width = 650;
        Height = 510;
        MinWidth = 340;
        MinHeight = 280;
        WindowStartupLocation = WindowStartupLocation.CenterOwner;
        var layout = new DockPanel { Margin = new Thickness(18) };
        var search = new TextBox { PlaceholderText = _l["help.search"] };
        var navigation = new StackPanel { Spacing = 8, Margin = new Thickness(0, 0, 0, 14) };
        navigation.Children.Add(search);
        navigation.Children.Add(_chapters);
        DockPanel.SetDock(navigation, Dock.Top);
        layout.Children.Add(navigation);
        _scroll.Content = _article;
        _scroll.HorizontalScrollBarVisibility = ScrollBarVisibility.Disabled;
        layout.Children.Add(_scroll);
        Content = layout;
        _chapters.SelectionChanged += (_, _) => ShowChapter();
        void Filter()
        {
            var query = search.Text?.Trim() ?? "";
            var matches = _topics.Where(t => (_l[t.Title] + " " + _l[t.Body])
                .Contains(query, StringComparison.CurrentCultureIgnoreCase)).Select(t => _l[t.Title]).ToList();
            _chapters.ItemsSource = matches;
            _chapters.SelectedIndex = matches.Count > 0 ? 0 : -1;
            ShowChapter();
        }
        search.TextChanged += (_, _) => Filter();
        Filter();
    }

    private static TextBlock Paragraph(string text) => new() { Text = text, TextWrapping = TextWrapping.Wrap };

    private void ShowChapter()
    {
        _article.Children.Clear();
        _scroll.Offset = default;
        if (_chapters.SelectedItem is not string title)
        {
            _article.Children.Add(Paragraph(_l["help.empty"]));
            return;
        }
        var topic = _topics.First(t => _l[t.Title] == title);
        var heading = Paragraph(title);
        heading.FontSize = 20;
        heading.FontWeight = FontWeight.SemiBold;
        _article.Children.Add(heading);
        foreach (var paragraph in _l[topic.Body].Split('\n', StringSplitOptions.RemoveEmptyEntries))
            _article.Children.Add(Paragraph(paragraph));
        if (topic.Body != "help.containers") return;
        var format = new ComboBox { ItemsSource = FileNaming.VideoExtensions.Order().ToArray() };
        var result = Paragraph("");
        format.SelectionChanged += (_, _) =>
        {
            if (format.SelectedItem is string ext)
                result.Text = _l["settings.audio.codec"] + " · " + string.Join(", ", AppSettings.AudioCodecs
                    .Where(c => FileNaming.SupportsAudio(ext, AppSettings.EncoderFor(c))));
        };
        _article.Children.Add(Paragraph(_l["help.check"]));
        _article.Children.Add(format);
        _article.Children.Add(result);
        format.SelectedItem = ".mkv";
    }
}
