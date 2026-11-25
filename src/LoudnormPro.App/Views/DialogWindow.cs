using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.Documents;
using Avalonia.Layout;
using Avalonia.Media;
using LoudnormPro.App.Localization;

namespace LoudnormPro.App.Views;

public enum DialogKind
{
    Info,
    Warning,
    Error,
    Question,
}

/// <summary>Einheitliche Dialoge (Meldung, Bestaetigung, Eingabe, Info-Tabelle) im Loudnorm-PRO-Look.</summary>
public sealed class DialogWindow : Window
{
    private readonly StackPanel _body = new() { Spacing = 10 };
    private readonly StackPanel _buttons = new() { Orientation = Orientation.Horizontal, Spacing = 8, HorizontalAlignment = HorizontalAlignment.Right };
    private readonly StackPanel _leftButtons = new() { Orientation = Orientation.Horizontal, Spacing = 8 };
    private object? _result;

    public DialogWindow(string title, double width = 520)
    {
        Title = title;
        Width = width;
        SizeToContent = SizeToContent.Height;
        CanResize = false;
        WindowStartupLocation = WindowStartupLocation.CenterOwner;
        ShowInTaskbar = false;
        MaxHeight = 720;

        var header = new Grid { ColumnDefinitions = new ColumnDefinitions("Auto,*"), Margin = new Thickness(18, 16, 18, 0) };
        var logo = new Border
        {
            Width = 24,
            Height = 24,
            CornerRadius = new CornerRadius(6),
            Background = this.FindResource("LnGradient") as IBrush,
            Margin = new Thickness(0, 0, 10, 0),
            Child = new Avalonia.Controls.Shapes.Path
            {
                Data = this.FindResource("IconWave") as Geometry,
                Stroke = Brushes.White,
                StrokeThickness = 2.4,
                StrokeLineCap = PenLineCap.Round,
                Width = 14,
                Height = 14,
                Stretch = Stretch.Uniform,
            },
        };
        header.Children.Add(logo);
        var titleText = new TextBlock { Text = title, Classes = { "h3" }, VerticalAlignment = VerticalAlignment.Center };
        Grid.SetColumn(titleText, 1);
        header.Children.Add(titleText);

        var footerGrid = new Grid { ColumnDefinitions = new ColumnDefinitions("*,Auto") };
        footerGrid.Children.Add(_leftButtons);
        Grid.SetColumn(_buttons, 1);
        footerGrid.Children.Add(_buttons);

        var footer = new Border
        {
            Padding = new Thickness(16, 12),
            BorderThickness = new Thickness(0, 1, 0, 0),
            Child = footerGrid,
        };
        footer.Bind(Border.BackgroundProperty, footer.GetResourceObservable("LnBg"));
        footer.Bind(Border.BorderBrushProperty, footer.GetResourceObservable("LnBorder"));

        var root = new DockPanel();
        DockPanel.SetDock(header, Dock.Top);
        DockPanel.SetDock(footer, Dock.Bottom);
        root.Children.Add(header);
        root.Children.Add(footer);
        root.Children.Add(new ScrollViewer
        {
            Content = new Border { Padding = new Thickness(18, 14, 18, 18), Child = _body },
            HorizontalScrollBarVisibility = Avalonia.Controls.Primitives.ScrollBarVisibility.Disabled,
        });

        Content = root;
        this.Bind(BackgroundProperty, this.GetResourceObservable("LnSurface"));

        KeyDown += (_, e) =>
        {
            if (e.Key == Avalonia.Input.Key.Escape) Close(_result);
        };
    }

    public StackPanel Body => _body;

    public DialogWindow AddText(string text, string? cssClass = null, bool selectable = false)
    {
        Control c = selectable
            ? new SelectableTextBlock { Text = text, TextWrapping = TextWrapping.Wrap }
            : new TextBlock { Text = text, TextWrapping = TextWrapping.Wrap };
        if (cssClass != null) c.Classes.Add(cssClass);
        _body.Children.Add(c);
        return this;
    }

    public DialogWindow AddControl(Control c)
    {
        _body.Children.Add(c);
        return this;
    }

    /// <summary>Tabelle mit Schluessel/Wert-Zeilen.</summary>
    public DialogWindow AddKeyValues(IEnumerable<(string Key, string Value)> rows)
    {
        _body.Children.Add(BuildKeyValues(rows));
        return this;
    }

    /// <summary>Tabelle mit Schluessel/Wert-Zeilen.</summary>
    public static Grid BuildKeyValues(IEnumerable<(string Key, string Value)> rows)
    {
        var grid = new Grid { ColumnDefinitions = new ColumnDefinitions("170,*") };
        var r = 0;
        foreach (var (k, v) in rows)
        {
            grid.RowDefinitions.Add(new RowDefinition(GridLength.Auto));
            var key = new TextBlock { Text = k, Classes = { "muted" }, Margin = new Thickness(0, 4, 12, 4), FontSize = 12.5 };
            var val = new SelectableTextBlock { Text = v, TextWrapping = TextWrapping.Wrap, Margin = new Thickness(0, 4), Classes = { "mono" } };
            val.Bind(TextBlock.ForegroundProperty, val.GetResourceObservable("LnHeading"));
            Grid.SetRow(key, r);
            Grid.SetRow(val, r);
            Grid.SetColumn(val, 1);
            grid.Children.Add(key);
            grid.Children.Add(val);
            r++;
        }
        return grid;
    }

    public DialogWindow AddButton(string text, object? result, bool primary = false, bool isDefault = false, bool isCancel = false)
    {
        var b = new Button { Content = text, MinWidth = 90, HorizontalContentAlignment = HorizontalAlignment.Center, IsDefault = isDefault, IsCancel = isCancel };
        if (primary) b.Classes.Add("primary");
        b.Click += (_, _) => Close(result);
        _buttons.Children.Add(b);
        return this;
    }

    public DialogWindow AddLeftButton(string text, Action onClick, string? cssClass = "ghost")
    {
        var b = new Button { Content = text };
        if (cssClass != null) b.Classes.Add(cssClass);
        b.Click += (_, _) => onClick();
        _leftButtons.Children.Add(b);
        return this;
    }

    public DialogWindow WithCancelResult(object? result)
    {
        _result = result;
        return this;
    }

    // ---------- Fertige Dialoge ----------

    public static async Task ShowMessageAsync(Window owner, string title, string message, DialogKind kind = DialogKind.Info)
    {
        var d = new DialogWindow(title);
        var cls = kind switch { DialogKind.Error => "err", DialogKind.Warning => "warn", _ => null };
        d.AddText(message, cls, selectable: true);
        d.AddButton(Loc.Instance["common.ok"], null, primary: true, isDefault: true, isCancel: true);
        await d.ShowDialog(owner);
    }

    public static async Task<bool> ConfirmAsync(Window owner, string title, string message, string? yes = null, string? no = null)
    {
        var d = new DialogWindow(title);
        d.AddText(message);
        d.AddButton(no ?? Loc.Instance["common.no"], false, isCancel: true);
        d.AddButton(yes ?? Loc.Instance["common.yes"], true, primary: true, isDefault: true);
        d.WithCancelResult(false);
        var r = await d.ShowDialog<object?>(owner);
        return r is true;
    }

    public static async Task<string?> PromptAsync(Window owner, string title, string prompt, string initial = "")
    {
        var d = new DialogWindow(title, 440);
        d.AddText(prompt);
        var tb = new TextBox { Text = initial };
        d.AddControl(tb);
        string? result = null;
        var ok = new Button { Content = Loc.Instance["common.ok"], MinWidth = 90, IsDefault = true, HorizontalContentAlignment = HorizontalAlignment.Center, Classes = { "primary" } };
        ok.Click += (_, _) =>
        {
            result = tb.Text?.Trim();
            d.Close();
        };
        d.AddButton(Loc.Instance["common.cancel"], null, isCancel: true);
        d._buttons.Children.Add(ok);
        d.Opened += (_, _) => tb.Focus();
        await d.ShowDialog(owner);
        return string.IsNullOrWhiteSpace(result) ? null : result;
    }

    /// <summary>Text mit fett gesetzten Zeilen (fuer Changelog / Release-Notes).</summary>
    public static Control Markdownish(string text)
    {
        var panel = new StackPanel { Spacing = 2 };
        foreach (var raw in text.Replace("\r", "").Split('\n'))
        {
            var line = raw.TrimEnd();
            if (line.StartsWith("## ", StringComparison.Ordinal))
            {
                panel.Children.Add(new TextBlock { Text = line[3..], Classes = { "h3" }, Margin = new Thickness(0, 10, 0, 2) });
            }
            else if (line.StartsWith("### ", StringComparison.Ordinal))
            {
                var t = new TextBlock { Text = line[4..], FontWeight = FontWeight.SemiBold, Margin = new Thickness(0, 6, 0, 0) };
                t.Bind(TextBlock.ForegroundProperty, t.GetResourceObservable("LnAccent"));
                panel.Children.Add(t);
            }
            else if (line.StartsWith("# ", StringComparison.Ordinal))
            {
                continue;
            }
            else if (line.StartsWith("- ", StringComparison.Ordinal) || line.StartsWith("* ", StringComparison.Ordinal))
            {
                var tb = new TextBlock { TextWrapping = TextWrapping.Wrap, Margin = new Thickness(8, 0, 0, 0) };
                tb.Inlines = new InlineCollection { new Run("•  "), new Run(line[2..].Replace("**", "").Replace("`", "")) };
                panel.Children.Add(tb);
            }
            else if (line.Length > 0)
            {
                panel.Children.Add(new TextBlock { Text = line.Replace("**", "").Replace("`", ""), TextWrapping = TextWrapping.Wrap });
            }
        }
        return panel;
    }
}
