using System.ComponentModel;
using Avalonia.Data;
using Avalonia.Markup.Xaml;
using LoudnormPro.Core.Localization;

namespace LoudnormPro.App.Localization;

/// <summary>
/// Bindbare Fassade fuer die Sprachdateien. In XAML: Text="{l:T menu.file}".
/// Bei einem Sprachwechsel werden alle Bindungen automatisch aktualisiert.
/// </summary>
public sealed class Loc : INotifyPropertyChanged
{
    public static Loc Instance { get; } = new();

    private LanguageCatalog _catalog = LanguageCatalog.Current;

    public event PropertyChangedEventHandler? PropertyChanged;

    /// <summary>Wird nach jedem Sprachwechsel (und Neuladen) ausgeloest.</summary>
    public event EventHandler? Changed;

    public LanguageCatalog Catalog => _catalog;

    public string this[string key] => _catalog.Get(key);

    public string F(string key, params (string Name, object? Value)[] args) => _catalog.Format(key, args);

    public void Attach(LanguageCatalog catalog)
    {
        _catalog.LanguageChanged -= OnLanguageChanged;
        _catalog = catalog;
        _catalog.LanguageChanged += OnLanguageChanged;
        Raise();
    }

    private void OnLanguageChanged(object? sender, EventArgs e) => Raise();

    public void Raise()
    {
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs("Item"));
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs("Item[]"));
        Changed?.Invoke(this, EventArgs.Empty);
    }
}

/// <summary>Markup-Erweiterung: {l:T schluessel}</summary>
public sealed class TExtension : MarkupExtension
{
    public TExtension()
    {
    }

    public TExtension(string key) => Key = key;

    public string Key { get; set; } = string.Empty;

    public override object ProvideValue(IServiceProvider serviceProvider) =>
        new Binding
        {
            Source = Loc.Instance,
            Path = $"[{Key}]",
            Mode = BindingMode.OneWay,
        };
}
