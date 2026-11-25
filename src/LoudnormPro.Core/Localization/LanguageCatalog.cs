using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using LoudnormPro.Core.Platform;

namespace LoudnormPro.Core.Localization;

/// <summary>Kopfdaten einer Sprachdatei.</summary>
public sealed record LanguageInfo(string Code, string Name, string EnglishName, string Author, string Version, string Source);

/// <summary>
/// Laedt Sprachdateien (*.json) aus zwei Ordnern:
///   1. Mitgeliefert: &lt;Programmordner&gt;/Languages
///   2. Benutzer:      &lt;Datenordner&gt;/languages  (ueberschreibt einzelne Texte oder fuegt neue Sprachen hinzu)
/// Fehlende Texte fallen auf Englisch zurueck, danach auf den Schluessel selbst.
///
/// Format:
/// {
///   "meta":    { "code": "fr", "name": "Français", "englishName": "French", "author": "...", "version": "2.0.0" },
///   "strings": { "menu.file": "Fichier", "files.added": "{count} fichier(s) ajouté(s)." }
/// }
/// </summary>
public sealed partial class LanguageCatalog
{
    public const string FallbackCode = "en";

    private readonly Dictionary<string, Dictionary<string, string>> _strings = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, LanguageInfo> _infos = new(StringComparer.OrdinalIgnoreCase);
    private readonly List<string> _loadErrors = new();

    public static LanguageCatalog Current { get; private set; } = new();

    public IReadOnlyCollection<LanguageInfo> Languages => _infos.Values.OrderBy(l => l.Name, StringComparer.CurrentCultureIgnoreCase).ToList();
    public IReadOnlyList<string> LoadErrors => _loadErrors;
    public string CurrentCode { get; private set; } = FallbackCode;

    public event EventHandler? LanguageChanged;

    /// <summary>
    /// Ladereihenfolge: 1. in die EXE eingebettete Sprachen (immer vorhanden, auch als Einzeldatei),
    /// 2. Ordner "Languages" neben der EXE, 3. Benutzerordner. Spaetere Quellen ergaenzen/ueberschreiben.
    /// </summary>
    public static LanguageCatalog LoadDefault(System.Reflection.Assembly? embeddedSource = null)
    {
        var catalog = new LanguageCatalog();
        if (embeddedSource != null) catalog.LoadEmbedded(embeddedSource);
        catalog.LoadFolder(AppPaths.BuiltInLanguagesDir, "built-in");
        catalog.LoadFolder(AppPaths.UserLanguagesDir, "user");
        Current = catalog;
        return catalog;
    }

    /// <summary>Laedt alle eingebetteten Ressourcen "Languages.*.json".</summary>
    public void LoadEmbedded(System.Reflection.Assembly asm)
    {
        foreach (var name in asm.GetManifestResourceNames().Where(n => n.StartsWith("Languages.", StringComparison.Ordinal) && n.EndsWith(".json", StringComparison.Ordinal)))
        {
            try
            {
                using var stream = asm.GetManifestResourceStream(name);
                if (stream == null) continue;
                using var reader = new StreamReader(stream);
                LoadJson(reader.ReadToEnd(), "embedded", name["Languages.".Length..^".json".Length]);
            }
            catch (Exception ex) when (ex is JsonException or IOException or InvalidDataException)
            {
                _loadErrors.Add($"{name}: {ex.Message}");
            }
        }
    }

    public void LoadFolder(string folder, string source)
    {
        if (!Directory.Exists(folder)) return;
        foreach (var file in Directory.EnumerateFiles(folder, "*.json").OrderBy(f => f, StringComparer.OrdinalIgnoreCase))
        {
            try
            {
                LoadJson(File.ReadAllText(file), source, Path.GetFileNameWithoutExtension(file));
            }
            catch (Exception ex) when (ex is JsonException or IOException or UnauthorizedAccessException or InvalidDataException)
            {
                _loadErrors.Add($"{Path.GetFileName(file)}: {ex.Message}");
            }
        }
    }

    /// <summary>Laedt eine Sprachdatei aus einem JSON-String (spaetere Dateien ergaenzen/ueberschreiben).</summary>
    public void LoadJson(string json, string source, string fallbackCode)
    {
        using var doc = JsonDocument.Parse(json, new JsonDocumentOptions { CommentHandling = JsonCommentHandling.Skip, AllowTrailingCommas = true });
        var root = doc.RootElement;

        string code = fallbackCode, name = fallbackCode, english = fallbackCode, author = "", version = "";
        if (root.TryGetProperty("meta", out var meta) && meta.ValueKind == JsonValueKind.Object)
        {
            code = Str(meta, "code", fallbackCode);
            name = Str(meta, "name", code);
            english = Str(meta, "englishName", name);
            author = Str(meta, "author", "");
            version = Str(meta, "version", "");
        }

        code = code.Trim().ToLowerInvariant();
        if (code.Length == 0) throw new InvalidDataException("meta.code fehlt");

        if (!root.TryGetProperty("strings", out var strings) || strings.ValueKind != JsonValueKind.Object)
            throw new InvalidDataException("\"strings\" fehlt");

        if (!_strings.TryGetValue(code, out var dict))
        {
            dict = new Dictionary<string, string>(StringComparer.Ordinal);
            _strings[code] = dict;
        }

        foreach (var p in strings.EnumerateObject())
        {
            if (p.Value.ValueKind == JsonValueKind.String)
                dict[p.Name] = p.Value.GetString() ?? string.Empty;
        }

        // Benutzerdatei darf Name/Autor ueberschreiben, ohne sie zu verlieren, wenn sie fehlen
        if (_infos.TryGetValue(code, out var existing) && !root.TryGetProperty("meta", out _))
            return;
        _infos[code] = new LanguageInfo(code, name, english, author, version, source);
    }

    public bool HasLanguage(string code) => _strings.ContainsKey(code);

    /// <summary>Waehlt die Sprache; leer = Systemsprache (falls vorhanden), sonst Englisch.</summary>
    public string SetLanguage(string? code)
    {
        var resolved = ResolveCode(code);
        if (!string.Equals(resolved, CurrentCode, StringComparison.OrdinalIgnoreCase))
        {
            CurrentCode = resolved;
            LanguageChanged?.Invoke(this, EventArgs.Empty);
        }
        return CurrentCode;
    }

    public string ResolveCode(string? code)
    {
        if (!string.IsNullOrWhiteSpace(code) && HasLanguage(code)) return code.ToLowerInvariant();
        var sys = CultureInfo.CurrentUICulture.TwoLetterISOLanguageName;
        if (HasLanguage(sys)) return sys;
        return HasLanguage(FallbackCode) ? FallbackCode : _strings.Keys.FirstOrDefault() ?? FallbackCode;
    }

    public string this[string key] => Get(key);

    public string Get(string key)
    {
        if (_strings.TryGetValue(CurrentCode, out var d) && d.TryGetValue(key, out var v)) return v;
        if (_strings.TryGetValue(FallbackCode, out var en) && en.TryGetValue(key, out var e)) return e;
        return key;
    }

    /// <summary>Text mit Platzhaltern: Format("files.added", ("count", 3)).</summary>
    public string Format(string key, params (string Name, object? Value)[] args)
    {
        var text = Get(key);
        foreach (var (n, val) in args)
            text = text.Replace("{" + n + "}", Convert.ToString(val, CultureInfo.CurrentCulture) ?? string.Empty, StringComparison.Ordinal);
        return text;
    }

    /// <summary>Alle Schluessel einer Sprache (fuer Vollstaendigkeitspruefung / Vorlage).</summary>
    public IReadOnlyDictionary<string, string> GetAll(string code) =>
        _strings.TryGetValue(code, out var d) ? d : new Dictionary<string, string>();

    /// <summary>Fehlende Schluessel einer Sprache gegenueber Englisch.</summary>
    public IReadOnlyList<string> MissingKeys(string code)
    {
        if (!_strings.TryGetValue(FallbackCode, out var en)) return Array.Empty<string>();
        var d = _strings.TryGetValue(code, out var x) ? x : new Dictionary<string, string>();
        return en.Keys.Where(k => !d.ContainsKey(k)).OrderBy(k => k, StringComparer.Ordinal).ToList();
    }

    /// <summary>Schreibt eine Vorlage fuer eine neue Sprache (alle englischen Texte) in den Benutzerordner.</summary>
    public string WriteTemplate(string newCode, string newName)
    {
        newCode = SafeCodeRegex().Replace(newCode.Trim().ToLowerInvariant(), "");
        if (newCode.Length == 0) newCode = "xx";
        var path = Path.Combine(AppPaths.UserLanguagesDir, newCode + ".json");
        var en = GetAll(FallbackCode);
        var payload = new
        {
            meta = new { code = newCode, name = newName, englishName = newName, author = "", version = AppInfo.Version },
            strings = en.OrderBy(k => k.Key, StringComparer.Ordinal).ToDictionary(k => k.Key, k => k.Value),
        };
        var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions
        {
            WriteIndented = true,
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        });
        if (!File.Exists(path))
            File.WriteAllText(path, json);
        return path;
    }

    private static string Str(JsonElement e, string name, string fallback) =>
        e.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(v.GetString())
            ? v.GetString()!.Trim()
            : fallback;

    [GeneratedRegex("[^a-z0-9_-]")]
    private static partial Regex SafeCodeRegex();
}

/// <summary>Kurzform fuer Code ausserhalb der UI.</summary>
public static class L
{
    public static string T(string key) => LanguageCatalog.Current.Get(key);
    public static string F(string key, params (string Name, object? Value)[] args) => LanguageCatalog.Current.Format(key, args);
}
