using System.Text;

namespace LoudnormPro.Core.Processing;

/// <summary>Minimaler RFC-4180-CSV-Leser/-Schreiber (kompatibel zu Pythons csv-Modul).</summary>
public static class Csv
{
    public static string Escape(string? value)
    {
        value ??= string.Empty;
        return value.IndexOfAny(new[] { ',', '"', '\n', '\r' }) >= 0
            ? "\"" + value.Replace("\"", "\"\"") + "\""
            : value;
    }

    public static string Line(params string?[] values) => string.Join(",", values.Select(Escape));

    public static List<List<string>> Parse(string text)
    {
        var rows = new List<List<string>>();
        var row = new List<string>();
        var field = new StringBuilder();
        var inQuotes = false;

        for (var i = 0; i < text.Length; i++)
        {
            var c = text[i];
            if (inQuotes)
            {
                if (c == '"')
                {
                    if (i + 1 < text.Length && text[i + 1] == '"') { field.Append('"'); i++; }
                    else inQuotes = false;
                }
                else field.Append(c);
                continue;
            }

            switch (c)
            {
                case '"': inQuotes = true; break;
                case ',': row.Add(field.ToString()); field.Clear(); break;
                case '\r': break;
                case '\n':
                    row.Add(field.ToString()); field.Clear();
                    rows.Add(row); row = new List<string>();
                    break;
                default: field.Append(c); break;
            }
        }

        if (field.Length > 0 || row.Count > 0)
        {
            row.Add(field.ToString());
            rows.Add(row);
        }
        return rows;
    }

    /// <summary>CSV mit Kopfzeile -> Liste von Dictionaries.</summary>
    public static List<Dictionary<string, string>> ParseWithHeader(string text)
    {
        var rows = Parse(text);
        var result = new List<Dictionary<string, string>>();
        if (rows.Count == 0) return result;
        var header = rows[0];
        foreach (var r in rows.Skip(1))
        {
            if (r.Count == 1 && r[0].Length == 0) continue;
            var d = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < header.Count; i++)
                d[header[i].Trim()] = i < r.Count ? r[i] : string.Empty;
            result.Add(d);
        }
        return result;
    }
}
