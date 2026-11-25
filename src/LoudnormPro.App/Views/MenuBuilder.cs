using Avalonia;
using Avalonia.Controls;
using Avalonia.Input;
using LoudnormPro.App.Localization;
using LoudnormPro.App.ViewModels;

namespace LoudnormPro.App.Views;

/// <summary>
/// Baut die Menueleiste als NativeMenu:
/// Windows/Linux -> im Fenster (NativeMenuBar), macOS -> globale Menueleiste.
/// Wird bei Sprach-, Design- oder Profilwechsel neu aufgebaut.
/// </summary>
public static class MenuBuilder
{
    public static KeyModifiers CommandModifier =>
        OperatingSystem.IsMacOS() ? KeyModifiers.Meta : KeyModifiers.Control;

    public static void Populate(NativeMenu menu, MainWindowViewModel vm, Action showGuide, Action<bool> setTips)
    {
        var l = Loc.Instance;
        var cmd = CommandModifier;
        menu.Items.Clear();

        // ---------------- Datei ----------------
        var file = new NativeMenu();
        file.Add(Item(l["menu.file.add_files"], () => vm.AddFilesCommand.Execute(null), new KeyGesture(Key.O, cmd)));
        file.Add(Item(l["menu.file.add_folder"], () => vm.AddFolderCommand.Execute(null), new KeyGesture(Key.O, cmd | KeyModifiers.Shift)));
        file.Add(new NativeMenuItemSeparator());
        file.Add(Item(l["menu.file.choose_output"], () => vm.BrowseOutputCommand.Execute(null)));
        file.Add(Item(l["menu.file.open_output"], () => vm.OpenOutputCommand.Execute(null), new KeyGesture(Key.E, cmd)));
        file.Add(new NativeMenuItemSeparator());
        file.Add(Item(l["menu.file.clear_list"], () => vm.ClearListCommand.Execute(null)));
        if (!OperatingSystem.IsMacOS())
        {
            file.Add(new NativeMenuItemSeparator());
            file.Add(Item(l["menu.file.exit"], () => vm.ExitCommand.Execute(null), new KeyGesture(Key.F4, KeyModifiers.Alt)));
        }
        menu.Add(new NativeMenuItem(l["menu.file"]) { Menu = file });

        // ---------------- Einstellungen ----------------
        var settings = new NativeMenu();
        settings.Add(Item(l["menu.settings.normalization"], () => vm.OpenSettingsCommand.Execute("norm"), new KeyGesture(Key.OemComma, cmd)));
        settings.Add(Item(l["menu.settings.audio"], () => vm.OpenSettingsCommand.Execute("audio")));
        settings.Add(Item(l["menu.settings.video"], () => vm.OpenSettingsCommand.Execute("video")));
        settings.Add(Item(l["menu.settings.jobs"], () => vm.OpenSettingsCommand.Execute("jobs")));
        settings.Add(Item(l["menu.settings.tools"], () => vm.OpenSettingsCommand.Execute("tools")));
        settings.Add(new NativeMenuItemSeparator());

        // Profile
        var profiles = new NativeMenu();
        if (vm.Settings.Profiles.Count == 0)
        {
            profiles.Add(new NativeMenuItem(l["menu.settings.profiles.none"]) { IsEnabled = false });
        }
        else
        {
            foreach (var p in vm.Settings.Profiles)
            {
                var name = p.Name;
                profiles.Add(Radio(name, vm.Settings.ActiveProfile == name, () => vm.ApplyProfile(name)));
            }
        }
        profiles.Add(new NativeMenuItemSeparator());
        profiles.Add(Item(l["menu.settings.profiles.save"], () => vm.SaveProfileCommand.Execute(null)));
        if (!string.IsNullOrEmpty(vm.Settings.ActiveProfile))
        {
            var active = vm.Settings.ActiveProfile;
            profiles.Add(Item(l.F("menu.settings.profiles.delete", ("name", active)), () => vm.DeleteProfile(active)));
        }

        // Sprache (aus den JSON-Dateien)
        var langs = new NativeMenu();
        foreach (var lang in l.Catalog.Languages)
        {
            var code = lang.Code;
            var label = lang.Name == lang.EnglishName ? lang.Name : $"{lang.Name} ({lang.EnglishName})";
            langs.Add(Radio(label, vm.CurrentLanguage == code, () => vm.SetLanguage(code)));
        }
        langs.Add(new NativeMenuItemSeparator());
        langs.Add(Item(l["menu.settings.language.folder"], () => vm.OpenLanguageFolderCommand.Execute(null)));
        langs.Add(Item(l["menu.settings.language.template"], () => vm.CreateLanguageTemplateCommand.Execute(null)));
        langs.Add(Item(l["menu.settings.language.reload"], () => vm.ReloadLanguagesCommand.Execute(null)));
        settings.Add(new NativeMenuItem(l["menu.settings.language"]) { Menu = langs });

        // Design
        var theme = new NativeMenu();
        theme.Add(Radio(l["menu.settings.theme.dark"], vm.CurrentTheme == "dark", () => vm.ApplyTheme("dark")));
        theme.Add(Radio(l["menu.settings.theme.light"], vm.CurrentTheme == "light", () => vm.ApplyTheme("light")));
        theme.Add(Radio(l["menu.settings.theme.system"], vm.CurrentTheme == "system", () => vm.ApplyTheme("system")));
        settings.Add(new NativeMenuItem(l["menu.settings.theme"]) { Menu = theme });
        menu.Add(new NativeMenuItem(l["menu.settings"]) { Menu = settings });
        var profileName = string.IsNullOrEmpty(vm.Settings.ActiveProfile)
            ? l["profile.inactive"] : vm.Settings.ActiveProfile;
        menu.Add(new NativeMenuItem($"{l["menu.settings.profiles"]} ({profileName})") { Menu = profiles });

        // ---------------- Info ----------------
        var info = new NativeMenu();
        info.Add(Item(l["menu.info.build"], () => vm.ShowBuildInfoCommand.Execute(null)));
        info.Add(Item(l["menu.info.update"], () => vm.CheckUpdatesCommand.Execute(null)));
        info.Add(new NativeMenuItemSeparator());
        info.Add(Item(l["menu.info.changelog"], () => vm.ShowChangelogCommand.Execute(null)));
        info.Add(Item(l["menu.info.repo"], () => vm.OpenRepoCommand.Execute(null)));
        info.Add(Item(l["menu.info.diagnostics"], () => vm.ShowDiagnosticsCommand.Execute(null)));
        info.Add(Item(l["menu.info.data_folder"], () => vm.OpenDataFolderCommand.Execute(null)));
        info.Add(new NativeMenuItemSeparator());
        info.Add(Item(l["menu.info.about"], () => vm.ShowAboutCommand.Execute(null)));
        menu.Add(new NativeMenuItem(l["menu.info"]) { Menu = info });

        var tips = new NativeMenu();
        tips.Add(Radio("On", vm.Settings.QuickTips, () => setTips(true)));
        tips.Add(Radio("Off", !vm.Settings.QuickTips, () => setTips(false)));
        var help = new NativeMenu();
        help.Add(Item(l["help.guide"], showGuide));
        help.Add(new NativeMenuItem(l["help.tips"]) { Menu = tips });
        menu.Add(new NativeMenuItem(l["menu.help"]) { Menu = help });

    }

    private static NativeMenuItem Item(string header, Action onClick, KeyGesture? gesture = null)
    {
        var item = new NativeMenuItem(header);
        if (gesture != null) item.Gesture = gesture;
        item.Click += (_, _) => onClick();
        return item;
    }

    private static NativeMenuItem Radio(string header, bool isChecked, Action onClick)
    {
        var item = new NativeMenuItem(header)
        {
            ToggleType = MenuItemToggleType.Radio,
            IsChecked = isChecked,
        };
        item.Click += (_, _) => onClick();
        return item;
    }
}
