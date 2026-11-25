using System.Runtime.InteropServices;
using System.Text;
using Avalonia.Controls;
using Avalonia.Threading;

namespace LoudnormPro.App.Services;

/// <summary>
/// Klassische Windows-Dateiablage (WM_DROPFILES) statt OLE-Drag&amp;Drop.
///
/// Hintergrund: Bei manchen Setups (z. B. mehrere Monitore mit unterschiedlicher Skalierung)
/// kommen OLE-Drag-Ereignisse aus dem Explorer nicht im Avalonia-Fenster an. WM_DROPFILES
/// ist der einfache Shell-Mechanismus, den auch viele klassische Programme nutzen, und
/// funktioniert zusaetzlich, wenn die App mit Administratorrechten laeuft.
/// </summary>
public static class WindowsFileDrop
{
    private const uint WM_DROPFILES = 0x0233;
    private const uint WM_COPYDATA = 0x004A;
    private const uint WM_COPYGLOBALDATA = 0x0049;
    private const uint MSGFLT_ALLOW = 1;

    private static Win32Properties.CustomWndProcHookCallback? _hook;

    public static bool Attach(Window window, Action<IReadOnlyList<string>> onDrop)
    {
        if (!OperatingSystem.IsWindows()) return false;
        var hwnd = window.TryGetPlatformHandle()?.Handle ?? IntPtr.Zero;
        if (hwnd == IntPtr.Zero) return false;

        try
        {
            // OLE-Ablageziel entfernen, sonst bevorzugt der Explorer dieses
            RevokeDragDrop(hwnd);
            ChangeWindowMessageFilterEx(hwnd, WM_DROPFILES, MSGFLT_ALLOW, IntPtr.Zero);
            ChangeWindowMessageFilterEx(hwnd, WM_COPYDATA, MSGFLT_ALLOW, IntPtr.Zero);
            ChangeWindowMessageFilterEx(hwnd, WM_COPYGLOBALDATA, MSGFLT_ALLOW, IntPtr.Zero);
            DragAcceptFiles(hwnd, true);

            _hook = (IntPtr h, uint msg, IntPtr wParam, IntPtr lParam, ref bool handled) =>
            {
                if (msg != WM_DROPFILES) return IntPtr.Zero;
                handled = true;
                var files = ReadDrop(wParam);
                if (files.Count > 0)
                    Dispatcher.UIThread.Post(() => onDrop(files));
                return IntPtr.Zero;
            };
            Win32Properties.AddWndProcHookCallback(window, _hook);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static List<string> ReadDrop(IntPtr hDrop)
    {
        var result = new List<string>();
        try
        {
            var count = DragQueryFileW(hDrop, 0xFFFFFFFF, null, 0);
            for (uint i = 0; i < count; i++)
            {
                var len = DragQueryFileW(hDrop, i, null, 0);
                if (len == 0) continue;
                var sb = new StringBuilder((int)len + 1);
                if (DragQueryFileW(hDrop, i, sb, len + 1) > 0)
                    result.Add(sb.ToString());
            }
        }
        finally
        {
            DragFinish(hDrop);
        }
        return result;
    }

    [DllImport("shell32.dll")]
    private static extern void DragAcceptFiles(IntPtr hWnd, [MarshalAs(UnmanagedType.Bool)] bool fAccept);

    [DllImport("shell32.dll", CharSet = CharSet.Unicode)]
    private static extern uint DragQueryFileW(IntPtr hDrop, uint iFile, StringBuilder? lpszFile, uint cch);

    [DllImport("shell32.dll")]
    private static extern void DragFinish(IntPtr hDrop);

    [DllImport("ole32.dll")]
    private static extern int RevokeDragDrop(IntPtr hwnd);

    [DllImport("user32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool ChangeWindowMessageFilterEx(IntPtr hWnd, uint message, uint action, IntPtr pChangeFilterStruct);
}
