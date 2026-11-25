using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace LoudnormPro.App.Services;

/// <summary>
/// Fortschritt in der Windows-Taskleiste (ITaskbarList3) und Aufmerksamkeit (FlashWindowEx).
/// Auf macOS/Linux tun die Methoden nichts.
/// </summary>
public static class TaskbarProgress
{
    public enum State
    {
        None = 0,
        Indeterminate = 0x1,
        Normal = 0x2,
        Error = 0x4,
        Paused = 0x8,
    }

    private static object? _taskbar;
    private static bool _failed;

    public static void Set(IntPtr hwnd, double fraction, State state = State.Normal)
    {
        if (!OperatingSystem.IsWindows() || hwnd == IntPtr.Zero || _failed) return;
        try
        {
            var tb = GetTaskbar();
            if (tb == null) return;
            tb.SetProgressState(hwnd, state);
            if (state is State.Normal or State.Error or State.Paused)
                tb.SetProgressValue(hwnd, (ulong)Math.Clamp(fraction * 1000, 0, 1000), 1000);
        }
        catch
        {
            _failed = true;
        }
    }

    public static void Clear(IntPtr hwnd) => Set(hwnd, 0, State.None);

    [SupportedOSPlatform("windows")]
    private static ITaskbarList3? GetTaskbar()
    {
        if (_taskbar is ITaskbarList3 existing) return existing;
        var tb = (ITaskbarList3)new TaskbarInstance();
        tb.HrInit();
        _taskbar = tb;
        return tb;
    }

    /// <summary>Taskleisten-Symbol blinken lassen, bis das Fenster aktiviert wird.</summary>
    public static void Flash(IntPtr hwnd)
    {
        if (!OperatingSystem.IsWindows() || hwnd == IntPtr.Zero) return;
        try
        {
            var info = new FLASHWINFO
            {
                cbSize = (uint)Marshal.SizeOf<FLASHWINFO>(),
                hwnd = hwnd,
                dwFlags = 0x3 | 0xC, // FLASHW_ALL | FLASHW_TIMERNOFG
                uCount = 5,
                dwTimeout = 0,
            };
            FlashWindowEx(ref info);
        }
        catch
        {
            // nicht kritisch
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct FLASHWINFO
    {
        public uint cbSize;
        public IntPtr hwnd;
        public uint dwFlags;
        public uint uCount;
        public uint dwTimeout;
    }

    [DllImport("user32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool FlashWindowEx(ref FLASHWINFO pwfi);

    [ComImport]
    [Guid("ea1afb91-9e28-4b86-90e9-9e9f8a5eefaf")]
    [InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
    private interface ITaskbarList3
    {
        // ITaskbarList
        [PreserveSig] void HrInit();
        [PreserveSig] void AddTab(IntPtr hwnd);
        [PreserveSig] void DeleteTab(IntPtr hwnd);
        [PreserveSig] void ActivateTab(IntPtr hwnd);
        [PreserveSig] void SetActiveAlt(IntPtr hwnd);
        // ITaskbarList2
        [PreserveSig] void MarkFullscreenWindow(IntPtr hwnd, [MarshalAs(UnmanagedType.Bool)] bool fFullscreen);
        // ITaskbarList3
        [PreserveSig] void SetProgressValue(IntPtr hwnd, ulong ullCompleted, ulong ullTotal);
        [PreserveSig] void SetProgressState(IntPtr hwnd, State state);
    }

    [ComImport]
    [Guid("56FDF344-FD6D-11d0-958A-006097C9A090")]
    [ClassInterface(ClassInterfaceType.None)]
    private class TaskbarInstance
    {
    }
}
