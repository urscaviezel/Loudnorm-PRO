# Loudnorm PRO

## 🎬 Loudnorm PRO – Audio Normalization Tool (FFmpeg GUI)

Loudnorm PRO is a cross-platform GUI tool for batch audio normalization of video files using FFmpeg.

Supports modern hardware acceleration:

* NVIDIA (NVENC)
* AMD (AMF on Windows, VAAPI on Linux)
* CPU (x265)

---

## 🚀 Features

* 🎧 EBU R128 loudness normalization (2-pass loudnorm)
* 🎬 Batch processing (multiple files or folders)
* ⚡ Hardware encoding support:

  * HEVC NVENC (NVIDIA)
  * HEVC AMF (AMD Windows)
  * HEVC VAAPI (Linux AMD)
  * HEVC x265 (CPU fallback)
* 📊 Audio track preview (language detection)
* 🔁 Resume interrupted jobs
* 🧠 Smart overwrite system (safe temp handling)
* 🎨 Dark / Light UI
* 🌍 Multi-language (DE / EN)

---

## 📦 Downloads

### Windows

* Installer: `Loudnorm_PRO_Setup_v1.0.3.exe`
* Portable: `Loudnorm_PRO_v1.0.3_portable.zip`

### Linux

* Archive: `Loudnorm_PRO_linux_x86_64.tar.gz`

---

## 🖥️ Requirements

### General

* FFmpeg (required)
* FFprobe (required)

### Windows

* FFmpeg included OR installed in:

  * `C:\ffmpeg\bin`
  * PATH

### Linux

* Install dependencies:

```bash
sudo apt install ffmpeg
```

or

```bash
sudo pacman -S ffmpeg
```

---

## ▶️ Usage

### Windows

Run:

```
Loudnorm PRO.exe
```

---

### Linux

Extract:

```bash
tar -xzf Loudnorm_PRO_linux_x86_64.tar.gz
cd Loudnorm_PRO
chmod +x Loudnorm_PRO
./Loudnorm_PRO
```

---

## ⚙️ Configuration

Settings file location:

### Windows

```
%LOCALAPPDATA%\Loudnorm PRO\loudnorm_settings.json
```

### Linux

```
~/.local/share/Loudnorm PRO/loudnorm_settings.json
```

Example:

```json
{
  "temp_work_dir": "/mnt/fastssd/loudnorm_temp"
}
```

---

## ⚠️ Notes

* Overwrite replaces original file **only after successful processing**
* Temporary files are used to prevent data loss
* Hardware encoding availability depends on your system and drivers

---

## 🧪 Tested Systems

### Windows

* NVIDIA RTX 5090 → NVENC ✔
* AMD RX 9070 XT → AMF ✔

### Linux

* AMD RX 9070 XT → VAAPI ✔

---

## 🛠️ Known Limitations

* VAAPI options depend on driver support
* AMF encoder is sensitive to parameter combinations
* FFmpeg must be properly installed and accessible

---

## ❤️ Credits

* FFmpeg (https://ffmpeg.org/)
* tkinter / tkinterdnd2

---

## 📄 License

GNU GPL v3

---

© Loudnorm PRO
