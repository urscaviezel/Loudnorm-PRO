## v1.0.4

### New
- WebM input support in file selection, folder scanning and Drag & Drop
- Automatic MKV output for WebM sources to support AAC, E-AC3 and HEVC safely

### Fixes
- Filenames containing special characters such as `|` are now handled correctly by Drag & Drop
- WebM source files remain untouched when overwrite mode is enabled
- Existing-output detection includes all supported encoder suffixes

## v1.0.3

### New
- Linux Version (tested and compiled on CachyOS)
- AMD AMF support (Windows)
- AMD VAAPI support (Linux)

### Fixes
- Encoder detection (Windows/Linux)
- Overwrite stability
- UI scaling issues (1080p)
