#!/bin/bash
# ------------------------------------------------------------------
# Loudnorm PRO v2 - Linux-Build
#   ./tools/build-linux.sh   -> publish/linux/LoudnormPRO-<version>-linux-x64.tar.gz
# ------------------------------------------------------------------
set -euo pipefail
RID="${1:-linux-x64}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROJ="$ROOT/src/LoudnormPro.App/LoudnormPro.App.csproj"
OUT="$ROOT/publish/linux"
PUB="$OUT/Loudnorm_PRO"
VERSION=$(grep -m1 '<Version>' "$ROOT/Directory.Build.props" | sed -E 's/.*<Version>(.*)<\/Version>.*/\1/')

rm -rf "$PUB"
dotnet publish "$PROJ" -c Release -r "$RID" --self-contained true -p:PublishSingleFile=true -o "$PUB"
cp "$ROOT/src/LoudnormPro.App/Assets/loudnorm-256.png" "$PUB/loudnorm.png"

cat > "$PUB/loudnorm-pro.desktop" <<DESK
[Desktop Entry]
Type=Application
Name=Loudnorm PRO
Comment=EBU R128 loudness normalization for videos
Exec=LoudnormPRO %F
Icon=loudnorm
Categories=AudioVideo;Audio;Video;
Terminal=false
DESK

tar -C "$OUT" -czf "$OUT/LoudnormPRO-$VERSION-$RID.tar.gz" Loudnorm_PRO
echo "Fertig: $OUT/LoudnormPRO-$VERSION-$RID.tar.gz"
