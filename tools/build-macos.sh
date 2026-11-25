#!/bin/bash
# Loudnorm PRO v2 - signed and notarized macOS build
#
#   ./tools/build-macos.sh            -> Apple Silicon (osx-arm64)
#   ./tools/build-macos.sh osx-x64    -> Intel Mac
#
# Requires a Developer ID Application certificate and a notarytool
# keychain profile named "loudnorm-notary".
set -euo pipefail

RID="${1:-osx-arm64}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROJ="$ROOT/src/LoudnormPro.App/LoudnormPro.App.csproj"
OUT="$ROOT/publish/macos"
PUB="$OUT/bin-$RID"
APP="$OUT/Loudnorm PRO.app"
DMG_STAGE="$OUT/dmg-stage"
ENTITLEMENTS="$ROOT/tools/macos.entitlements"
SIGN_IDENTITY="${SIGN_IDENTITY:-Developer ID Application: Urs Caviezel (28KW362VFW)}"
NOTARY_PROFILE="${NOTARY_PROFILE:-loudnorm-notary}"
VERSION=$(grep -m1 '<Version>' "$ROOT/Directory.Build.props" | sed -E 's/.*<Version>(.*)<\/Version>.*/\1/')
DMG="$OUT/LoudnormPRO-$VERSION-$RID.dmg"

echo "==> Signing identity prüfen"
security find-identity -v -p codesigning | grep -F "$SIGN_IDENTITY" >/dev/null

echo "==> dotnet publish ($RID, Version $VERSION)"
rm -rf "$PUB" "$APP" "$DMG_STAGE"
rm -f "$DMG"
dotnet publish "$PROJ" -c Release -r "$RID" --self-contained true \
  -p:PublishSingleFile=false -p:UseAppHost=true -o "$PUB"

echo "==> .app-Bundle erstellen"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"
cp -R "$PUB"/. "$APP/Contents/MacOS/"

ICONSET="$OUT/loudnorm.iconset"
rm -rf "$ICONSET"
mkdir -p "$ICONSET"
SRC_PNG="$ROOT/src/LoudnormPro.App/Assets/loudnorm-256.png"
for s in 16 32 64 128 256; do
  sips -z "$s" "$s" "$SRC_PNG" --out "$ICONSET/icon_${s}x${s}.png" >/dev/null
done
cp "$ICONSET/icon_32x32.png" "$ICONSET/icon_16x16@2x.png"
cp "$ICONSET/icon_64x64.png" "$ICONSET/icon_32x32@2x.png"
cp "$ICONSET/icon_256x256.png" "$ICONSET/icon_128x128@2x.png"
rm -f "$ICONSET/icon_64x64.png"
iconutil -c icns "$ICONSET" -o "$APP/Contents/Resources/loudnorm.icns"
rm -rf "$ICONSET"

cat > "$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "https://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key>               <string>Loudnorm PRO</string>
  <key>CFBundleDisplayName</key>        <string>Loudnorm PRO</string>
  <key>CFBundleIdentifier</key>         <string>com.pfdhub.loudnormpro</string>
  <key>CFBundleVersion</key>            <string>$VERSION</string>
  <key>CFBundleShortVersionString</key> <string>$VERSION</string>
  <key>CFBundleExecutable</key>         <string>LoudnormPRO</string>
  <key>CFBundleIconFile</key>           <string>loudnorm.icns</string>
  <key>CFBundlePackageType</key>        <string>APPL</string>
  <key>LSMinimumSystemVersion</key>     <string>12.0</string>
  <key>NSHighResolutionCapable</key>    <true/>
  <key>LSApplicationCategoryType</key>  <string>public.app-category.video</string>
</dict>
</plist>
PLIST

echo "==> App und .NET-Bestandteile mit Developer ID signieren"
codesign --force --deep --sign "$SIGN_IDENTITY" --options runtime --timestamp \
  --entitlements "$ENTITLEMENTS" "$APP"
codesign --verify --deep --strict --verbose=2 "$APP"

echo "==> Drag-to-Applications-DMG erstellen"
mkdir -p "$DMG_STAGE"
cp -R "$APP" "$DMG_STAGE/"
ln -s /Applications "$DMG_STAGE/Applications"
hdiutil create -volname "Loudnorm PRO" -srcfolder "$DMG_STAGE" \
  -ov -format UDZO "$DMG" >/dev/null
rm -rf "$DMG_STAGE"

echo "==> DMG signieren"
codesign --force --sign "$SIGN_IDENTITY" --timestamp "$DMG"
codesign --verify --verbose=2 "$DMG"

echo "==> Bei Apple zur Notarisierung einreichen"
xcrun notarytool submit "$DMG" --keychain-profile "$NOTARY_PROFILE" --wait

echo "==> Notarisierungsticket anheften"
xcrun stapler staple "$DMG"
xcrun stapler validate "$DMG"
spctl --assess --type open --context context:primary-signature --verbose=2 "$DMG"

echo
echo "Fertig:"
echo "  $APP"
echo "  $DMG"
