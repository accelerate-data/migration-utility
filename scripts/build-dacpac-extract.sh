#!/usr/bin/env bash
# Build the dacpac-extract .NET sidecar and copy to src-tauri/binaries/.
# Usage: ./scripts/build-dacpac-extract.sh [target-triple]
# Default target triple: aarch64-apple-darwin (macOS ARM)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TARGET="${1:-aarch64-apple-darwin}"

# Map Rust target triple to .NET RID.
case "$TARGET" in
  aarch64-apple-darwin) RID="osx-arm64" ;;
  x86_64-apple-darwin)  RID="osx-x64" ;;
  x86_64-unknown-linux-gnu) RID="linux-x64" ;;
  x86_64-pc-windows-msvc) RID="win-x64" ;;
  *) echo "Unknown target triple: $TARGET" >&2; exit 1 ;;
esac

PROJECT="$REPO_ROOT/app/dacpac-extract/dacpac-extract.csproj"
OUT_DIR="$REPO_ROOT/app/src-tauri/binaries"

echo "Building dacpac-extract for $RID → binaries/dacpac-extract-$TARGET"
dotnet publish "$PROJECT" \
  -c Release \
  -r "$RID" \
  --self-contained \
  -p:PublishSingleFile=true \
  -p:PublishTrimmed=false \
  -o "$REPO_ROOT/app/dacpac-extract/bin/Release/net8.0/$RID/publish"

SRC="$REPO_ROOT/app/dacpac-extract/bin/Release/net8.0/$RID/publish/dacpac-extract"
DEST="$OUT_DIR/dacpac-extract-$TARGET"
mkdir -p "$OUT_DIR"
cp "$SRC" "$DEST"
chmod +x "$DEST"
echo "Done: $DEST ($(du -sh "$DEST" | cut -f1))"
