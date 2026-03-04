#!/usr/bin/env bash
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)

AUR_FILES=(PKGBUILD .SRCINFO music-file-playlist-online-sync.install)
AUR_BRANCH="aur-push"
MAIN_BRANCH="main"

cd "$ROOT"

# Ensure we're on main before starting
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ "$CURRENT_BRANCH" != "$MAIN_BRANCH" ]]; then
  echo "ERROR: Must be on '$MAIN_BRANCH' branch (currently on '$CURRENT_BRANCH')." >&2
  exit 1
fi

# Regenerate .SRCINFO from PKGBUILD
if ! command -v makepkg &>/dev/null; then
  echo "ERROR: 'makepkg' not found. Install pacman/devtools to regenerate .SRCINFO." >&2
  exit 1
fi
echo "Regenerating .SRCINFO..."
makepkg --printsrcinfo > .SRCINFO

# Stage any changes to AUR files on main so they're committed
git add PKGBUILD .SRCINFO music-file-playlist-online-sync.install
if ! git diff --cached --quiet; then
  git commit -m "Update AUR files"
fi

# Switch to aur-push branch and update files from main
echo "Switching to '$AUR_BRANCH'..."
git checkout "$AUR_BRANCH"

echo "Updating AUR files from '$MAIN_BRANCH'..."
git checkout "$MAIN_BRANCH" -- "${AUR_FILES[@]}"

# Commit if there are changes
if ! git diff --cached --quiet; then
  PKGVER=$(grep -E '^pkgver=' PKGBUILD | cut -d= -f2)
  PKGREL=$(grep -E '^pkgrel=' PKGBUILD | cut -d= -f2)
  git commit -m "Update to v${PKGVER}-${PKGREL}"
else
  echo "No changes to AUR files, nothing to commit."
fi

echo "Pushing to AUR..."
git push aur

echo "Done. Switching back to '$MAIN_BRANCH'."
git checkout "$MAIN_BRANCH"
