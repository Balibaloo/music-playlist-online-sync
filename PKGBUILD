# Maintainer: Roman <your-email@example.com>
pkgname=music-file-playlist-online-sync
giturl="https://github.com/roman/music-playlist-online-sync.git"
pkgver=0.2.0
pkgrel=1
pkgdesc="Music folder to remote playlist sync tool (Spotify/Tidal prototype)"
arch=('x86_64')
url="https://github.com/roman/music-playlist-online-sync"
license=('MIT')
depends=('systemd')
makedepends=('rust' 'cargo' 'pkg-config')
source=("$giturl")
md5sums=('SKIP')

build() {
  cd "$srcdir/music-playlist-online-sync"
  cargo build --release --locked
}

package() {
  cd "$srcdir/music-playlist-online-sync"
  install -Dm755 target/release/cli "$pkgdir/usr/bin/music-file-playlist-online-sync"
  install -Dm644 config/example-config.toml "$pkgdir/etc/music-sync/example-config.toml"
  install -Dm644 LICENSE "$pkgdir/usr/share/licenses/$pkgname/LICENSE"
  install -Dm644 systemd/music-file-playlist-online-sync-worker.service "$pkgdir/usr/lib/systemd/system/music-file-playlist-online-sync-worker.service"
  install -Dm644 systemd/music-file-playlist-online-sync-worker.timer "$pkgdir/usr/lib/systemd/system/music-file-playlist-online-sync-worker.timer"
  install -Dm644 systemd/music-file-playlist-online-sync-watcher.service "$pkgdir/usr/lib/systemd/system/music-file-playlist-online-sync-watcher.service"
  install -Dm644 systemd/music-file-playlist-online-sync-reconcile.timer "$pkgdir/usr/lib/systemd/system/music-file-playlist-online-sync-reconcile.timer"
}

install=music-file-playlist-online-sync.install
