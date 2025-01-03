let
  nixpkgs = fetchTarball "https://github.com/NixOS/nixpkgs/tarball/nixos-24.11";
  fenix = import (fetchTarball "https://github.com/nix-community/fenix/archive/main.tar.gz") {};
  pkgs = import nixpkgs { config = {}; overlays = []; };
in

pkgs.mkShell rec {
  nativeBuildInputs = with pkgs; [
    pkg-config
    git
    clang
    gcc
    protobuf
    mold
    (fenix.fromToolchainFile {
      dir = ./.;
    })
    cargo-nextest
    taplo
    curl
  ];

  buildInputs = with pkgs; [
    libgit2
    libz
  ];

  LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath buildInputs;
}
