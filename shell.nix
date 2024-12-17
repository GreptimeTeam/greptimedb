let
  nixpkgs = fetchTarball "https://github.com/NixOS/nixpkgs/tarball/nixos-unstable";
  fenix = import (fetchTarball "https://github.com/nix-community/fenix/archive/main.tar.gz") {};
  pkgs = import nixpkgs { config = {}; overlays = []; };
in

pkgs.mkShell rec {
  nativeBuildInputs = with pkgs; [
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
  ];

  buildInputs = with pkgs; [
    libgit2
  ];

  LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath buildInputs;
}
