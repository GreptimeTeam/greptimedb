let
  nixpkgs = fetchTarball "https://github.com/NixOS/nixpkgs/tarball/nixos-unstable";
  fenix = import (fetchTarball "https://github.com/nix-community/fenix/archive/main.tar.gz") {};
  pkgs = import nixpkgs { config = {}; overlays = []; };
in

pkgs.mkShellNoCC {
  packages = with pkgs; [
    git
    clang
    gcc
    mold
    libgit2
    protobuf
    (fenix.fromToolchainFile {
      dir = ./.;
    })
    fenix.rust-analyzer
    cargo-nextest
  ];

}
