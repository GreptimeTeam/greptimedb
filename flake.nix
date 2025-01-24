{
  description = "Development environment flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, fenix, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        buildInputs = with pkgs; [
          libgit2
          libz
        ];

      in
      {
        devShells.default = pkgs.mkShell {
          nativeBuildInputs = with pkgs; [
            pkg-config
            git
            clang
            gcc
            protobuf
            gnumake
            mold
            (fenix.packages.${system}.fromToolchainFile {
              dir = ./.;
              sha256 = "sha256-9GnMWM2pjzawUuVU7EbnPMn79rMknaA4EaxipyTgqig=";
            })
            cargo-nextest
            cargo-llvm-cov
            taplo
            curl
          ];

          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath buildInputs;
        };
      });
}
