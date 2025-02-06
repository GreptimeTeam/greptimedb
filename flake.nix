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
        lib = nixpkgs.lib;
        rustToolchain = fenix.packages.${system}.fromToolchainName {
          name = (lib.importTOML ./rust-toolchain.toml).toolchain.channel;
          sha256 = "sha256-f/CVA1EC61EWbh0SjaRNhLL0Ypx2ObupbzigZp8NmL4=";
        };
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
            (rustToolchain.withComponents [
              "cargo"
              "clippy"
              "rust-src"
              "rustc"
              "rustfmt"
              "rust-analyzer"
              "llvm-tools"
            ])
            cargo-nextest
            cargo-llvm-cov
            taplo
            curl
            gnuplot ## for cargo bench
          ];

          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath buildInputs;
        };
      });
}
