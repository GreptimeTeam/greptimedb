{
  description = "Development environment flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
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
        ];
        lib = nixpkgs.lib;
        rustToolchain = fenix.packages.${system}.fromToolchainName {
          name = (lib.importTOML ./rust-toolchain.toml).toolchain.channel;
          sha256 = "sha256-GCGEXGZeJySLND0KU5TdtTrqFV76TF3UdvAHSUegSsk=";
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

          buildInputs = buildInputs;
          NIX_HARDENING_ENABLE = "";
        };
      });
}
