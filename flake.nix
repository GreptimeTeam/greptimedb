{
  description = "GreptimeDB as nix flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
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

        lib = nixpkgs.lib;
        rustToolchain = fenix.packages.${system}.fromToolchainName {
          name = (lib.importTOML ./rust-toolchain.toml).toolchain.channel;
          sha256 = "sha256-tJJr8oqX3YD+ohhPK7jlt/7kvKBnBqJVjYtoFr520d4=";
        };

        # Read Cargo.toml to get package name and version
        cargoToml = lib.importTOML ./Cargo.toml;
        packageName = "greptimedb";
        packageVersion = cargoToml.workspace.package.version;
      in
      {
        packages.default = pkgs.rustPlatform.buildRustPackage {
          pname = packageName;
          version = packageVersion;

          src = lib.cleanSource ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
            allowBuiltinFetchGit = true;
            outputHashes = {
              "datafusion-45.0.0" = "sha256-Js5E0m0igvpo6WcOws6sRs3JGreNTxPOFjRSsBQnJfk=";
              "greptime-proto-0.1.0" = "sha256-gLgzqreOeDQmwMHkZu+iYUSmMRN0qdFKm8JwCNugI6w=";
              "influxdb_line_protocol-0.1.0" = "sha256-RvPS4erQDrMxzPbZu8gT4g5q9UHMaq9DS5P0kb0UhD4=";
              "jsonb-0.4.3" = "sha256-/nBUhsbDtuKqDpPNeP5dAqDaIFKMGoIsqv07qHZmkug=";
              "loki-proto-0.1.0" = "sha256-puwIdzwZ9zKh6BCgXZ2iE1f3f733GtByNbr3GvTrTkw=";
              "meter-core-0.1.0" = "sha256-E2lvqsfY5nEKgXcHAubzNKcZveX8VI0GMT9OSRvIFgs=";
              "opensrv-mysql-0.8.0" = "sha256-UJcihra+L7L5bOOtayjMqKKpkTYYxPre8Fwd1lvOc18=";
              "orc-rust-0.6.0" = "sha256-uPG94OF5l6JxWbjiejqJ6Zz+vOX9hL3pamomz2hkrG8=";
              "otel-arrow-rust-0.1.0" = "sha256-PB6zInde/ASQR7fqMs44IhaXqMpKW5AILMZOnaPN2eY=";
              "pgwire-0.30.2" = "sha256-TJMYAleRrXp8nW5l/OXfHji+AgyBGsxUamrhMLn8hps=";
              "rskafka-0.6.0" = "sha256-wjJ+FuHSBu5a5tSYq22ZrQGLHou/aIcsmUT0j1OinJw=";
              "sqlness-0.6.1" = "sha256-ZtUVLJ1U4A6mNR+iNvRtkhIPWHe2ZSJ+kt533B6vivQ=";
              "sqlparser-0.54.0-greptime" = "sha256-l2Szp9ytcbXJDyvYR+jKFwa+AYqtwZvFqQ3KzgGbH2A=";
              "uddsketch-0.1.0" = "sha256-VEub0vUF3w7U5LZsmBueegHNiuAE6jSgyo+x5r1C4h8=";
            };
          };

          inherit buildInputs;
          inherit nativeBuildInputs;

          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath buildInputs;
          NIX_HARDENING_ENABLE = "";

          cargoBuildFlags = [ "--bin" "greptime" ];

          meta = with lib; {
            description = "Cloud-native database for observability.";
            license = licenses.asl20;
            maintainers = [ "sunning@greptime.com" ];
            platforms = platforms.all;
          };
        };

        devShells.default = pkgs.mkShell {
          inherit nativeBuildInputs;

          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath buildInputs;
          NIX_HARDENING_ENABLE = "";
        };
      });
}
