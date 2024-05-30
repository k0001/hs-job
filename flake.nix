{
  description = "Haskell 'job' library";

  inputs = {
    flakety.url =
      "github:k0001/flakety/d5262bc8bbed901ad2e0bec59904b60d9a5e28df";
    nixpkgs.follows = "flakety/nixpkgs";
    flake-parts.follows = "flakety/flake-parts";
    hs_sq.url = "github:k0001/hs-sq.git";
    hs_sq.inputs.flakety.follows = "flakety";
  };

  outputs = inputs@{ ... }:
    inputs.flake-parts.lib.mkFlake { inherit inputs; } {
      flake.overlays.default = inputs.nixpkgs.lib.composeManyExtensions [
        inputs.hs_sq.overlays.default
        (final: prev:
          let
            hsLib = prev.haskell.lib;
            hsClean = drv:
              hsLib.overrideCabal drv
              (old: { src = prev.lib.sources.cleanSource old.src; });
          in {
            haskell = prev.haskell // {
              packageOverrides = prev.lib.composeExtensions
                (prev.haskell.packageOverrides or (_: _: { })) (hfinal: hprev:
                  prev.lib.optionalAttrs
                  (prev.lib.versionAtLeast hprev.ghc.version "9.6") {
                    job = hsLib.doBenchmark (hfinal.callPackage ./job { });
                    #job-hasql =
                    #  hsLib.doBenchmark (hfinal.callPackage ./job-hasql { });
                    job-sq =
                      hsLib.doBenchmark (hfinal.callPackage ./job-sq { });
                  });
            };
          })
      ];
      systems = [ "x86_64-linux" "i686-linux" "aarch64-linux" ];
      perSystem = { config, pkgs, system, ... }: {
        _module.args.pkgs = import inputs.nixpkgs {
          inherit system;
          overlays = [ inputs.self.overlays.default ];
        };
        packages = {
          job__ghc98 = pkgs.haskell.packages.ghc98.job;
          job__ghc98__sdist =
            pkgs.haskell.packages.ghc98.cabalSdist { src = ./job; };
          job__ghc98__sdistDoc =
            pkgs.haskell.lib.documentationTarball config.packages.job__ghc98;
          job-sq__ghc98 = pkgs.haskell.packages.ghc98.job-sq;
          job-sq__ghc98__sdist =
            pkgs.haskell.packages.ghc98.cabalSdist { src = ./job-sq; };
          job-sq__ghc98__sdistDoc =
            pkgs.haskell.lib.documentationTarball config.packages.job-sq__ghc98;
          #job-hasql__ghc98 = pkgs.haskell.packages.ghc98.job-hasql;
          #job-hasql__ghc98__sdist =
          #  pkgs.haskell.packages.ghc98.cabalSdist { src = ./job-hasql; };
          #job-hasql__ghc98__sdistDoc = pkgs.haskell.lib.documentationTarball
          #  config.packages.job-hasql__ghc98;
          default = pkgs.releaseTools.aggregate {
            name = "every output from this flake";
            constituents = [
              config.packages.job__ghc98
              config.packages.job__ghc98.doc
              config.packages.job__ghc98__sdist
              config.packages.job__ghc98__sdistDoc
              config.packages.job-sq__ghc98
              config.packages.job-sq__ghc98.doc
              config.packages.job-sq__ghc98__sdist
              config.packages.job-sq__ghc98__sdistDoc
              #config.packages.job-hasql__ghc98
              #config.packages.job-hasql__ghc98.doc
              #config.packages.job-hasql__ghc98__sdist
              #config.packages.job-hasql__ghc98__sdistDoc
              config.devShells.ghc98
            ];
          };
        };
        devShells = let
          mkShellFor = ghc:
            ghc.shellFor {
              packages = p: [
                p.job
                p.job-sq # p.job-hasql
              ];
              doBenchmark = true;
              withHoogle = true;
              nativeBuildInputs =
                [ pkgs.cabal-install pkgs.cabal2nix pkgs.ghcid ];
            };
        in {
          default = config.devShells.ghc98;
          ghc98 = mkShellFor pkgs.haskell.packages.ghc98;
        };
      };
    };
}
