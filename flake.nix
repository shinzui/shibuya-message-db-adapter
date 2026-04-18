{
  description = "A shibuya adapter for message-db-hs";

  inputs.nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.treefmt-nix.url = "github:numtide/treefmt-nix";
  inputs.pre-commit-hooks.url = "github:cachix/git-hooks.nix";

  outputs = { self, nixpkgs, flake-utils, treefmt-nix, pre-commit-hooks }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        ghcVersion = "ghc912";
        haskellPackages = pkgs.haskell.packages."${ghcVersion}";
        treefmtEval = treefmt-nix.lib.evalModule pkgs ./treefmt.nix;
        formatter = treefmtEval.config.build.wrapper;

        # Feature flags
        withProcessCompose = true;
        withPostgresql = true;
      in
      {
        formatter = formatter;

        packages = {
          default = haskellPackages.shibuya-message-db-adapter;
        };

        checks = {
          formatting = treefmtEval.config.build.check self;
          pre-commit-check = pre-commit-hooks.lib.${system}.run {
            src = ./.;
            hooks = {
              treefmt.package = formatter;
              treefmt.enable = true;
            };
          };
        };

        devShells.default = pkgs.mkShell {
          nativeBuildInputs = [
            pkgs.zlib
            pkgs.just
            pkgs.cabal-install
            pkgs.pkg-config
            (haskellPackages.ghcWithPackages (ps: [
              ps.haskell-language-server
            ]))
          ]
          ++ pkgs.lib.optional withProcessCompose pkgs.process-compose
          ++ pkgs.lib.optional withPostgresql pkgs.postgresql;

          shellHook = ''
            ${self.checks.${system}.pre-commit-check.shellHook}
            export LANG=en_US.UTF-8
          ''
          + pkgs.lib.optionalString withPostgresql ''
            export PGHOST="$PWD/db"
            export PGDATA="$PGHOST/db"
            export PGLOG=$PGHOST/postgres.log
            export PGDATABASE=shibuya-message-db-adapter
            export PG_CONNECTION_STRING=postgresql://$(jq -rn --arg x $PGHOST '$x|@uri')/$PGDATABASE

            mkdir -p $PGHOST
            mkdir -p .dev

            if [ ! -d $PGDATA ]; then
              initdb --auth=trust --no-locale --encoding=UTF8
            fi
          '';
        };
      }
    );
}
