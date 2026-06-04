# Dev shell, built from the haskell-nix-dev base flake's mkDevShell (GHC 9.12.4 +
# cabal + HLS). This project is dev-shell-only: the package and its many
# source-repository-package dependencies are built with cabal inside this shell.
# Add project-specific dev tools via `haskellProject.extraDevPackages`, or
# directly in the extraNativeBuildInputs list below.
#
# mkDevShell already provides: the GHC compiler, cabal, HLS (when withHls),
# pkg-config, and zlib, plus a LANG=en_US.UTF-8 export. Only list tools BEYOND
# those in extraNativeBuildInputs (e.g. pkgs.just, pkgs.process-compose).
{ inputs, lib, flake-parts-lib, ... }:
{
  options.perSystem = flake-parts-lib.mkPerSystemOption ({ ... }: {
    options.haskellProject.extraDevPackages = lib.mkOption {
      type = lib.types.listOf lib.types.package;
      default = [ ];
      example = lib.literalExpression "[ pkgs.ghciwatch ]";
      description = "Extra packages to add to the dev shell.";
    };
  });

  config.perSystem = { system, pkgs, config, ... }:
    let
      hsdev = inputs.haskell-nix-dev.lib.${system};

      mkProjectShell = ghc: hsdev.mkDevShell {
        inherit ghc;
        withHls = true;
        extraNativeBuildInputs =
          [
            pkgs.just
            pkgs.process-compose
            pkgs.postgresql
          ]
          ++ config.haskellProject.extraDevPackages;
        shellHook = ''
          ${config.pre-commit.installationScript}

          # Integration tests read the message-db SQL scripts from this
          # directory. Keeping the path explicit in the dev shell avoids
          # a hard-coded fallback printing a warning on every run.
          export MESSAGE_DB_SQL_DIR=/Users/shinzui/Keikaku/hub/event-sourcing/message-db-project/message-db/database

          export PGHOST="$PWD/db"
          export PGDATA="$PGHOST/db"
          export PGLOG=$PGHOST/postgres.log
          export PGDATABASE=shibuya_message_db_adapter_dev
          export PG_CONNECTION_STRING=postgresql://$(jq -rn --arg x $PGHOST '$x|@uri')/$PGDATABASE

          mkdir -p $PGHOST
          mkdir -p .dev

          if [ ! -d $PGDATA ]; then
            initdb --auth=trust --no-locale --encoding=UTF8
          fi
        '';
      };
    in
    {
      devShells.default = mkProjectShell "ghc9124";
      devShells.ghc9124 = mkProjectShell "ghc9124";
    };
}
