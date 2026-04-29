let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/9b1d6eea8027ae57576cf0712c0b9167fccbc1a9/package.dhall
        sha256:a19f5dd9181db28ba7a6a1b77b5ab8715e81aba3e2a8f296f40973003a0b4412

let emptyRuntime = { deployable = False, exposesApi = False }

let emptyDeps = [] : List Schema.Dependency

let emptyDocs = [] : List Schema.DocRef.Type

let emptyConfig = [] : List Schema.ConfigItem.Type

in  Schema.Project::{ project =
      Schema.ProjectIdentity::{ name = "shibuya-message-db-adapter"
      , namespace = "shinzui"
      , type = Schema.PackageType.Library
      , description = Some
          "message-db adapter for the Shibuya queue processing framework"
      , language = Schema.Language.Haskell
      , lifecycle = Schema.Lifecycle.Active
      , domains = [ "concurrency", "queue-processing", "event-sourcing" ]
      , owners = [ "shinzui" ]
      }
    , repos =
      [ Schema.Repo::{ name = "shibuya-message-db-adapter"
        , github = Some "shinzui/shibuya-message-db-adapter"
        , localPath = Some "."
        }
      ]
    , packages =
      [ Schema.Package::{ name = "shibuya-message-db-adapter"
        , type = Schema.PackageType.Library
        , language = Schema.Language.Haskell
        , path = Some "shibuya-message-db-adapter"
        , description = Some
            "Polling adapter for message-db: converts MessageDb.Message to Shibuya.Envelope and streams into Shibuya handlers"
        , runtime = emptyRuntime
        , dependencies =
          [ Schema.Dependency.ByName "effectful/effectful"
          , Schema.Dependency.ByName "composewell/streamly"
          , Schema.Dependency.ByName "shinzui/shibuya"
          , Schema.Dependency.ByName "tan/message-db-hs"
          , Schema.Dependency.ByName "hasql/hasql"
          ]
        , docs = emptyDocs
        , config = emptyConfig
        }
      , Schema.Package::{ name = "shibuya-message-db-adapter-jitsurei"
        , type = Schema.PackageType.Application
        , language = Schema.Language.Haskell
        , path = Some "shibuya-message-db-adapter-jitsurei"
        , description = Some
            "Runnable examples: basic consumer, retry, dead-letter, checkpoint restart, multi-partition"
        , visibility = Schema.Visibility.Internal
        , runtime = { deployable = True, exposesApi = False }
        , dependencies = emptyDeps
        , docs = emptyDocs
        , config = emptyConfig
        }
      ]
    , dependencies =
      [ "shinzui/shibuya"
      , "effectful/effectful"
      , "composewell/streamly"
      , "tan/message-db-hs"
      , "hasql/hasql"
      ]
    , agents =
      [ Schema.AgentHint::{ role = "adapter-dev"
        , description = Some
            "message-db adapter development: polling, conversion, ack semantics"
        , includePaths =
          [ "shibuya-message-db-adapter/src/**"
          , "shibuya-message-db-adapter/test/**"
          ]
        , excludePaths =
          [ "dist-newstyle/**"
          ]
        , relatedPackages =
          [ "shibuya-message-db-adapter"
          ]
        }
      , Schema.AgentHint::{ role = "examples-dev"
        , description = Some
            "Jitsurei examples: usage patterns for the message-db adapter"
        , includePaths =
          [ "shibuya-message-db-adapter-jitsurei/**"
          ]
        , excludePaths =
          [ "dist-newstyle/**"
          ]
        , relatedPackages =
          [ "shibuya-message-db-adapter-jitsurei"
          ]
        }
      ]
    , docs =
      [ Schema.DocRef::{ key = "masterplan"
        , kind = Schema.DocKind.Reference
        , audience = Schema.DocAudience.Internal
        , description = Some "Master plan coordinating adapter ExecPlans"
        , location = Schema.DocLocation.LocalDir "docs/masterplans"
        }
      , Schema.DocRef::{ key = "plans"
        , kind = Schema.DocKind.Reference
        , audience = Schema.DocAudience.Internal
        , description = Some "Execution plans for adapter development"
        , location = Schema.DocLocation.LocalDir "docs/plans"
        }
      ]
    }
