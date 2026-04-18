let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/ad9960dd3dd3b33eadd45f17bcf430b0e1ec13bc/package.dhall
        sha256:83aa1432e98db5da81afde4ab2057dcab7ce4b2e883d0bc7f16c7d25b917dd0c

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
