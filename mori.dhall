let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/027403783777cbce0e87eb660a0b3d8119ebe8d2/package.dhall
        sha256:d29ca03286afa92b7589d09b7a6d98ad8e39d11b255a4b8751f3327b0722fba3

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
      [ Schema.DocRef::{ key = "user-guide"
        , kind = Schema.DocKind.Guide
        , audience = Schema.DocAudience.User
        , description = Some
            "User guide overview: what the adapter does and where to start"
        , location = Schema.DocLocation.LocalFile "docs/user/README.md"
        }
      , Schema.DocRef::{ key = "getting-started"
        , kind = Schema.DocKind.Guide
        , audience = Schema.DocAudience.User
        , description = Some
            "Walkthrough: wire the adapter into a Haskell program that consumes one category"
        , location = Schema.DocLocation.LocalFile "docs/user/getting-started.md"
        }
      , Schema.DocRef::{ key = "configuration"
        , kind = Schema.DocKind.Reference
        , audience = Schema.DocAudience.User
        , description = Some
            "MessageDbAdapterConfig field reference and defaults"
        , location = Schema.DocLocation.LocalFile "docs/user/configuration.md"
        }
      , Schema.DocRef::{ key = "handler-decisions"
        , kind = Schema.DocKind.Guide
        , audience = Schema.DocAudience.User
        , description = Some
            "AckDecision semantics: Ack, Retry, DeadLetter, and Skip"
        , location = Schema.DocLocation.LocalFile "docs/user/handler-decisions.md"
        }
      , Schema.DocRef::{ key = "checkpointing"
        , kind = Schema.DocKind.Guide
        , audience = Schema.DocAudience.User
        , description = Some
            "At-least-once delivery, checkpoint storage, and restart behavior"
        , location = Schema.DocLocation.LocalFile "docs/user/checkpointing.md"
        }
      , Schema.DocRef::{ key = "consumer-groups"
        , kind = Schema.DocKind.Guide
        , audience = Schema.DocAudience.User
        , description = Some
            "Splitting a category across N cooperating adapter processes via partition routing"
        , location = Schema.DocLocation.LocalFile "docs/user/consumer-groups.md"
        }
      , Schema.DocRef::{ key = "examples"
        , kind = Schema.DocKind.Guide
        , audience = Schema.DocAudience.User
        , description = Some
            "Tour of the jitsurei runnable examples (basic, retry, dead-letter, checkpoint, partitioned)"
        , location = Schema.DocLocation.LocalFile "docs/user/examples.md"
        }
      ]
    , okfBundles =
      [ Schema.OkfBundle::{ name = "capabilities"
        , path = "docs/capabilities"
        , profile = Some "docs/capabilities/profile.dhall"
        , okfVersion = "0.2"
        , description = Some
            "What shibuya-message-db-adapter provides today, one concept per capability, with evidence"
        }
      ]
    }
