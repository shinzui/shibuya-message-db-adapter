-- | Configuration for the message-db adapter.
module Shibuya.Adapter.MessageDb.Config (
    MessageDbAdapterConfig (..),
    CategoryStream (..),
    BatchSize (..),
    PollInterval (..),
    DrainTimeout (..),
    defaultConfig,
)
where

import Data.Text (Text)
import Data.Time.Clock (NominalDiffTime)
import GHC.Generics (Generic)

{- | The category to poll (\"orders\", \"shipments\", etc.).

Adapter-level wrapper over 'Data.Text.Text' so the config type stays
structural. Call-sites parse this into message-db's internal
@CategoryStream@ before dispatching a query.
-}
newtype CategoryStream = CategoryStream {unCategoryStream :: Text}
    deriving stock (Eq, Show, Generic)

-- | Maximum number of messages to fetch per polling round.
newtype BatchSize = BatchSize {unBatchSize :: Int}
    deriving stock (Eq, Show, Generic)

{- | How long to wait between polls when the previous poll returned no
messages.
-}
newtype PollInterval = PollInterval {unPollInterval :: NominalDiffTime}
    deriving stock (Eq, Show, Generic)

{- | How long to wait for in-flight messages to drain before forcing
shutdown. Surfaced through the adapter for symmetry with 'ShutdownConfig'
at the framework level; not yet used by the stub ack handler in EP-1.
-}
newtype DrainTimeout = DrainTimeout {unDrainTimeout :: NominalDiffTime}
    deriving stock (Eq, Show, Generic)

-- | Configuration for a single message-db category polling loop.
data MessageDbAdapterConfig = MessageDbAdapterConfig
    { category :: !CategoryStream
    , batchSize :: !BatchSize
    , pollInterval :: !PollInterval
    , drainTimeout :: !DrainTimeout
    }
    deriving stock (Eq, Show, Generic)

{- | Default adapter configuration for the given category.

Defaults:

* @batchSize@: 100 messages
* @pollInterval@: 500 ms (0.5 s) between empty polls
* @drainTimeout@: 10 s to wait for in-flight work on shutdown
-}
defaultConfig :: CategoryStream -> MessageDbAdapterConfig
defaultConfig cat =
    MessageDbAdapterConfig
        { category = cat
        , batchSize = BatchSize 100
        , pollInterval = PollInterval 0.5
        , drainTimeout = DrainTimeout 10
        }
