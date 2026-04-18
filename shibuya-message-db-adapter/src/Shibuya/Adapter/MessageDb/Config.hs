-- | Configuration for the message-db adapter.
module Shibuya.Adapter.MessageDb.Config (
    MessageDbAdapterConfig (..),
    CategoryStream (..),
    BatchSize (..),
    PollInterval (..),
    DrainTimeout (..),
    CheckpointInterval (..),
    SubscriptionName,
    defaultConfig,
)
where

import Data.Text (Text)
import Data.Time.Clock (NominalDiffTime)
import GHC.Generics (Generic)
import MessageDb.CheckpointStore.Effectful (SubscriptionName)

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
shutdown.
-}
newtype DrainTimeout = DrainTimeout {unDrainTimeout :: NominalDiffTime}
    deriving stock (Eq, Show, Generic)

{- | How often the background persister flushes the contiguous-prefix
checkpoint to the durable store.

Balances durability against write amplification: at 100 msgs/s a 1 s
interval bounds worst-case replay after a crash to ~100 messages,
which is cheap given message-db's indexed @get_category_messages@.
Aggressive flushing (every message) would make the store a bottleneck;
long intervals force large replays. 1 s is the default.
-}
newtype CheckpointInterval = CheckpointInterval {unCheckpointInterval :: NominalDiffTime}
    deriving stock (Eq, Show, Generic)

{- | Configuration for a single message-db category polling loop.

@subscriptionName@ keys this adapter's checkpoint row in the
@checkpoints@ table. Two adapters consuming the same category must use
different subscription names, or they will overwrite each other's
progress.
-}
data MessageDbAdapterConfig = MessageDbAdapterConfig
    { category :: !CategoryStream
    , subscriptionName :: !SubscriptionName
    , batchSize :: !BatchSize
    , pollInterval :: !PollInterval
    , drainTimeout :: !DrainTimeout
    , checkpointInterval :: !CheckpointInterval
    }
    deriving stock (Eq, Show, Generic)

{- | Default adapter configuration for the given category and
subscription name.

Defaults:

* @batchSize@: 100 messages
* @pollInterval@: 500 ms (0.5 s) between empty polls
* @drainTimeout@: 10 s to wait for in-flight work on shutdown
* @checkpointInterval@: 1 s between background checkpoint flushes
-}
defaultConfig :: CategoryStream -> SubscriptionName -> MessageDbAdapterConfig
defaultConfig cat sub =
    MessageDbAdapterConfig
        { category = cat
        , subscriptionName = sub
        , batchSize = BatchSize 100
        , pollInterval = PollInterval 0.5
        , drainTimeout = DrainTimeout 10
        , checkpointInterval = CheckpointInterval 1
        }
