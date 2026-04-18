-- | Configuration for the message-db adapter.
module Shibuya.Adapter.MessageDb.Config (
    MessageDbAdapterConfig (..),
    CategoryStream (..),
    BatchSize (..),
    PollInterval (..),
    DrainTimeout (..),
    CheckpointInterval (..),
    SubscriptionName,
    DlqStrategy (..),
    MaxRetryBufferSize (..),
    defaultConfig,
)
where

import Data.Text (Text)
import Data.Time.Clock (NominalDiffTime)
import GHC.Generics (Generic)
import MessageDb.CheckpointStore.Effectful (SubscriptionName)
import MessageDb.Message.Stream (Stream)

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

{- | How to handle an 'Shibuya.Core.Ack.AckDeadLetter' decision.

message-db has no native dead-letter queue primitive, so the adapter
implements DLQs in-process. Two strategies are supported:

* 'DlqSkipAndLog' — log the reason at warning level and advance the
  checkpoint. Suitable for analytics or observability-only deployments
  where the original log has enough context to reconstruct what failed.

* 'DlqWriteToStream' — write a deterministic-id copy of the failing
  message to the named stream via @writeStreamMessage@, then advance the
  checkpoint. Suitable for production deployments that want durable
  replay material. The DLQ @messageId@ is derived from the original via
  UUIDv5 with a fixed namespace, so a crash between the DB write and the
  checkpoint advance is safely retryable — message-db rejects the
  duplicate with a unique-violation, which the adapter treats as
  success.

A future @DlqCustom@ strategy taking an @Eff es ()@ callback is a
deliberate non-goal of EP-3. Callers that need custom routing can wrap
the adapter in their own layer.
-}
data DlqStrategy
    = DlqSkipAndLog
    | DlqWriteToStream !Stream
    deriving stock (Eq, Show, Generic)

{- | Upper bound on the number of retries held in the in-process retry
buffer.

When a handler returns 'Shibuya.Core.Ack.AckRetry' and the buffer is
already at capacity, the adapter downgrades the decision to
@AckDeadLetter MaxRetriesExceeded@ rather than applying back-pressure
to the poll loop. Back-pressure would let one misbehaving handler stall
every sibling message on the subscription; downgrading uses Shibuya's
existing taxonomy and keeps progress flowing.
-}
newtype MaxRetryBufferSize = MaxRetryBufferSize {unMaxRetryBufferSize :: Int}
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
    , dlqStrategy :: !DlqStrategy
    , maxRetryBufferSize :: !MaxRetryBufferSize
    }
    deriving stock (Eq, Show, Generic)

{- | Default adapter configuration for the given category and
subscription name.

Defaults:

* @batchSize@: 100 messages
* @pollInterval@: 500 ms (0.5 s) between empty polls
* @drainTimeout@: 10 s to wait for in-flight work on shutdown
* @checkpointInterval@: 1 s between background checkpoint flushes
* @dlqStrategy@: 'DlqSkipAndLog'
* @maxRetryBufferSize@: 1000 retries held in memory before overflow
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
        , dlqStrategy = DlqSkipAndLog
        , maxRetryBufferSize = MaxRetryBufferSize 1000
        }
