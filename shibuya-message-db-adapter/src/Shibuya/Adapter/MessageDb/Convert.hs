{- | Pure conversion from @MessageDb.Message@ to @Shibuya.Envelope@.

The hot-path mapping is deliberately isolated here so that it can be
exercised without standing up Postgres or any effect interpreters.
-}
module Shibuya.Adapter.MessageDb.Convert (
    messageToEnvelope,
    extractTraceContext,
)
where

import Data.Aeson qualified as Aeson
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Text.Encoding qualified as TE
import Data.UUID qualified as UUID
import MessageDb.Message qualified as Mdb
import Shibuya.Core.Types (
    Cursor (..),
    Envelope (..),
    MessageId (..),
    TraceHeaders,
 )

{- | Convert a message-db @Message@ to a Shibuya @Envelope@ wrapping the same
message as payload.

* @messageId@ is the textual form of the message-db UUID.
* @cursor@ is the 'CursorInt' of the message-db @globalPosition@, so
  downstream consumers can checkpoint using a monotonically-increasing
  store-wide ordinal.
* @partition@ is @Nothing@: message-db has no native partitioning at this
  layer; downstream adapters (consumer groups in EP-4) can set it.
* @enqueuedAt@ is the message-db @time@ (UTC).
* @traceContext@ extracts W3C @traceparent@/@tracestate@ from the
  message's metadata JSON when present.
* @payload@ is the original @MessageDb.Message@ so handlers keep access
  to stream name, message type, data, and metadata.
-}
messageToEnvelope :: Mdb.Message -> Envelope Mdb.Message
messageToEnvelope m =
    Envelope
        { messageId =
            MessageId (UUID.toText (Mdb.unMessageId m.messageId))
        , cursor =
            Just (CursorInt (fromIntegral (Mdb.unGlobalPosition m.globalPosition)))
        , partition = Nothing
        , enqueuedAt = Just m.time
        , traceContext = extractTraceContext m.messageMetadata
        , payload = m
        }

{- | Extract W3C trace context headers from message-db metadata.

Looks for JSON object keys @traceparent@ and optionally @tracestate@.
Returns @Nothing@ unless @traceparent@ is present (per W3C, @tracestate@
without @traceparent@ is not a valid context). Metadata that is not a
JSON object (null, string, array, etc.) yields @Nothing@ without
throwing.
-}
extractTraceContext :: Mdb.MessageMetadata -> Maybe TraceHeaders
extractTraceContext md =
    case Mdb.unMessageMetadata md of
        Aeson.Object obj -> do
            tp <- lookupString obj "traceparent"
            let ts = lookupString obj "tracestate"
            pure $ ("traceparent", tp) : maybe [] (\v -> [("tracestate", v)]) ts
        _ -> Nothing
  where
    lookupString obj k =
        case KeyMap.lookup (Key.fromText k) obj of
            Just (Aeson.String t) -> Just (TE.encodeUtf8 t)
            _ -> Nothing
