{- | Dead-letter-queue helpers for the message-db adapter.

message-db has no native DLQ primitive, so a dead-letter destination is
simply another stream the adapter writes to via @writeStreamMessage@.
This module builds the 'Mdb.NewMessage' value that gets written, and
wraps the write in error-handling that recognizes the
idempotency-duplicate case (SQLSTATE 23505).

The DLQ @messageId@ is a deterministic UUIDv5 of the original message's
id under a fixed namespace. A crash between the DB write and the ack
ledger update therefore replays safely: the next attempt produces the
same @(stream_name, message_id)@, message-db rejects it with a unique
violation, and the adapter treats that as success.

Internal module — not part of the public API.
-}
module Shibuya.Adapter.MessageDb.Internal.Dlq (
    DlqWriteError (..),
    dlqMessageId,
    mkDlqMessage,
    buildDlqMetadata,
    tryWriteDlq,
    dlqNamespace,
)
where

import Data.Aeson qualified as Aeson
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KeyMap
import Data.ByteString.Lazy qualified as BSL
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Time.Clock (UTCTime)
import Data.UUID (UUID)
import Data.UUID qualified as UUID
import Data.UUID.V5 qualified as UUIDv5
import Effectful (Eff, (:>))
import Effectful.Error.Static (Error, runErrorNoCallStack, tryError)
import Effectful.Hasql (SessionError)
import Hasql.Errors (
    ServerError (..),
    SessionError (..),
    StatementError (..),
 )
import MessageDb.Db.Errors (WrongExpectedVersion)
import MessageDb.Effectful (MessageDb, writeStreamMessage)
import MessageDb.Message qualified as Mdb
import MessageDb.Message.Stream qualified as Mdb.Stream
import Shibuya.Core.Ack (DeadLetterReason (..))

{- | Outcome of a DLQ write that did not succeed outright.

'DuplicateDlqEntry' is the expected idempotent-replay case — the
adapter crashed after a prior DLQ write succeeded but before it could
record the ack, and the second attempt hit a unique-violation on
@messages.id@. Callers should treat this as success.

'OtherDlqError' wraps any other failure (connection drop, permission
error, etc.). Callers should log and proceed — dropping a DLQ write
under duress is preferable to stalling the entire subscription on one
misbehaving message.
-}
data DlqWriteError
    = DuplicateDlqEntry
    | OtherDlqError !Text
    deriving stock (Eq, Show)

{- | Fixed UUIDv5 namespace for derived DLQ message ids.

Chosen as an arbitrary constant. Any fixed UUID works as long as it
does not change between adapter versions — the only invariant that
matters is that @dlqMessageId@ be deterministic on its input.
-}
dlqNamespace :: UUID
dlqNamespace = UUID.fromWords 0x5486e5b1 0x000a 0x5ec0 0x73686264

{- | Derive a deterministic DLQ @MessageId@ from the original message's
id.

Feeds the 16 bytes of the original UUID plus the ASCII suffix @"-dlq"@
into a UUIDv5 hash keyed on 'dlqNamespace'. Two calls with the same
input produce the same output; two calls with different inputs produce
different outputs with cryptographic probability.
-}
dlqMessageId :: Mdb.MessageId -> Mdb.MessageId
dlqMessageId original =
    let originalBytes = BSL.unpack (UUID.toByteString (Mdb.unMessageId original))
        suffix = [fromIntegral (fromEnum c) | c <- ("-dlq" :: String)]
     in Mdb.MessageId (UUIDv5.generateNamed dlqNamespace (originalBytes <> suffix))

{- | Build the 'Mdb.NewMessage' to write to the DLQ stream.

Preserves the original payload verbatim in @messageData@ so consumers
of the DLQ can reconstruct what failed. Replaces the metadata with a
fresh DLQ-shaped object — see 'buildDlqMetadata'.

@expectedPosition = Nothing@ because we do not enforce stream-version
ordering on the DLQ: multiple producers writing to the same DLQ stream
should not collide on version checks. Idempotency is achieved by the
deterministic @messageId@ alone.
-}
mkDlqMessage ::
    -- | target DLQ stream
    Mdb.Stream.Stream ->
    -- | original failed message
    Mdb.Message ->
    DeadLetterReason ->
    -- | current time
    UTCTime ->
    Mdb.NewMessage
mkDlqMessage target orig reason now =
    Mdb.NewMessage
        { messageId = dlqMessageId orig.messageId
        , stream = target
        , messageType = Mdb.MessageType "$DeadLetter"
        , messageData = orig.messageData
        , messageMetadata = buildDlqMetadata orig reason now
        , expectedPosition = Nothing
        }

{- | Build the metadata object for a DLQ message.

Fields:

* @correlation@ — the original message's @messageMetadata.correlation@
  if present (to preserve workflow tracking), otherwise the original
  message's id. This makes the DLQ entry joinable back to the source
  message's trace.

* @causation@ — the original message's id. This identifies the single
  message whose failure caused the DLQ entry.

* @originalStream@ — the text form of the original message's stream
  name, so DLQ consumers can recognize which category the failure came
  from without re-parsing the payload.

* @deadLetterReason@ — a tagged string form of the Shibuya
  'DeadLetterReason', suitable for indexing or log correlation.

* @deadLetteredAt@ — the timestamp when the DLQ write was attempted.
-}
buildDlqMetadata ::
    Mdb.Message ->
    DeadLetterReason ->
    UTCTime ->
    Mdb.MessageMetadata
buildDlqMetadata orig reason now =
    Mdb.MessageMetadata . Aeson.Object $
        KeyMap.fromList
            [ (Key.fromText "correlation", correlationVal)
            , (Key.fromText "causation", originalIdVal)
            , (Key.fromText "originalStream", Aeson.String (Mdb.Stream.toText orig.stream))
            , (Key.fromText "deadLetterReason", Aeson.String (renderReason reason))
            , (Key.fromText "deadLetteredAt", Aeson.toJSON now)
            ]
  where
    originalIdVal = Aeson.String (UUID.toText (Mdb.unMessageId orig.messageId))
    correlationVal =
        case Mdb.unMessageMetadata orig.messageMetadata of
            Aeson.Object obj ->
                case KeyMap.lookup (Key.fromText "correlation") obj of
                    Just v -> v
                    Nothing -> originalIdVal
            _ -> originalIdVal

renderReason :: DeadLetterReason -> Text
renderReason = \case
    PoisonPill t -> "poison_pill:" <> t
    InvalidPayload t -> "invalid_payload:" <> t
    MaxRetriesExceeded -> "max_retries_exceeded"

{- | Attempt to write a DLQ message, recognizing the idempotent-duplicate
case as success.

Catches both errors that 'writeStreamMessage' can surface:

* 'WrongExpectedVersion' — should not fire since we pass
  @expectedPosition = Nothing@, but caught defensively and reported as
  'OtherDlqError' if it does.

* Hasql 'SessionError' — if it carries SQLSTATE 23505
  (@unique_violation@), the DLQ row already exists for this
  deterministic id, which is our idempotent-success signal; reported
  as 'DuplicateDlqEntry'. All other SessionErrors are reported as
  'OtherDlqError' so the caller can log-and-advance rather than
  crash-and-stall.
-}
tryWriteDlq ::
    (MessageDb :> es, Error SessionError :> es) =>
    Mdb.NewMessage ->
    Eff es (Either DlqWriteError Mdb.MessagePosition)
tryWriteDlq nm = do
    wevR <- runErrorNoCallStack @WrongExpectedVersion $ do
        sessR <- tryError @SessionError (writeStreamMessage nm)
        pure sessR
    pure $ case wevR of
        Left _wev ->
            Left (OtherDlqError "unexpected WrongExpectedVersion on DLQ write")
        Right (Right pos) -> Right pos
        Right (Left (_cs, err))
            | isUniqueViolation err -> Left DuplicateDlqEntry
            | otherwise -> Left (OtherDlqError (Text.pack (show err)))

{- | True iff the session error wraps a server error with SQLSTATE
23505 (@unique_violation@).

message-db's @write_message@ inserts into @message_store.messages@
with a primary-key constraint on @id@, so a duplicate message UUID
surfaces as a 23505. Any other failure (connection, permission,
syntax) produces a different SQLSTATE or a non-server error.
-}
isUniqueViolation :: SessionError -> Bool
isUniqueViolation = \case
    StatementSessionError _ _ _ _ _ (ServerStatementError (ServerError code _ _ _ _)) ->
        code == "23505"
    _ -> False
