{- | Unit tests for the EP-3 dead-letter-queue helpers.

Exercises the pure pieces of 'Shibuya.Adapter.MessageDb.Internal.Dlq':
deterministic @messageId@ derivation and DLQ metadata construction. The
idempotent DB-side write is covered by the EP-3 integration tests.
-}
module Shibuya.Adapter.MessageDb.DlqTest (tests) where

import Data.Aeson (Value (..))
import Data.Aeson qualified as Aeson
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Time.Calendar (fromGregorian)
import Data.Time.Clock (UTCTime (..), secondsToDiffTime)
import Data.UUID (UUID)
import Data.UUID qualified as UUID
import MessageDb.Message qualified as Mdb
import MessageDb.Message.Stream qualified as Mdb.Stream
import Shibuya.Adapter.MessageDb.Internal.Dlq (
    buildDlqMetadata,
    dlqMessageId,
    mkDlqMessage,
 )
import Shibuya.Core.Ack (DeadLetterReason (..))
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

-- * Fixtures

fixedNow :: UTCTime
fixedNow = UTCTime (fromGregorian 2026 4 18) (secondsToDiffTime 3600)

sampleUuid :: UUID
sampleUuid = UUID.fromWords 0x11111111 0x2222 0x3333 0x44444444

otherUuid :: UUID
otherUuid = UUID.fromWords 0xdeadbeef 0xcafe 0xbabe 0xfeedface

origStream :: Mdb.Stream.Stream
origStream = case Mdb.Stream.parseEither "orders-42" of
    Right s -> s
    Left err -> error ("origStream: " <> show err)

dlqStream :: Mdb.Stream.Stream
dlqStream = case Mdb.Stream.parseEither "orders.dlq" of
    Right s -> s
    Left err -> error ("dlqStream: " <> show err)

messageWithMetadata :: Aeson.Value -> Mdb.Message
messageWithMetadata md =
    Mdb.Message
        { messageId = Mdb.MessageId sampleUuid
        , stream = origStream
        , messageType = Mdb.MessageType "OrderPlaced"
        , position = Mdb.MessagePosition 0
        , globalPosition = Mdb.GlobalPosition 42
        , messageData = Mdb.MessageData (Aeson.object [(Key.fromText "n", Aeson.Number 42)])
        , messageMetadata = Mdb.MessageMetadata md
        , time = fixedNow
        }

-- * Tests

tests :: TestTree
tests =
    testGroup
        "Dlq"
        [ testCase "dlqMessageId is deterministic for the same input" $ do
            let a = dlqMessageId (Mdb.MessageId sampleUuid)
                b = dlqMessageId (Mdb.MessageId sampleUuid)
            a @?= b
        , testCase "dlqMessageId differs from the input id" $ do
            let derived = dlqMessageId (Mdb.MessageId sampleUuid)
            assertBool
                "derived DLQ id should differ from original"
                (Mdb.unMessageId derived /= sampleUuid)
        , testCase "dlqMessageId is distinct for distinct inputs" $ do
            let a = dlqMessageId (Mdb.MessageId sampleUuid)
                b = dlqMessageId (Mdb.MessageId otherUuid)
            assertBool "distinct inputs produce distinct DLQ ids" (a /= b)
        , testCase "buildDlqMetadata sets correlation to original correlation when present" $ do
            let originalCorrelation = Aeson.String "workflow-xyz"
                inputMd =
                    Aeson.object
                        [(Key.fromText "correlation", originalCorrelation)]
                dlqMd = buildDlqMetadata (messageWithMetadata inputMd) (PoisonPill "bad") fixedNow
                obj = unpackObject (Mdb.unMessageMetadata dlqMd)
            assertEqual
                "correlation preserved from original metadata"
                (Just originalCorrelation)
                (KeyMap.lookup (Key.fromText "correlation") obj)
        , testCase "buildDlqMetadata falls back to original id for correlation when absent" $ do
            let dlqMd = buildDlqMetadata (messageWithMetadata (Aeson.object [])) (PoisonPill "bad") fixedNow
                obj = unpackObject (Mdb.unMessageMetadata dlqMd)
                expected = Aeson.String (UUID.toText sampleUuid)
            assertEqual
                "correlation falls back to original id"
                (Just expected)
                (KeyMap.lookup (Key.fromText "correlation") obj)
        , testCase "buildDlqMetadata sets causation to original id" $ do
            let dlqMd = buildDlqMetadata (messageWithMetadata (Aeson.object [])) (PoisonPill "bad") fixedNow
                obj = unpackObject (Mdb.unMessageMetadata dlqMd)
                expected = Aeson.String (UUID.toText sampleUuid)
            assertEqual
                "causation = original id"
                (Just expected)
                (KeyMap.lookup (Key.fromText "causation") obj)
        , testCase "buildDlqMetadata renders MaxRetriesExceeded reason" $ do
            let dlqMd = buildDlqMetadata (messageWithMetadata (Aeson.object [])) MaxRetriesExceeded fixedNow
                obj = unpackObject (Mdb.unMessageMetadata dlqMd)
            assertEqual
                "reason text"
                (Just (Aeson.String "max_retries_exceeded"))
                (KeyMap.lookup (Key.fromText "deadLetterReason") obj)
        , testCase "buildDlqMetadata includes originalStream" $ do
            let dlqMd = buildDlqMetadata (messageWithMetadata (Aeson.object [])) (PoisonPill "bad") fixedNow
                obj = unpackObject (Mdb.unMessageMetadata dlqMd)
            assertEqual
                "originalStream preserved"
                (Just (Aeson.String (Mdb.Stream.toText origStream)))
                (KeyMap.lookup (Key.fromText "originalStream") obj)
        , testCase "mkDlqMessage uses deterministic id, target stream, DeadLetter type, preserves data" $ do
            let orig = messageWithMetadata (Aeson.object [])
                newMsg = mkDlqMessage dlqStream orig (PoisonPill "bad") fixedNow
            newMsg.messageId @?= dlqMessageId orig.messageId
            newMsg.stream @?= dlqStream
            newMsg.messageType @?= Mdb.MessageType "$DeadLetter"
            newMsg.messageData @?= orig.messageData
            newMsg.expectedPosition @?= Nothing
        ]

-- * Helpers

unpackObject :: Aeson.Value -> KeyMap.KeyMap Aeson.Value
unpackObject = \case
    Object o -> o
    other -> error ("unpackObject: expected object, got " <> show other)
