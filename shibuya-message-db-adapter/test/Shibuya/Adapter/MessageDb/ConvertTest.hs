-- | Unit tests for the pure @Message -> Envelope@ conversion.
module Shibuya.Adapter.MessageDb.ConvertTest (tests) where

import Data.Aeson qualified as Aeson
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Time.Calendar (fromGregorian)
import Data.Time.Clock (UTCTime (..))
import Data.UUID qualified as UUID
import MessageDb.Message qualified as Mdb
import MessageDb.Message.Stream qualified as S
import Shibuya.Adapter.MessageDb.Convert (extractTraceContext, messageToEnvelope)
import Shibuya.Core.Types (
    Cursor (..),
    Envelope (..),
    MessageId (..),
 )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertEqual, testCase, (@?=))

-- | A deterministic UUID used across test fixtures.
fixtureUuid :: UUID.UUID
fixtureUuid =
    case UUID.fromString "00000000-0000-0000-0000-000000000001" of
        Just u -> u
        Nothing -> error "fixtureUuid: hard-coded UUID failed to parse"

fixtureTime :: UTCTime
fixtureTime = UTCTime (fromGregorian 2026 4 18) 0

fixtureStream :: S.Stream
fixtureStream =
    case S.parseEither "orders-1" of
        Right s -> s
        Left e -> error $ "fixtureStream: " <> show e

mkMessage :: Mdb.MessageMetadata -> Mdb.Message
mkMessage metadata =
    Mdb.Message
        { messageId = Mdb.MessageId fixtureUuid
        , stream = fixtureStream
        , messageType = Mdb.MessageType "OrderPlaced"
        , position = Mdb.MessagePosition 0
        , globalPosition = Mdb.GlobalPosition 42
        , messageData = Mdb.MessageData (Aeson.Object KeyMap.empty)
        , messageMetadata = metadata
        , time = fixtureTime
        }

emptyMetadata :: Mdb.MessageMetadata
emptyMetadata = Mdb.MessageMetadata (Aeson.Object KeyMap.empty)

traceparentOnly :: Mdb.MessageMetadata
traceparentOnly =
    Mdb.MessageMetadata $
        Aeson.Object $
            KeyMap.fromList
                [ ("traceparent", Aeson.String "00-abc-def-01")
                ]

traceparentAndState :: Mdb.MessageMetadata
traceparentAndState =
    Mdb.MessageMetadata $
        Aeson.Object $
            KeyMap.fromList
                [ ("traceparent", Aeson.String "00-abc-def-01")
                , ("tracestate", Aeson.String "vendor=state")
                ]

nonObjectMetadata :: Mdb.MessageMetadata
nonObjectMetadata = Mdb.MessageMetadata (Aeson.String "oops")

tests :: TestTree
tests =
    testGroup
        "Convert"
        [ testCase "empty metadata yields Nothing traceContext" $ do
            let env = messageToEnvelope (mkMessage emptyMetadata)
            env.traceContext @?= Nothing
            env.messageId @?= MessageId (UUID.toText fixtureUuid)
            env.cursor @?= Just (CursorInt 42)
            env.partition @?= Nothing
            env.enqueuedAt @?= Just fixtureTime
        , testCase "only traceparent yields one header" $ do
            let md = traceparentOnly
            extractTraceContext md
                @?= Just [("traceparent", "00-abc-def-01")]
        , testCase "traceparent + tracestate yields two headers in order" $ do
            let md = traceparentAndState
            assertEqual
                "header order: traceparent first, tracestate second"
                (Just [("traceparent", "00-abc-def-01"), ("tracestate", "vendor=state")])
                (extractTraceContext md)
        , testCase "non-object metadata yields Nothing and does not throw" $ do
            extractTraceContext nonObjectMetadata @?= Nothing
        ]
