{- | Unit tests for the EP-3 retry buffer embedded in 'InflightState'.

Exercises the STM primitives directly, without running the full
adapter: enqueue via 'scheduleRetry', observe buffer size, drain
through 'popRetryHeadToChannel' and 'drainRetryChannel', and assert
overflow behavior at the configured capacity.

The retry fiber itself is covered by the EP-3 integration tests
against an ephemeral-pg harness.
-}
module Shibuya.Adapter.MessageDb.RetryBufferTest (tests) where

import Control.Concurrent.STM (atomically)
import Data.Aeson qualified as Aeson
import Data.Time.Calendar (fromGregorian)
import Data.Time.Clock (UTCTime (..), secondsToDiffTime)
import Data.UUID qualified as UUID
import MessageDb.Message qualified as Mdb
import MessageDb.Message.Stream qualified as Mdb.Stream
import Shibuya.Adapter.MessageDb.Internal.InflightState (
    RetryEntry (..),
    drainRetryChannel,
    newInflightState,
    popRetryHeadToChannel,
    recordIngested,
    retryBufferSize,
    scheduleRetry,
    tryPeekRetry,
 )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertEqual, testCase, (@?=))

-- * Fixtures

fixedNow :: UTCTime
fixedNow = UTCTime (fromGregorian 2026 4 18) (secondsToDiffTime 0)

-- | Build a stub message whose only meaningful field is @globalPosition@.
stubMessage :: Int -> Mdb.Message
stubMessage n =
    Mdb.Message
        { messageId = Mdb.MessageId (UUID.fromWords 0 0 0 (fromIntegral n))
        , stream =
            case Mdb.Stream.parseEither "orders-retrydemo-1" of
                Right s -> s
                Left err -> error ("stubMessage: " <> show err)
        , messageType = Mdb.MessageType "OrderPlaced"
        , position = Mdb.MessagePosition 0
        , globalPosition = Mdb.GlobalPosition (fromIntegral n)
        , messageData = Mdb.MessageData (Aeson.object [])
        , messageMetadata = Mdb.MessageMetadata (Aeson.object [])
        , time = fixedNow
        }

gp :: Int -> Mdb.GlobalPosition
gp = Mdb.GlobalPosition . fromIntegral

-- * Tests

tests :: TestTree
tests =
    testGroup
        "RetryBuffer"
        [ testCase "scheduleRetry on a fresh state enqueues and bumps size" $ do
            st <- newInflightState 4 (gp 0)
            atomically $ recordIngested st (gp 1)
            ok <- atomically $ scheduleRetry st (gp 1) 0.05 fixedNow (stubMessage 1)
            ok @?= True
            sz <- atomically $ retryBufferSize st
            sz @?= 1
        , testCase "scheduleRetry returns False when buffer is full" $ do
            st <- newInflightState 2 (gp 0)
            -- Fill to capacity.
            atomically $ do
                recordIngested st (gp 1)
                recordIngested st (gp 2)
                recordIngested st (gp 3)
            ok1 <- atomically $ scheduleRetry st (gp 1) 0.05 fixedNow (stubMessage 1)
            ok2 <- atomically $ scheduleRetry st (gp 2) 0.05 fixedNow (stubMessage 2)
            ok3 <- atomically $ scheduleRetry st (gp 3) 0.05 fixedNow (stubMessage 3)
            assertEqual "first two enqueues succeed" [True, True] [ok1, ok2]
            ok3 @?= False
            sz <- atomically $ retryBufferSize st
            sz @?= 2
        , testCase "popRetryHeadToChannel drains one entry and drainRetryChannel flushes it" $ do
            st <- newInflightState 4 (gp 0)
            atomically $ do
                recordIngested st (gp 1)
                recordIngested st (gp 2)
            _ <- atomically $ scheduleRetry st (gp 1) 0.05 fixedNow (stubMessage 1)
            _ <- atomically $ scheduleRetry st (gp 2) 0.05 fixedNow (stubMessage 2)
            atomically $ popRetryHeadToChannel st
            atomically $ popRetryHeadToChannel st
            sz <- atomically $ retryBufferSize st
            sz @?= 0
            drained <- atomically $ drainRetryChannel st
            length drained @?= 2
            let positions =
                    [fromIntegral (Mdb.unGlobalPosition msg.globalPosition) :: Int | msg <- drained]
            assertEqual "drained in FIFO order" [1, 2] positions
        , testCase "tryPeekRetry sees the earliest scheduled entry" $ do
            st <- newInflightState 4 (gp 0)
            atomically $ do
                recordIngested st (gp 7)
                recordIngested st (gp 8)
            _ <- atomically $ scheduleRetry st (gp 7) 0.1 fixedNow (stubMessage 7)
            _ <- atomically $ scheduleRetry st (gp 8) 0.5 fixedNow (stubMessage 8)
            mHead <- atomically $ tryPeekRetry st
            case mHead of
                Nothing -> assertEqual "buffer should not be empty" True False
                Just RetryEntry{retryPosition = p} ->
                    assertEqual
                        "peek returns the first enqueued entry"
                        (gp 7)
                        p
        , testCase "empty drainRetryChannel returns []" $ do
            st <- newInflightState 4 (gp 0)
            drained <- atomically $ drainRetryChannel st
            drained @?= []
        ]
