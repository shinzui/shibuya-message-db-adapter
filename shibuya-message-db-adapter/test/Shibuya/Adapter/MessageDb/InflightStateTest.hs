-- | Unit tests for the contiguous-prefix @InflightState@ ledger.
module Shibuya.Adapter.MessageDb.InflightStateTest (tests) where

import Control.Concurrent.STM (atomically)
import MessageDb.Message qualified as Mdb
import Shibuya.Adapter.MessageDb.Internal.InflightState (
    AckOutcome (..),
    advanceCheckpointTo,
    newInflightState,
    recordAckResult,
    recordIngested,
 )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

gp :: Int -> Mdb.GlobalPosition
gp = Mdb.GlobalPosition . fromIntegral

tests :: TestTree
tests =
    testGroup
        "InflightState"
        [ testCase "empty state returns Nothing" $ do
            s <- newInflightState 100 (gp 0)
            r <- atomically (advanceCheckpointTo s)
            r @?= Nothing
        , testCase "single complete advances, second call returns Nothing" $ do
            s <- newInflightState 100 (gp 0)
            atomically $ do
                recordIngested s (gp 1)
                recordAckResult s (gp 1) AckComplete
            r1 <- atomically (advanceCheckpointTo s)
            r1 @?= Just (gp 1)
            r2 <- atomically (advanceCheckpointTo s)
            r2 @?= Nothing
        , testCase "AckRetry blocks advancement past retry position" $ do
            s <- newInflightState 100 (gp 0)
            atomically $ do
                mapM_ (recordIngested s . gp) [1, 2, 3]
                recordAckResult s (gp 1) AckComplete
                recordAckResult s (gp 2) AckRetry
                recordAckResult s (gp 3) AckComplete
            r <- atomically (advanceCheckpointTo s)
            r @?= Just (gp 1)
        , testCase "interleaved: pending middle position blocks advancement" $ do
            s <- newInflightState 100 (gp 0)
            atomically $ do
                mapM_ (recordIngested s . gp) [1, 2, 3, 4, 5]
                recordAckResult s (gp 1) AckComplete
                recordAckResult s (gp 2) AckComplete
                recordAckResult s (gp 4) AckComplete
                recordAckResult s (gp 5) AckComplete
            r <- atomically (advanceCheckpointTo s)
            r @?= Just (gp 2)
        , testCase "retry completes: advancement unblocks" $ do
            s <- newInflightState 100 (gp 0)
            atomically $ do
                mapM_ (recordIngested s . gp) [1, 2, 3]
                recordAckResult s (gp 1) AckComplete
                recordAckResult s (gp 2) AckRetry
                recordAckResult s (gp 3) AckComplete
            r1 <- atomically (advanceCheckpointTo s)
            r1 @?= Just (gp 1)
            atomically (recordAckResult s (gp 2) AckComplete)
            r2 <- atomically (advanceCheckpointTo s)
            r2 @?= Just (gp 3)
        , testCase "out-of-order complete with unknown preceding positions does not advance" $ do
            s <- newInflightState 100 (gp 0)
            atomically $ do
                recordIngested s (gp 3)
                recordAckResult s (gp 3) AckComplete
            r <- atomically (advanceCheckpointTo s)
            r @?= Nothing
        ]
