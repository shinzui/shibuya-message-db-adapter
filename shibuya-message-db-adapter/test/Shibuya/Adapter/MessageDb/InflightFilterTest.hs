{- | Unit tests for consumer-group filter coordination with
'InflightState'.

Proves that filtered-out messages recorded via 'recordFilteredCompleted'
do not block the contiguous-prefix checkpoint from advancing once the
belonging messages in the same batch are acked.
-}
module Shibuya.Adapter.MessageDb.InflightFilterTest (tests) where

import Control.Concurrent.STM (atomically)
import MessageDb.Message qualified as Mdb
import Shibuya.Adapter.MessageDb.Internal (recordFilteredCompleted)
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
        "inflight-filter"
        [ testCase "filtered messages advance the contiguous prefix" $ do
            s <- newInflightState 100 (gp 0)
            atomically $ do
                -- Filtered positions 1, 3, 5 are recorded as
                -- ingested-and-immediately-complete.
                mapM_ (recordFilteredCompleted s . gp) [1, 3, 5]
                -- Belonging positions 2 and 4 are ingested but not yet acked.
                mapM_ (recordIngested s . gp) [2, 4]

            -- Cannot advance: position 2 is still inflight.
            r0 <- atomically (advanceCheckpointTo s)
            r0 @?= Just (gp 1)

            -- Complete position 2 — 1, 2, 3 are now a contiguous run of completed
            -- entries, so the checkpoint jumps to 3.
            atomically (recordAckResult s (gp 2) AckComplete)
            r1 <- atomically (advanceCheckpointTo s)
            r1 @?= Just (gp 3)

            -- Complete position 4 — 4 and 5 become contiguous with the saved
            -- prefix; checkpoint jumps to 5.
            atomically (recordAckResult s (gp 4) AckComplete)
            r2 <- atomically (advanceCheckpointTo s)
            r2 @?= Just (gp 5)

            -- Idempotent: no further advancement.
            r3 <- atomically (advanceCheckpointTo s)
            r3 @?= Nothing
        ]
