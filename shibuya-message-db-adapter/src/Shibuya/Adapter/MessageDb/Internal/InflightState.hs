{- | Contiguous-prefix bookkeeping for inflight messages.

message-db tracks consumer progress with a single @GlobalPosition@ — the
highest position a subscription has processed. Shibuya handlers, by
contrast, ack each message independently, and may return @AckRetry@ on
an arbitrary position while later positions succeed.

This module reconciles the two models. It maintains a per-position
ledger of inflight messages and their ack outcomes, and exposes
'advanceCheckpointTo', which returns the longest gap-free run of
completed positions starting just past the last saved checkpoint. A
pending @AckRetry@ at position N pins the checkpoint at N-1 until N
either completes or is abandoned — the \"contiguous-prefix\" rule.

The state is held behind an opaque 'TVar'; callers use 'STM' to compose
@recordIngested@/@recordAckResult@ with other transactional bookkeeping
(for example, signalling shutdown atomically with recording a halt).

This module has no external side effects and is fully testable in
isolation.
-}
module Shibuya.Adapter.MessageDb.Internal.InflightState (
    InflightState,
    AckOutcome (..),
    newInflightState,
    recordIngested,
    recordAckResult,
    advanceCheckpointTo,
    inflightSize,
)
where

import Control.Concurrent.STM (STM, TVar, modifyTVar', newTVarIO, readTVar, writeTVar)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import MessageDb.Message qualified as Mdb

{- | The outcome recorded for a previously-ingested position.

'AckComplete' means the position is safe to advance the checkpoint
past; 'AckRetry' pins the checkpoint until the position is re-run.
-}
data AckOutcome
    = AckComplete
    | AckRetry
    deriving stock (Eq, Show)

-- Internal, not exported.
data InflightStateS = InflightStateS
    { lastSaved :: !Mdb.GlobalPosition
    , outcomes :: !(Map Mdb.GlobalPosition (Maybe AckOutcome))
    , highestIngested :: !Mdb.GlobalPosition
    }

-- | Opaque ledger of inflight messages and their ack outcomes.
newtype InflightState = InflightState (TVar InflightStateS)

{- | Create an empty ledger seeded with @stored@ as the initial
@lastSaved@ position.

Callers pass the value returned by 'MessageDb.CheckpointStore.Effectful.getLastCheckpoint',
defaulting to @GlobalPosition 0@ when the subscription has never run.
-}
newInflightState :: Mdb.GlobalPosition -> IO InflightState
newInflightState stored =
    InflightState
        <$> newTVarIO
            InflightStateS
                { lastSaved = stored
                , outcomes = Map.empty
                , highestIngested = stored
                }

{- | Record that a message at @pos@ has been emitted by the source
stream but not yet finalized.

Inserts @(pos, Nothing)@ into the ledger and bumps @highestIngested@
if @pos@ exceeds it. No-op if @pos@ is already tracked.
-}
recordIngested :: InflightState -> Mdb.GlobalPosition -> STM ()
recordIngested (InflightState tv) pos =
    modifyTVar' tv $ \s ->
        s
            { outcomes = Map.insertWith (\_new old -> old) pos Nothing s.outcomes
            , highestIngested = max s.highestIngested pos
            }

{- | Record the ack outcome for a previously-ingested position.

Updates @outcomes[pos]@ to @Just outcome@. Silently ignores positions
not present in the ledger (idempotent against double-finalize, and
against finalize-for-a-position-we-never-ingested).
-}
recordAckResult ::
    InflightState ->
    Mdb.GlobalPosition ->
    AckOutcome ->
    STM ()
recordAckResult (InflightState tv) pos outcome =
    modifyTVar' tv $ \s ->
        s
            { outcomes = Map.adjust (const (Just outcome)) pos s.outcomes
            }

{- | Walk the ledger from @lastSaved + 1@ upward, advancing past every
@Just AckComplete@ entry. Stop at the first position that is missing,
still @Nothing@ (ingested-but-not-finalized), or @Just AckRetry@.

Remove advanced-past entries from the ledger so it does not grow
unboundedly. If any advancement occurred, update @lastSaved@ and
return @Just newLastSaved@; otherwise return @Nothing@.
-}
advanceCheckpointTo :: InflightState -> STM (Maybe Mdb.GlobalPosition)
advanceCheckpointTo (InflightState tv) = do
    s <- readTVar tv
    let (newLastSaved, remaining) = walk s.lastSaved s.outcomes
    if newLastSaved == s.lastSaved
        then pure Nothing
        else do
            writeTVar tv s{lastSaved = newLastSaved, outcomes = remaining}
            pure (Just newLastSaved)
  where
    walk ::
        Mdb.GlobalPosition ->
        Map Mdb.GlobalPosition (Maybe AckOutcome) ->
        (Mdb.GlobalPosition, Map Mdb.GlobalPosition (Maybe AckOutcome))
    walk current m =
        let Mdb.GlobalPosition n = current
            nextPos = Mdb.GlobalPosition (n + 1)
         in case Map.lookup nextPos m of
                Just (Just AckComplete) -> walk nextPos (Map.delete nextPos m)
                _ -> (current, m)

{- | Number of ledger entries whose outcome is still @Nothing@.

Used by the adapter's shutdown path to wait for in-flight messages to
drain before flushing the final checkpoint.
-}
inflightSize :: InflightState -> STM Int
inflightSize (InflightState tv) = do
    s <- readTVar tv
    pure $ Map.size (Map.filter (== Nothing) s.outcomes)
