{- | Unit tests for the consumer-group hash.

Pins 'categoryPartition' to the same output as @message-db-subscription@'s
private @getPartitionMurmur@. The equivalent computation is duplicated
here (not imported) so a divergence in the upstream hash surfaces as a
test failure instead of a silent partition split.
-}
module Shibuya.Adapter.MessageDb.PartitionTest (tests) where

import Data.Digest.Murmur64 qualified as Murmur
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Shibuya.Adapter.MessageDb.Internal (categoryPartition)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (Assertion, assertBool, testCase, (@?=))

{- | Reference implementation copied from
@message-db-subscription@'s @getPartitionMurmur@, applied directly to
the category name so the test does not depend on private exports.
-}
referencePartition :: Int -> Text -> Int
referencePartition size cat =
    fromIntegral (Murmur.asWord64 catHash) `mod` size
  where
    catBS = Text.encodeUtf8 cat
    catHash = Murmur.hash64 catBS

sampleCategories :: [Text]
sampleCategories =
    [ "orders"
    , "shipments"
    , "payments"
    , "customers"
    , "invoices"
    , "audit"
    , "cart"
    , "user"
    , "session"
    , "notification"
    ]
        <> fmap (\n -> "cat" <> Text.pack (show n)) [1 .. 90 :: Int]

deterministic :: Assertion
deterministic = do
    categoryPartition 7 "orders" @?= categoryPartition 7 "orders"
    categoryPartition 3 "cart" @?= categoryPartition 3 "cart"

inRange :: Assertion
inRange =
    mapM_ assertOne [(n, c) | n <- [1 .. 32 :: Int], c <- sampleCategories]
  where
    assertOne (n, c) = do
        let r = categoryPartition n c
        assertBool
            ( "out of range: categoryPartition "
                <> show n
                <> " "
                <> Text.unpack c
                <> " = "
                <> show r
            )
            (r >= 0 && r < n)

matchesReference :: Assertion
matchesReference =
    mapM_ assertOne [(n, c) | n <- [2, 3, 5, 8 :: Int], c <- sampleCategories]
  where
    assertOne (n, c) =
        categoryPartition n c @?= referencePartition n c

emptyCategoryDoesNotCrash :: Assertion
emptyCategoryDoesNotCrash = do
    let r = categoryPartition 3 ""
    assertBool
        ("empty category out of range: " <> show r)
        (r >= 0 && r < 3)

tests :: TestTree
tests =
    testGroup
        "partition"
        [ testCase "deterministic" deterministic
        , testCase "in range" inRange
        , testCase "matches message-db-subscription for representative inputs" matchesReference
        , testCase "empty category does not crash" emptyCategoryDoesNotCrash
        ]
