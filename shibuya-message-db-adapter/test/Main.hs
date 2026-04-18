module Main (main) where

import Shibuya.Adapter.MessageDb.CheckpointResumeTest qualified as CheckpointResumeTest
import Shibuya.Adapter.MessageDb.ConvertTest qualified as ConvertTest
import Shibuya.Adapter.MessageDb.DlqTest qualified as DlqTest
import Shibuya.Adapter.MessageDb.InflightStateTest qualified as InflightStateTest
import Shibuya.Adapter.MessageDb.PartitionTest qualified as PartitionTest
import Shibuya.Adapter.MessageDb.RetryBufferTest qualified as RetryBufferTest
import Shibuya.Adapter.MessageDb.RetryDlqHaltResumeTest qualified as RetryDlqHaltResumeTest
import Test.Tasty (defaultMain, testGroup)

main :: IO ()
main =
    defaultMain $
        testGroup
            "shibuya-message-db-adapter"
            [ ConvertTest.tests
            , InflightStateTest.tests
            , RetryBufferTest.tests
            , DlqTest.tests
            , PartitionTest.tests
            , CheckpointResumeTest.tests
            , RetryDlqHaltResumeTest.tests
            ]
