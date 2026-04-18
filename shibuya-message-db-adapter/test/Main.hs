module Main (main) where

import Shibuya.Adapter.MessageDb.CheckpointResumeTest qualified as CheckpointResumeTest
import Shibuya.Adapter.MessageDb.ConvertTest qualified as ConvertTest
import Shibuya.Adapter.MessageDb.InflightStateTest qualified as InflightStateTest
import Test.Tasty (defaultMain, testGroup)

main :: IO ()
main =
    defaultMain $
        testGroup
            "shibuya-message-db-adapter"
            [ ConvertTest.tests
            , InflightStateTest.tests
            , CheckpointResumeTest.tests
            ]
