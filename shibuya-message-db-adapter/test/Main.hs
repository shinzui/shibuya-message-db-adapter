module Main (main) where

import Shibuya.Adapter.MessageDb.ConvertTest qualified as ConvertTest
import Test.Tasty (defaultMain, testGroup)

main :: IO ()
main =
    defaultMain $
        testGroup
            "shibuya-message-db-adapter"
            [ConvertTest.tests]
