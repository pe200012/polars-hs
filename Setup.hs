module Main (main) where

import Distribution.PackageDescription (PackageDescription)
import Distribution.Simple
import Distribution.Simple.LocalBuildInfo (LocalBuildInfo)
import Distribution.Simple.Setup (BuildFlags, buildVerbosity, fromFlagOrDefault)
import Distribution.Simple.UserHooks (UserHooks (buildHook), simpleUserHooks)
import Distribution.Simple.Utils (rawSystemExit)
import Distribution.Verbosity (normal)

main :: IO ()
main =
    defaultMainWithHooks
        simpleUserHooks
            { buildHook = rustBuildHook
            }

rustBuildHook :: PackageDescription -> LocalBuildInfo -> UserHooks -> BuildFlags -> IO ()
rustBuildHook packageDescription localBuildInfo hooks flags = do
    let verbosity = fromFlagOrDefault normal (buildVerbosity flags)
    rawSystemExit
        verbosity
        "cargo"
        [ "build"
        , "--release"
        , "--manifest-path"
        , "rust/polars-hs-ffi/Cargo.toml"
        ]
    buildHook simpleUserHooks packageDescription localBuildInfo hooks flags
