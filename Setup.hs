module Main (main) where

import Distribution.PackageDescription
    ( Benchmark (benchmarkBuildInfo)
    , Executable (buildInfo)
    , PackageDescription (benchmarks, executables, library, testSuites)
    , TestSuite (testBuildInfo)
    , emptyHookedBuildInfo
    , libBuildInfo
    )
import qualified Distribution.PackageDescription as PD
import Distribution.Simple
import Distribution.Simple.LocalBuildInfo (LocalBuildInfo (localPkgDescr))
import Distribution.Simple.Setup
    ( BuildFlags
    , ConfigFlags
    , buildVerbosity
    , configVerbosity
    , fromFlagOrDefault
    )
import Distribution.Simple.Utils (rawSystemExit)
import Distribution.Types.BuildInfo (BuildInfo (extraLibDirs))
import Distribution.Utils.Path (makeSymbolicPath)
import Distribution.Verbosity (Verbosity, normal)
import System.Directory (getCurrentDirectory)
import System.FilePath ((</>))

main :: IO ()
main =
    defaultMainWithHooks
        simpleUserHooks
            { preConf = rustPreConf
            , confHook = rustConfHook
            , buildHook = rustBuildHook
            }

rustPreConf :: [String] -> ConfigFlags -> IO PD.HookedBuildInfo
rustPreConf _ flags = do
    runCargo (fromFlagOrDefault normal (configVerbosity flags))
    pure emptyHookedBuildInfo

rustConfHook :: (PD.GenericPackageDescription, PD.HookedBuildInfo) -> ConfigFlags -> IO LocalBuildInfo
rustConfHook input flags = do
    runCargo (fromFlagOrDefault normal (configVerbosity flags))
    localBuildInfo <- confHook simpleUserHooks input flags
    libDir <- rustReleaseDir
    pure localBuildInfo {localPkgDescr = addRustLibDir libDir (localPkgDescr localBuildInfo)}

rustBuildHook :: PD.PackageDescription -> LocalBuildInfo -> UserHooks -> BuildFlags -> IO ()
rustBuildHook packageDescription localBuildInfo hooks flags = do
    runCargo (fromFlagOrDefault normal (buildVerbosity flags))
    buildHook simpleUserHooks packageDescription localBuildInfo hooks flags

runCargo :: Verbosity -> IO ()
runCargo verbosity =
    rawSystemExit
        verbosity
        Nothing
        "cargo"
        [ "build"
        , "--release"
        , "--manifest-path"
        , "rust/polars-hs-ffi/Cargo.toml"
        ]

rustReleaseDir :: IO FilePath
rustReleaseDir = do
    cwd <- getCurrentDirectory
    pure (cwd </> "rust" </> "polars-hs-ffi" </> "target" </> "release")

addRustLibDir :: FilePath -> PackageDescription -> PackageDescription
addRustLibDir dir packageDescription =
    packageDescription
        { library = fmap updateLibrary (library packageDescription)
        , executables = fmap updateExecutable (executables packageDescription)
        , testSuites = fmap updateTestSuite (testSuites packageDescription)
        , benchmarks = fmap updateBenchmark (benchmarks packageDescription)
        }
  where
    rustDir = makeSymbolicPath dir

    updateBuildInfo buildInfoValue = buildInfoValue {extraLibDirs = rustDir : extraLibDirs buildInfoValue}
    updateLibrary lib = lib {libBuildInfo = updateBuildInfo (libBuildInfo lib)}
    updateExecutable exe = exe {buildInfo = updateBuildInfo (buildInfo exe)}
    updateTestSuite suite = suite {testBuildInfo = updateBuildInfo (testBuildInfo suite)}
    updateBenchmark bench = bench {benchmarkBuildInfo = updateBuildInfo (benchmarkBuildInfo bench)}
