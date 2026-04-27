{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.IPC
Description : Arrow IPC byte and file helpers for DataFrame interchange.

The MVP IPC API copies bytes into Haskell-owned ByteStrings. This gives a safe
interchange path before adding zero-copy Arrow C Data Interface support.
-}
module Polars.IPC
    ( fromIpcBytes
    , readIpcFile
    , toIpcBytes
    , writeIpcFile
    ) where

import qualified Data.ByteString as BS
import Foreign.C.Types (CInt, CSize)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr (Ptr, castPtr, nullPtr)
import Foreign.Storable (peek, poke)

import Polars.Error (PolarsError)
import Polars.Internal.Bytes (copyAndFreeBytes)
import Polars.Internal.CString (withFilePathCString)
import Polars.Internal.Managed (DataFrame, mkDataFrame, withDataFrame)
import Polars.Internal.Raw
    ( RawDataFrame
    , RawError
    , phs_dataframe_from_ipc_bytes
    , phs_dataframe_to_ipc_bytes
    , phs_read_ipc_file
    , phs_write_ipc_file
    )
import Polars.Internal.Result (consumeError, nullPointerError)

toIpcBytes :: DataFrame -> IO (Either PolarsError BS.ByteString)
toIpcBytes df = withDataFrame df $ \ptr ->
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- phs_dataframe_to_ipc_bytes ptr outPtr errPtr
            if status == 0
                then Right <$> (copyAndFreeBytes =<< peek outPtr)
                else Left <$> (consumeError status =<< peek errPtr)

fromIpcBytes :: BS.ByteString -> IO (Either PolarsError DataFrame)
fromIpcBytes bytes = BS.useAsCStringLen bytes $ \(dataPtr, len) ->
    dataframeOut (phs_dataframe_from_ipc_bytes (castPtr dataPtr) (fromIntegral len :: CSize))

readIpcFile :: FilePath -> IO (Either PolarsError DataFrame)
readIpcFile path = withFilePathCString path $ \cPath -> dataframeOut (phs_read_ipc_file cPath)

writeIpcFile :: FilePath -> DataFrame -> IO (Either PolarsError ())
writeIpcFile path df = withFilePathCString path $ \cPath -> withDataFrame df $ \dfPtr ->
    alloca $ \errPtr -> do
        poke errPtr nullPtr
        status <- phs_write_ipc_file cPath dfPtr errPtr
        if status == 0
            then pure (Right ())
            else Left <$> (consumeError status =<< peek errPtr)

dataframeOut :: (Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError DataFrame)
dataframeOut action =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if status == 0
                then do
                    ptr <- peek outPtr
                    if ptr == nullPtr
                        then pure (Left (nullPointerError "dataframe output"))
                        else Right <$> mkDataFrame ptr
                else Left <$> (consumeError status =<< peek errPtr)
