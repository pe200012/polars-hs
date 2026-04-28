{-# LANGUAGE ForeignFunctionInterface #-}

{- |
Module      : ArrowRecordBatch
Description : Test-only Arrow C Data Interface RecordBatch fixture.

The C fixture allocates a top-level struct ArrowSchema and ArrowArray with two
children: name :: Utf8 and age :: Int64. C release callbacks own every
allocation, so Rust can consume the batch through the normal Arrow C Data
Interface release protocol.
-}
module ArrowRecordBatch
    ( withAgeArray
    , withPeopleRecordBatch
    ) where

import Foreign.C.Types (CInt (..))
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr (Ptr, castPtr)
import Foreign.Storable (peek)

-- Opaque types for Apache Arrow C Data Interface structs.
data ArrowSchema
data ArrowArray

foreign import ccall unsafe "phs_test_people_record_batch"
    c_phs_test_people_record_batch :: Ptr (Ptr ()) -> Ptr (Ptr ()) -> IO CInt

foreign import ccall unsafe "phs_test_age_array"
    c_phs_test_age_array :: Ptr (Ptr ()) -> Ptr (Ptr ()) -> IO CInt

withPeopleRecordBatch :: (Ptr ArrowSchema -> Ptr ArrowArray -> IO a) -> IO a
withPeopleRecordBatch = withArrowFixture c_phs_test_people_record_batch "RecordBatch"

withAgeArray :: (Ptr ArrowSchema -> Ptr ArrowArray -> IO a) -> IO a
withAgeArray = withArrowFixture c_phs_test_age_array "age array"

withArrowFixture :: (Ptr (Ptr ()) -> Ptr (Ptr ()) -> IO CInt) -> String -> (Ptr ArrowSchema -> Ptr ArrowArray -> IO a) -> IO a
withArrowFixture allocate label action =
    alloca $ \schemaOut ->
        alloca $ \arrayOut -> do
            status <- allocate schemaOut arrayOut
            if status == 0
                then do
                    schema <- peek schemaOut
                    array <- peek arrayOut
                    action (castPtr schema) (castPtr array)
                else ioError (userError ("failed to allocate Arrow " <> label <> " fixture"))
