# Polars Haskell Binding MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first working Haskell binding to Rust Polars with eager CSV/DataFrame APIs, lazy filter/select/collect APIs, typed errors, managed foreign handles, and IPC byte round-trips.

**Architecture:** A Rust crate at `rust/polars-hs-ffi` owns all direct Polars calls and exposes a stable `phs_*` C ABI. Haskell modules under `src/Polars` wrap Rust handles in `ForeignPtr`, return `Either PolarsError a`, and keep expressions as a pure Haskell AST compiled into temporary Rust expression handles at FFI boundaries.

**Tech Stack:** Haskell Stack/Hpack with `resolver: nightly-2026-04-26` and `compiler: ghc-9.12.2`; Rust 2024 edition; Polars crates `0.53.0`; cbindgen; Hspec; HLint.

---

## File Structure

Create and modify these files.

### Haskell package and build files

- Modify `package.yaml`: switch to `build-type: Custom`, add `text`, `bytestring`, `filepath`, `directory`, `hspec`, and native link settings.
- Modify `Setup.hs`: run `cargo build --release --manifest-path rust/polars-hs-ffi/Cargo.toml` before Cabal builds Haskell components.
- Regenerate `polars-hs.cabal` through `stack build` or `stack test`.

### Rust adapter crate

- Create `rust/polars-hs-ffi/Cargo.toml`: Rust adapter metadata, Polars dependencies, staticlib/cdylib crate types, cbindgen build dependency.
- Create `rust/polars-hs-ffi/build.rs`: generate `include/polars_hs.h` from Rust exports.
- Create `rust/polars-hs-ffi/cbindgen.toml`: C header settings.
- Create `rust/polars-hs-ffi/src/lib.rs`: exported module declarations and version function.
- Create `rust/polars-hs-ffi/src/error.rs`: status codes, `phs_error`, panic boundary, and error free/access functions.
- Create `rust/polars-hs-ffi/src/handles.rs`: opaque handle types and pointer conversion helpers.
- Create `rust/polars-hs-ffi/src/bytes.rs`: Rust-owned byte buffers for schema text, string rendering, and IPC.
- Create `rust/polars-hs-ffi/src/dataframe.rs`: eager CSV/Parquet readers, shape, schema, head, tail, and display.
- Create `rust/polars-hs-ffi/src/expr.rs`: expression constructors and binary operators.
- Create `rust/polars-hs-ffi/src/lazyframe.rs`: scan, lazy operations, collect.
- Create `rust/polars-hs-ffi/src/ipc.rs`: dataframe IPC import/export and file helpers.

### Haskell library modules

- Replace `src/Lib.hs` with `src/Polars.hs` as the public re-export module.
- Create `src/Polars/Error.hs`: error code and `PolarsError` type.
- Create `src/Polars/Schema.hs`: `DataType`, `Field`, and schema byte parser.
- Create `src/Polars/Expr.hs`: pure expression AST and constructors.
- Create `src/Polars/Operators.hs`: infix expression operators.
- Create `src/Polars/DataFrame.hs`: eager DataFrame API and IPC exports.
- Create `src/Polars/LazyFrame.hs`: lazy API.
- Create `src/Polars/IPC.hs`: IPC byte/file helpers.
- Create `src/Polars/Internal/Raw.hs`: raw FFI imports.
- Create `src/Polars/Internal/Managed.hs`: `ForeignPtr` constructors and pointer helpers.
- Create `src/Polars/Internal/Result.hs`: status/error conversion helpers.
- Create `src/Polars/Internal/CString.hs`: `Text`/`CString` helpers.
- Create `src/Polars/Internal/Bytes.hs`: Rust byte buffer copying.
- Create `src/Polars/Internal/Expr.hs`: compile pure `Expr` AST to temporary raw Rust expression handles.

### Tests, examples, and docs

- Replace `test/Spec.hs` with Hspec integration tests.
- Create `test/data/people.csv`.
- Create `examples/iris.hs`.
- Modify `README.md`: quickstart and build requirements.

---

## Task 1: Add failing Haskell tests and package shape

**Files:**
- Modify: `package.yaml`
- Modify: `test/Spec.hs`
- Create: `test/data/people.csv`

- [ ] **Step 1: Replace `package.yaml` with the MVP package configuration**

```yaml
name:                polars-hs
version:             0.1.0.0
github:              "pe200012/polars-hs"
license:             BSD-3-Clause
author:              "pe200012"
maintainer:          "1326263755@qq.com"
copyright:           "2026 pe200012"

build-type:          Custom

extra-source-files:
- README.md
- CHANGELOG.md
- include/polars_hs.h
- rust/polars-hs-ffi/Cargo.toml
- rust/polars-hs-ffi/Cargo.lock
- rust/polars-hs-ffi/build.rs
- rust/polars-hs-ffi/cbindgen.toml
- rust/polars-hs-ffi/src/*.rs
- test/data/*.csv
- examples/*.hs

synopsis:            Haskell bindings for the Rust Polars dataframe library
category:            Data

description:         Please see the README on GitHub at <https://github.com/pe200012/polars-hs#readme>

custom-setup:
  dependencies:
  - base >= 4.7 && < 5
  - Cabal

dependencies:
- base >= 4.7 && < 5
- bytestring >= 0.12 && < 0.14
- text >= 2.1 && < 2.3
- filepath >= 1.4 && < 1.6
- directory >= 1.3 && < 1.4

ghc-options:
- -Wall
- -Wcompat
- -Widentities
- -Wincomplete-record-updates
- -Wincomplete-uni-patterns
- -Wmissing-export-lists
- -Wmissing-home-modules
- -Wpartial-fields
- -Wredundant-constraints

library:
  source-dirs: src
  exposed-modules:
  - Polars
  - Polars.DataFrame
  - Polars.Error
  - Polars.Expr
  - Polars.IPC
  - Polars.LazyFrame
  - Polars.Operators
  - Polars.Schema
  other-modules:
  - Polars.Internal.Bytes
  - Polars.Internal.CString
  - Polars.Internal.Expr
  - Polars.Internal.Managed
  - Polars.Internal.Raw
  - Polars.Internal.Result
  include-dirs:
  - include
  extra-lib-dirs:
  - rust/polars-hs-ffi/target/release
  extra-libraries:
  - polars_hs_ffi
  - pthread
  - dl
  - m

executables:
  polars-hs-exe:
    main:                Main.hs
    source-dirs:         app
    ghc-options:
    - -threaded
    - -rtsopts
    - -with-rtsopts=-N
    dependencies:
    - polars-hs

tests:
  polars-hs-test:
    main:                Spec.hs
    source-dirs:         test
    ghc-options:
    - -threaded
    - -rtsopts
    - -with-rtsopts=-N
    dependencies:
    - polars-hs
    - hspec >= 2.11 && < 2.12
```

- [ ] **Step 2: Replace `test/Spec.hs` with failing integration tests**

```haskell
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Prelude hiding (filter, head)

import qualified Data.ByteString as BS
import qualified Data.Text as T
import Test.Hspec

import qualified Polars as Pl

fixtureCsv :: FilePath
fixtureCsv = "test/data/people.csv"

main :: IO ()
main = hspec $ do
    describe "Polars.DataFrame" $ do
        it "reads a CSV file and reports shape" $ do
            result <- Pl.readCsv fixtureCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> Pl.shape df `shouldReturn` Right (3, 2)

        it "reports schema field names" $ do
            result <- Pl.readCsv fixtureCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    schemaResult <- Pl.schema df
                    fmap (map Pl.fieldName) schemaResult `shouldBe` Right ["name", "age"]

        it "returns a typed error for a missing CSV file" $ do
            result <- Pl.readCsv "test/data/missing.csv"
            case result of
                Right _ -> expectationFailure "expected a Polars error"
                Left err -> Pl.polarsErrorCode err `shouldBe` Pl.PolarsFailure

        it "renders a dataframe to text" $ do
            result <- Pl.readCsv fixtureCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    textResult <- Pl.toText df
                    fmap (T.isInfixOf "Alice") textResult `shouldBe` Right True

    describe "Polars.LazyFrame" $ do
        it "filters, selects, and collects a lazy CSV scan" $ do
            scanResult <- Pl.scanCsv fixtureCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    filtered <- Pl.filter (Pl.col "age" Pl..> Pl.litInt 35) lf0
                    case filtered of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            selected <- Pl.select [Pl.col "name"] lf1
                            case selected of
                                Left err -> expectationFailure (show err)
                                Right lf2 -> do
                                    collected <- Pl.collect lf2
                                    case collected of
                                        Left err -> expectationFailure (show err)
                                        Right df -> Pl.shape df `shouldReturn` Right (1, 1)

    describe "Polars.IPC" $ do
        it "round-trips a dataframe through IPC bytes" $ do
            result <- Pl.readCsv fixtureCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df0 -> do
                    bytesResult <- Pl.toIpcBytes df0
                    case bytesResult of
                        Left err -> expectationFailure (show err)
                        Right bytes -> do
                            BS.length bytes `shouldSatisfy` (> 0)
                            dfResult <- Pl.fromIpcBytes bytes
                            case dfResult of
                                Left err -> expectationFailure (show err)
                                Right df1 -> Pl.shape df1 `shouldReturn` Right (3, 2)
```

- [ ] **Step 3: Create the CSV fixture**

```csv
name,age
Alice,34
Bob,45
Carol,29
```

Write it to `test/data/people.csv`.

- [ ] **Step 4: Run the failing Haskell test**

Run:

```bash
stack test --fast
```

Expected: FAIL. The failure should mention missing modules such as `Polars`, because implementation files have not been created yet.

- [ ] **Step 5: Commit the failing test task**

Run:

```bash
jj status || true
jj commit -m "test: add polars binding mvp expectations" || true
```

Expected in a jj repository: a commit is created. Expected in the current checkout: jj reports that the directory is not a jj repo.

---

## Task 2: Add Cargo build bridge and Rust FFI foundation

**Files:**
- Modify: `Setup.hs`
- Create: `rust/polars-hs-ffi/Cargo.toml`
- Create: `rust/polars-hs-ffi/build.rs`
- Create: `rust/polars-hs-ffi/cbindgen.toml`
- Create: `rust/polars-hs-ffi/src/lib.rs`
- Create: `rust/polars-hs-ffi/src/error.rs`
- Create: `rust/polars-hs-ffi/src/handles.rs`
- Create: `rust/polars-hs-ffi/src/bytes.rs`

- [ ] **Step 1: Replace `Setup.hs` with a Cargo build hook**

```haskell
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
```

- [ ] **Step 2: Create `rust/polars-hs-ffi/Cargo.toml`**

```toml
[package]
name = "polars-hs-ffi"
version = "0.1.0"
edition = "2024"
license = "BSD-3-Clause"
publish = false

[lib]
name = "polars_hs_ffi"
crate-type = ["staticlib", "cdylib"]

[dependencies]
libc = "0.2"
polars = { version = "0.53.0", default-features = false, features = ["lazy", "csv", "parquet", "ipc", "fmt", "dtype-slim", "strings", "temporal"] }

[build-dependencies]
cbindgen = "0.29"
```

- [ ] **Step 3: Create `rust/polars-hs-ffi/build.rs`**

```rust
use std::env;
use std::path::PathBuf;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by Cargo");
    let header_path = PathBuf::from(&crate_dir)
        .join("..")
        .join("..")
        .join("include")
        .join("polars_hs.h");

    println!("cargo:rerun-if-changed=src");

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_config(cbindgen::Config::from_file("cbindgen.toml").expect("valid cbindgen.toml"))
        .generate()
        .expect("generate C header")
        .write_to_file(header_path);
}
```

- [ ] **Step 4: Create `rust/polars-hs-ffi/cbindgen.toml`**

```toml
language = "C"
include_guard = "POLARS_HS_H"
pragma_once = true
style = "tag"
sort_by = "Name"
autogen_warning = "/* Generated by cbindgen. Keep Rust exports and Haskell FFI imports in sync. */"

[export]
prefix = ""
include = [
  "phs_dataframe",
  "phs_lazyframe",
  "phs_expr",
  "phs_error",
  "phs_bytes"
]
```

- [ ] **Step 5: Create `rust/polars-hs-ffi/src/error.rs`**

```rust
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

use polars::prelude::PolarsError;

pub const PHS_STATUS_OK: c_int = 0;
pub const PHS_STATUS_POLARS: c_int = 1;
pub const PHS_STATUS_INVALID_ARGUMENT: c_int = 2;
pub const PHS_STATUS_UTF8: c_int = 3;
pub const PHS_STATUS_PANIC: c_int = 4;

#[repr(C)]
pub struct phs_error {
    code: c_int,
    message: CString,
}

#[derive(Debug)]
pub struct PhsError {
    pub code: c_int,
    pub message: String,
}

impl PhsError {
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self { code: PHS_STATUS_INVALID_ARGUMENT, message: message.into() }
    }

    pub fn utf8(message: impl Into<String>) -> Self {
        Self { code: PHS_STATUS_UTF8, message: message.into() }
    }

    pub fn polars(message: impl Into<String>) -> Self {
        Self { code: PHS_STATUS_POLARS, message: message.into() }
    }

    pub fn panic(message: impl Into<String>) -> Self {
        Self { code: PHS_STATUS_PANIC, message: message.into() }
    }

    fn into_raw(self) -> *mut phs_error {
        let sanitized = self.message.replace('\0', "\\0");
        let message = CString::new(sanitized).expect("message was sanitized");
        Box::into_raw(Box::new(phs_error { code: self.code, message }))
    }
}

impl From<PolarsError> for PhsError {
    fn from(value: PolarsError) -> Self {
        PhsError::polars(value.to_string())
    }
}

impl From<std::io::Error> for PhsError {
    fn from(value: std::io::Error) -> Self {
        PhsError::polars(value.to_string())
    }
}

pub unsafe fn cstr_to_str<'a>(ptr: *const c_char, name: &str) -> Result<&'a str, PhsError> {
    if ptr.is_null() {
        return Err(PhsError::invalid_argument(format!("{name} pointer was null")));
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|err| PhsError::utf8(format!("{name} was not valid UTF-8: {err}")))
}

pub unsafe fn set_error(err_out: *mut *mut phs_error, error: PhsError) -> c_int {
    let code = error.code;
    if !err_out.is_null() {
        unsafe { *err_out = error.into_raw() };
    }
    code
}

pub unsafe fn clear_error(err_out: *mut *mut phs_error) {
    if !err_out.is_null() {
        unsafe { *err_out = ptr::null_mut() };
    }
}

pub unsafe fn ffi_boundary<F>(err_out: *mut *mut phs_error, f: F) -> c_int
where
    F: FnOnce() -> Result<(), PhsError>,
{
    unsafe { clear_error(err_out) };
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => PHS_STATUS_OK,
        Ok(Err(err)) => unsafe { set_error(err_out, err) },
        Err(payload) => {
            let message = if let Some(text) = payload.downcast_ref::<&str>() {
                (*text).to_owned()
            } else if let Some(text) = payload.downcast_ref::<String>() {
                text.clone()
            } else {
                "Rust panic crossed the FFI boundary".to_owned()
            };
            unsafe { set_error(err_out, PhsError::panic(message)) }
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_error_code(error: *const phs_error) -> c_int {
    if error.is_null() {
        return PHS_STATUS_INVALID_ARGUMENT;
    }
    unsafe { (*error).code }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_error_message(error: *const phs_error) -> *const c_char {
    if error.is_null() {
        return ptr::null();
    }
    unsafe { (*error).message.as_ptr() }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_error_free(error: *mut phs_error) {
    if !error.is_null() {
        unsafe { drop(Box::from_raw(error)) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_message_sanitizes_nuls() {
        let err = PhsError::invalid_argument("a\0b");
        let raw = err.into_raw();
        let message = unsafe { CStr::from_ptr(phs_error_message(raw)) }
            .to_str()
            .expect("valid utf8");
        assert_eq!(message, "a\\0b");
        unsafe { phs_error_free(raw) };
    }
}
```

- [ ] **Step 6: Create `rust/polars-hs-ffi/src/handles.rs`**

```rust
use polars::prelude::{DataFrame, Expr, LazyFrame};

use crate::error::PhsError;

#[repr(C)]
pub struct phs_dataframe {
    _private: [u8; 0],
}

#[repr(C)]
pub struct phs_lazyframe {
    _private: [u8; 0],
}

#[repr(C)]
pub struct phs_expr {
    _private: [u8; 0],
}

pub struct DataFrameHandle {
    pub value: DataFrame,
}

pub struct LazyFrameHandle {
    pub value: LazyFrame,
}

pub struct ExprHandle {
    pub value: Expr,
}

pub fn into_dataframe_handle(value: DataFrame) -> *mut phs_dataframe {
    Box::into_raw(Box::new(DataFrameHandle { value })) as *mut phs_dataframe
}

pub fn into_lazyframe_handle(value: LazyFrame) -> *mut phs_lazyframe {
    Box::into_raw(Box::new(LazyFrameHandle { value })) as *mut phs_lazyframe
}

pub fn into_expr_handle(value: Expr) -> *mut phs_expr {
    Box::into_raw(Box::new(ExprHandle { value })) as *mut phs_expr
}

pub unsafe fn dataframe_ref<'a>(ptr: *const phs_dataframe) -> Result<&'a DataFrameHandle, PhsError> {
    if ptr.is_null() {
        return Err(PhsError::invalid_argument("dataframe pointer was null"));
    }
    Ok(unsafe { &*(ptr as *const DataFrameHandle) })
}

pub unsafe fn lazyframe_ref<'a>(ptr: *const phs_lazyframe) -> Result<&'a LazyFrameHandle, PhsError> {
    if ptr.is_null() {
        return Err(PhsError::invalid_argument("lazyframe pointer was null"));
    }
    Ok(unsafe { &*(ptr as *const LazyFrameHandle) })
}

pub unsafe fn expr_ref<'a>(ptr: *const phs_expr) -> Result<&'a ExprHandle, PhsError> {
    if ptr.is_null() {
        return Err(PhsError::invalid_argument("expression pointer was null"));
    }
    Ok(unsafe { &*(ptr as *const ExprHandle) })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_free(ptr: *mut phs_dataframe) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr as *mut DataFrameHandle)) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_free(ptr: *mut phs_lazyframe) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr as *mut LazyFrameHandle)) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_free(ptr: *mut phs_expr) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr as *mut ExprHandle)) };
    }
}
```

- [ ] **Step 7: Create `rust/polars-hs-ffi/src/bytes.rs`**

```rust
use std::os::raw::c_uchar;
use std::ptr;

#[repr(C)]
pub struct phs_bytes {
    bytes: Vec<u8>,
}

pub fn into_bytes_handle(bytes: Vec<u8>) -> *mut phs_bytes {
    Box::into_raw(Box::new(phs_bytes { bytes }))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_bytes_data(bytes: *const phs_bytes) -> *const c_uchar {
    if bytes.is_null() {
        return ptr::null();
    }
    unsafe { (*bytes).bytes.as_ptr() }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_bytes_len(bytes: *const phs_bytes) -> usize {
    if bytes.is_null() {
        return 0;
    }
    unsafe { (*bytes).bytes.len() }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_bytes_free(bytes: *mut phs_bytes) {
    if !bytes.is_null() {
        unsafe { drop(Box::from_raw(bytes)) };
    }
}
```

- [ ] **Step 8: Create `rust/polars-hs-ffi/src/lib.rs`**

```rust
pub mod bytes;
pub mod error;
pub mod handles;

#[unsafe(no_mangle)]
pub extern "C" fn phs_version_major() -> u32 {
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn phs_version_minor() -> u32 {
    1
}
```

- [ ] **Step 9: Run Rust foundation tests**

Run:

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
```

Expected: PASS. Cargo should create `rust/polars-hs-ffi/Cargo.lock` and `include/polars_hs.h`.

- [ ] **Step 10: Commit the Rust foundation task**

Run:

```bash
jj status || true
jj commit -m "build: add rust polars ffi foundation" || true
```

Expected in a jj repository: a commit is created. Expected in the current checkout: jj reports that the directory is not a jj repo.

---

## Task 3: Implement Rust eager DataFrame and IPC functions

**Files:**
- Modify: `rust/polars-hs-ffi/src/dataframe.rs`
- Modify: `rust/polars-hs-ffi/src/ipc.rs`

- [ ] **Step 1: Update `rust/polars-hs-ffi/src/lib.rs` for eager DataFrame and IPC modules**

```rust
pub mod bytes;
pub mod dataframe;
pub mod error;
pub mod handles;
pub mod ipc;

#[unsafe(no_mangle)]
pub extern "C" fn phs_version_major() -> u32 {
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn phs_version_minor() -> u32 {
    1
}
```

- [ ] **Step 2: Replace `rust/polars-hs-ffi/src/dataframe.rs`**

```rust
use std::fs::File;
use std::os::raw::{c_char, c_int};
use std::path::PathBuf;
use std::ptr;

use polars::prelude::*;

use crate::bytes::{into_bytes_handle, phs_bytes};
use crate::error::{PhsError, cstr_to_str, ffi_boundary};
use crate::handles::{dataframe_ref, into_dataframe_handle, phs_dataframe};

unsafe fn pathbuf_from_c(path: *const c_char, name: &str) -> Result<PathBuf, PhsError> {
    Ok(PathBuf::from(unsafe { cstr_to_str(path, name)? }))
}

unsafe fn out_dataframe<'a>(out: *mut *mut phs_dataframe) -> Result<&'a mut *mut phs_dataframe, PhsError> {
    if out.is_null() {
        return Err(PhsError::invalid_argument("dataframe out pointer was null"));
    }
    unsafe {
        *out = ptr::null_mut();
        Ok(&mut *out)
    }
}

unsafe fn out_u64<'a>(out: *mut u64, name: &str) -> Result<&'a mut u64, PhsError> {
    if out.is_null() {
        return Err(PhsError::invalid_argument(format!("{name} pointer was null")));
    }
    Ok(unsafe { &mut *out })
}

unsafe fn out_bytes<'a>(out: *mut *mut phs_bytes) -> Result<&'a mut *mut phs_bytes, PhsError> {
    if out.is_null() {
        return Err(PhsError::invalid_argument("bytes out pointer was null"));
    }
    unsafe {
        *out = ptr::null_mut();
        Ok(&mut *out)
    }
}

fn schema_bytes(df: &DataFrame) -> Vec<u8> {
    let mut out = Vec::new();
    for field in df.schema().iter_fields() {
        out.extend_from_slice(field.name().as_bytes());
        out.push(0);
        out.extend_from_slice(format!("{:?}", field.dtype()).as_bytes());
        out.push(0);
    }
    out
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_read_csv(
    path: *const c_char,
    out: *mut *mut phs_dataframe,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let out = out_dataframe(out)?;
            let path = pathbuf_from_c(path, "path")?;
            let df = CsvReadOptions::default()
                .with_has_header(true)
                .try_into_reader_with_file_path(Some(path))?
                .finish()?;
            *out = into_dataframe_handle(df);
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_read_parquet(
    path: *const c_char,
    out: *mut *mut phs_dataframe,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let out = out_dataframe(out)?;
            let path = pathbuf_from_c(path, "path")?;
            let file = File::open(path)?;
            let df = ParquetReader::new(file).finish()?;
            *out = into_dataframe_handle(df);
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_shape(
    df: *const phs_dataframe,
    rows_out: *mut u64,
    cols_out: *mut u64,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let df = dataframe_ref(df)?;
            let rows_out = out_u64(rows_out, "rows_out")?;
            let cols_out = out_u64(cols_out, "cols_out")?;
            let (rows, cols) = df.value.shape();
            *rows_out = rows as u64;
            *cols_out = cols as u64;
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_head(
    df: *const phs_dataframe,
    n: u64,
    out: *mut *mut phs_dataframe,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let df = dataframe_ref(df)?;
            let out = out_dataframe(out)?;
            *out = into_dataframe_handle(df.value.head(Some(n as usize)));
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_tail(
    df: *const phs_dataframe,
    n: u64,
    out: *mut *mut phs_dataframe,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let df = dataframe_ref(df)?;
            let out = out_dataframe(out)?;
            *out = into_dataframe_handle(df.value.tail(Some(n as usize)));
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_schema(
    df: *const phs_dataframe,
    out: *mut *mut phs_bytes,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let df = dataframe_ref(df)?;
            let out = out_bytes(out)?;
            *out = into_bytes_handle(schema_bytes(&df.value));
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_to_string(
    df: *const phs_dataframe,
    out: *mut *mut phs_bytes,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let df = dataframe_ref(df)?;
            let out = out_bytes(out)?;
            *out = into_bytes_handle(format!("{}", df.value).into_bytes());
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes::{phs_bytes_data, phs_bytes_free, phs_bytes_len};
    use crate::handles::phs_dataframe_free;
    use std::ffi::CString;
    use std::slice;

    #[test]
    fn reads_fixture_csv_and_reports_shape() {
        let path = CString::new("../../test/data/people.csv").expect("valid cstring");
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_read_csv(path.as_ptr(), &mut out, &mut err) };
        assert_eq!(status, 0);
        assert!(err.is_null());
        let mut rows = 0_u64;
        let mut cols = 0_u64;
        let status = unsafe { phs_dataframe_shape(out, &mut rows, &mut cols, &mut err) };
        assert_eq!(status, 0);
        assert_eq!((rows, cols), (3, 2));
        unsafe { phs_dataframe_free(out) };
    }

    #[test]
    fn schema_bytes_include_field_names() {
        let path = CString::new("../../test/data/people.csv").expect("valid cstring");
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_read_csv(path.as_ptr(), &mut out, &mut err) };
        assert_eq!(status, 0);
        let mut bytes = ptr::null_mut();
        let status = unsafe { phs_dataframe_schema(out, &mut bytes, &mut err) };
        assert_eq!(status, 0);
        let len = unsafe { phs_bytes_len(bytes) };
        let data = unsafe { phs_bytes_data(bytes) };
        let slice = unsafe { slice::from_raw_parts(data, len) };
        assert!(slice.windows(4).any(|w| w == b"name"));
        assert!(slice.windows(3).any(|w| w == b"age"));
        unsafe { phs_bytes_free(bytes) };
        unsafe { phs_dataframe_free(out) };
    }
}
```

- [ ] **Step 3: Replace `rust/polars-hs-ffi/src/ipc.rs`**

```rust
use std::fs::File;
use std::io::{Cursor, Write};
use std::os::raw::{c_char, c_int, c_uchar};
use std::ptr;

use polars::prelude::*;

use crate::bytes::{into_bytes_handle, phs_bytes};
use crate::dataframe::pathbuf_from_c;
use crate::error::{PhsError, ffi_boundary};
use crate::handles::{dataframe_ref, into_dataframe_handle, phs_dataframe};

unsafe fn out_dataframe<'a>(out: *mut *mut phs_dataframe) -> Result<&'a mut *mut phs_dataframe, PhsError> {
    if out.is_null() {
        return Err(PhsError::invalid_argument("dataframe out pointer was null"));
    }
    unsafe {
        *out = ptr::null_mut();
        Ok(&mut *out)
    }
}

unsafe fn out_bytes<'a>(out: *mut *mut phs_bytes) -> Result<&'a mut *mut phs_bytes, PhsError> {
    if out.is_null() {
        return Err(PhsError::invalid_argument("bytes out pointer was null"));
    }
    unsafe {
        *out = ptr::null_mut();
        Ok(&mut *out)
    }
}

unsafe fn input_bytes<'a>(data: *const c_uchar, len: usize) -> Result<&'a [u8], PhsError> {
    if data.is_null() && len > 0 {
        return Err(PhsError::invalid_argument("IPC data pointer was null"));
    }
    Ok(unsafe { std::slice::from_raw_parts(data, len) })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_to_ipc_bytes(
    df: *const phs_dataframe,
    out: *mut *mut phs_bytes,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let df = dataframe_ref(df)?;
            let out = out_bytes(out)?;
            let mut buffer = Cursor::new(Vec::new());
            let mut cloned = df.value.clone();
            IpcWriter::new(&mut buffer).finish(&mut cloned)?;
            *out = into_bytes_handle(buffer.into_inner());
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_from_ipc_bytes(
    data: *const c_uchar,
    len: usize,
    out: *mut *mut phs_dataframe,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let bytes = input_bytes(data, len)?;
            let out = out_dataframe(out)?;
            let cursor = Cursor::new(bytes.to_vec());
            let df = IpcReader::new(cursor).finish()?;
            *out = into_dataframe_handle(df);
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_write_ipc_file(
    path: *const c_char,
    df: *const phs_dataframe,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let path = pathbuf_from_c(path, "path")?;
            let df = dataframe_ref(df)?;
            let mut file = File::create(path)?;
            let mut cloned = df.value.clone();
            IpcWriter::new(&mut file).finish(&mut cloned)?;
            file.flush()?;
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_read_ipc_file(
    path: *const c_char,
    out: *mut *mut phs_dataframe,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let path = pathbuf_from_c(path, "path")?;
            let out = out_dataframe(out)?;
            let file = File::open(path)?;
            let df = IpcReader::new(file).finish()?;
            *out = into_dataframe_handle(df);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataframe::{phs_dataframe_shape, phs_read_csv};
    use crate::handles::phs_dataframe_free;
    use std::ffi::CString;

    #[test]
    fn ipc_bytes_roundtrip_preserves_shape() {
        let path = CString::new("../../test/data/people.csv").expect("valid cstring");
        let mut df0 = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_read_csv(path.as_ptr(), &mut df0, &mut err) }, 0);
        let mut bytes = ptr::null_mut();
        assert_eq!(unsafe { phs_dataframe_to_ipc_bytes(df0, &mut bytes, &mut err) }, 0);
        let data = unsafe { crate::bytes::phs_bytes_data(bytes) };
        let len = unsafe { crate::bytes::phs_bytes_len(bytes) };
        let mut df1 = ptr::null_mut();
        assert_eq!(unsafe { phs_dataframe_from_ipc_bytes(data, len, &mut df1, &mut err) }, 0);
        let mut rows = 0_u64;
        let mut cols = 0_u64;
        assert_eq!(unsafe { phs_dataframe_shape(df1, &mut rows, &mut cols, &mut err) }, 0);
        assert_eq!((rows, cols), (3, 2));
        unsafe { crate::bytes::phs_bytes_free(bytes) };
        unsafe { phs_dataframe_free(df0) };
        unsafe { phs_dataframe_free(df1) };
    }
}
```

- [ ] **Step 4: Run Rust tests**

Run:

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
```

Expected: PASS.

- [ ] **Step 5: Commit the Rust eager dataframe task**

Run:

```bash
jj status || true
jj commit -m "feat: add rust dataframe and ipc ffi" || true
```

Expected in a jj repository: a commit is created. Expected in the current checkout: jj reports that the directory is not a jj repo.

---

## Task 4: Implement Rust expression and LazyFrame functions

**Files:**
- Modify: `rust/polars-hs-ffi/src/expr.rs`
- Modify: `rust/polars-hs-ffi/src/lazyframe.rs`

- [ ] **Step 1: Update `rust/polars-hs-ffi/src/lib.rs` for expression and LazyFrame modules**

```rust
pub mod bytes;
pub mod dataframe;
pub mod error;
pub mod expr;
pub mod handles;
pub mod ipc;
pub mod lazyframe;

#[unsafe(no_mangle)]
pub extern "C" fn phs_version_major() -> u32 {
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn phs_version_minor() -> u32 {
    1
}
```

- [ ] **Step 2: Replace `rust/polars-hs-ffi/src/expr.rs`**

```rust
use std::os::raw::{c_char, c_double, c_int};
use std::ptr;

use polars::prelude::*;

use crate::error::{PhsError, cstr_to_str, ffi_boundary};
use crate::handles::{expr_ref, into_expr_handle, phs_expr};

pub const PHS_OP_EQ: c_int = 1;
pub const PHS_OP_NOT_EQ: c_int = 2;
pub const PHS_OP_GT: c_int = 3;
pub const PHS_OP_GT_EQ: c_int = 4;
pub const PHS_OP_LT: c_int = 5;
pub const PHS_OP_LT_EQ: c_int = 6;
pub const PHS_OP_AND: c_int = 7;
pub const PHS_OP_OR: c_int = 8;
pub const PHS_OP_ADD: c_int = 9;
pub const PHS_OP_SUBTRACT: c_int = 10;
pub const PHS_OP_MULTIPLY: c_int = 11;
pub const PHS_OP_DIVIDE: c_int = 12;

unsafe fn out_expr<'a>(out: *mut *mut phs_expr) -> Result<&'a mut *mut phs_expr, PhsError> {
    if out.is_null() {
        return Err(PhsError::invalid_argument("expression out pointer was null"));
    }
    unsafe {
        *out = ptr::null_mut();
        Ok(&mut *out)
    }
}

fn apply_binary(op: c_int, left: Expr, right: Expr) -> Result<Expr, PhsError> {
    match op {
        PHS_OP_EQ => Ok(left.eq(right)),
        PHS_OP_NOT_EQ => Ok(left.neq(right)),
        PHS_OP_GT => Ok(left.gt(right)),
        PHS_OP_GT_EQ => Ok(left.gt_eq(right)),
        PHS_OP_LT => Ok(left.lt(right)),
        PHS_OP_LT_EQ => Ok(left.lt_eq(right)),
        PHS_OP_AND => Ok(left.and(right)),
        PHS_OP_OR => Ok(left.or(right)),
        PHS_OP_ADD => Ok(left + right),
        PHS_OP_SUBTRACT => Ok(left - right),
        PHS_OP_MULTIPLY => Ok(left * right),
        PHS_OP_DIVIDE => Ok(left / right),
        _ => Err(PhsError::invalid_argument(format!("unknown binary operator code {op}"))),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_col(
    name: *const c_char,
    out: *mut *mut phs_expr,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let name = cstr_to_str(name, "column name")?;
            let out = out_expr(out)?;
            *out = into_expr_handle(col(name));
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_lit_bool(
    value: c_int,
    out: *mut *mut phs_expr,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let out = out_expr(out)?;
            *out = into_expr_handle(lit(value != 0));
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_lit_int(
    value: i64,
    out: *mut *mut phs_expr,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let out = out_expr(out)?;
            *out = into_expr_handle(lit(value));
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_lit_double(
    value: c_double,
    out: *mut *mut phs_expr,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let out = out_expr(out)?;
            *out = into_expr_handle(lit(value));
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_lit_text(
    value: *const c_char,
    out: *mut *mut phs_expr,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let value = cstr_to_str(value, "literal text")?;
            let out = out_expr(out)?;
            *out = into_expr_handle(lit(value.to_owned()));
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_alias(
    expr: *const phs_expr,
    name: *const c_char,
    out: *mut *mut phs_expr,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let expr = expr_ref(expr)?.value.clone();
            let name = cstr_to_str(name, "alias name")?;
            let out = out_expr(out)?;
            *out = into_expr_handle(expr.alias(name));
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_binary(
    op: c_int,
    left: *const phs_expr,
    right: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let left = expr_ref(left)?.value.clone();
            let right = expr_ref(right)?.value.clone();
            let out = out_expr(out)?;
            *out = into_expr_handle(apply_binary(op, left, right)?);
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_not(
    expr: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let expr = expr_ref(expr)?.value.clone();
            let out = out_expr(out)?;
            *out = into_expr_handle(expr.not());
            Ok(())
        })
    }
}
```

- [ ] **Step 3: Replace `rust/polars-hs-ffi/src/lazyframe.rs`**

```rust
use std::os::raw::{c_char, c_int};
use std::ptr;

use polars::prelude::*;

use crate::dataframe::pathbuf_from_c;
use crate::error::{PhsError, cstr_to_str, ffi_boundary};
use crate::handles::{expr_ref, into_dataframe_handle, into_lazyframe_handle, lazyframe_ref, phs_dataframe, phs_expr, phs_lazyframe};

unsafe fn out_lazyframe<'a>(out: *mut *mut phs_lazyframe) -> Result<&'a mut *mut phs_lazyframe, PhsError> {
    if out.is_null() {
        return Err(PhsError::invalid_argument("lazyframe out pointer was null"));
    }
    unsafe {
        *out = ptr::null_mut();
        Ok(&mut *out)
    }
}

unsafe fn out_dataframe<'a>(out: *mut *mut phs_dataframe) -> Result<&'a mut *mut phs_dataframe, PhsError> {
    if out.is_null() {
        return Err(PhsError::invalid_argument("dataframe out pointer was null"));
    }
    unsafe {
        *out = ptr::null_mut();
        Ok(&mut *out)
    }
}

unsafe fn expr_slice(exprs: *const *const phs_expr, len: usize) -> Result<Vec<Expr>, PhsError> {
    if exprs.is_null() && len > 0 {
        return Err(PhsError::invalid_argument("expression array pointer was null"));
    }
    let raw = unsafe { std::slice::from_raw_parts(exprs, len) };
    raw.iter()
        .map(|expr| unsafe { expr_ref(*expr).map(|handle| handle.value.clone()) })
        .collect()
}

unsafe fn text_slice(values: *const *const c_char, len: usize) -> Result<Vec<PlSmallStr>, PhsError> {
    if values.is_null() && len > 0 {
        return Err(PhsError::invalid_argument("text array pointer was null"));
    }
    let raw = unsafe { std::slice::from_raw_parts(values, len) };
    raw.iter()
        .enumerate()
        .map(|(index, value)| unsafe {
            cstr_to_str(*value, &format!("text array element {index}")).map(PlSmallStr::from_str)
        })
        .collect()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_scan_csv(
    path: *const c_char,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let path = cstr_to_str(path, "path")?;
            let out = out_lazyframe(out)?;
            let lf = LazyCsvReader::new(PlRefPath::new(path)).finish()?;
            *out = into_lazyframe_handle(lf);
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_scan_parquet(
    path: *const c_char,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let path = cstr_to_str(path, "path")?;
            let out = out_lazyframe(out)?;
            let lf = LazyFrame::scan_parquet(PlRefPath::new(path), Default::default())?;
            *out = into_lazyframe_handle(lf);
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_collect(
    lf: *const phs_lazyframe,
    out: *mut *mut phs_dataframe,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let lf = lazyframe_ref(lf)?.value.clone();
            let out = out_dataframe(out)?;
            *out = into_dataframe_handle(lf.collect()?);
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_filter(
    lf: *const phs_lazyframe,
    predicate: *const phs_expr,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let lf = lazyframe_ref(lf)?.value.clone();
            let predicate = expr_ref(predicate)?.value.clone();
            let out = out_lazyframe(out)?;
            *out = into_lazyframe_handle(lf.filter(predicate));
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_select(
    lf: *const phs_lazyframe,
    exprs: *const *const phs_expr,
    len: usize,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let lf = lazyframe_ref(lf)?.value.clone();
            let exprs = expr_slice(exprs, len)?;
            let out = out_lazyframe(out)?;
            *out = into_lazyframe_handle(lf.select(exprs));
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_with_columns(
    lf: *const phs_lazyframe,
    exprs: *const *const phs_expr,
    len: usize,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let lf = lazyframe_ref(lf)?.value.clone();
            let exprs = expr_slice(exprs, len)?;
            let out = out_lazyframe(out)?;
            *out = into_lazyframe_handle(lf.with_columns(exprs));
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_sort(
    lf: *const phs_lazyframe,
    names: *const *const c_char,
    len: usize,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let lf = lazyframe_ref(lf)?.value.clone();
            let names = text_slice(names, len)?;
            let out = out_lazyframe(out)?;
            *out = into_lazyframe_handle(lf.sort(names, Default::default()));
            Ok(())
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_limit(
    lf: *const phs_lazyframe,
    n: u32,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut crate::error::phs_error,
) -> c_int {
    unsafe {
        ffi_boundary(err, || {
            let lf = lazyframe_ref(lf)?.value.clone();
            let out = out_lazyframe(out)?;
            *out = into_lazyframe_handle(lf.limit(n));
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataframe::phs_dataframe_shape;
    use crate::expr::{PHS_OP_GT, phs_expr_binary, phs_expr_col, phs_expr_lit_int};
    use crate::handles::{phs_dataframe_free, phs_expr_free, phs_lazyframe_free};
    use std::ffi::CString;

    #[test]
    fn lazy_filter_select_collect() {
        let path = CString::new("../../test/data/people.csv").expect("valid cstring");
        let age = CString::new("age").expect("valid cstring");
        let name = CString::new("name").expect("valid cstring");
        let mut err = ptr::null_mut();
        let mut lf0 = ptr::null_mut();
        assert_eq!(unsafe { phs_scan_csv(path.as_ptr(), &mut lf0, &mut err) }, 0);
        let mut age_expr = ptr::null_mut();
        let mut lit_expr = ptr::null_mut();
        let mut predicate = ptr::null_mut();
        assert_eq!(unsafe { phs_expr_col(age.as_ptr(), &mut age_expr, &mut err) }, 0);
        assert_eq!(unsafe { phs_expr_lit_int(35, &mut lit_expr, &mut err) }, 0);
        assert_eq!(unsafe { phs_expr_binary(PHS_OP_GT, age_expr, lit_expr, &mut predicate, &mut err) }, 0);
        let mut lf1 = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_filter(lf0, predicate, &mut lf1, &mut err) }, 0);
        let mut name_expr = ptr::null_mut();
        assert_eq!(unsafe { phs_expr_col(name.as_ptr(), &mut name_expr, &mut err) }, 0);
        let exprs = [name_expr as *const phs_expr];
        let mut lf2 = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_select(lf1, exprs.as_ptr(), exprs.len(), &mut lf2, &mut err) }, 0);
        let mut df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(lf2, &mut df, &mut err) }, 0);
        let mut rows = 0_u64;
        let mut cols = 0_u64;
        assert_eq!(unsafe { phs_dataframe_shape(df, &mut rows, &mut cols, &mut err) }, 0);
        assert_eq!((rows, cols), (1, 1));
        unsafe { phs_expr_free(age_expr) };
        unsafe { phs_expr_free(lit_expr) };
        unsafe { phs_expr_free(predicate) };
        unsafe { phs_expr_free(name_expr) };
        unsafe { phs_lazyframe_free(lf0) };
        unsafe { phs_lazyframe_free(lf1) };
        unsafe { phs_lazyframe_free(lf2) };
        unsafe { phs_dataframe_free(df) };
    }
}
```

- [ ] **Step 4: Run Rust tests**

Run:

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
```

Expected: PASS.

- [ ] **Step 5: Commit the Rust lazy task**

Run:

```bash
jj status || true
jj commit -m "feat: add rust lazyframe and expression ffi" || true
```

Expected in a jj repository: a commit is created. Expected in the current checkout: jj reports that the directory is not a jj repo.

---

## Task 5: Implement Haskell raw FFI and internal helpers

**Files:**
- Create: `src/Polars/Internal/Raw.hs`
- Create: `src/Polars/Internal/Managed.hs`
- Create: `src/Polars/Internal/Result.hs`
- Create: `src/Polars/Internal/CString.hs`
- Create: `src/Polars/Internal/Bytes.hs`
- Create: `src/Polars/Internal/Expr.hs`

- [ ] **Step 1: Create `src/Polars/Internal/Raw.hs`**

```haskell
{-# LANGUAGE ForeignFunctionInterface #-}

{- |
Module      : Polars.Internal.Raw
Description : Raw C FFI imports for the Rust Polars adapter.

This module is the unsafe boundary. Public modules must wrap every pointer in a
managed type and convert status/error outputs into 'Polars.Error.PolarsError'.
-}
module Polars.Internal.Raw
    ( RawDataFrame
    , RawLazyFrame
    , RawExpr
    , RawError
    , RawBytes
    , phs_error_code
    , phs_error_message
    , phs_error_free
    , phs_bytes_data
    , phs_bytes_len
    , phs_bytes_free
    , phs_dataframe_free_funptr
    , phs_lazyframe_free_funptr
    , phs_expr_free_funptr
    , phs_read_csv
    , phs_read_parquet
    , phs_dataframe_shape
    , phs_dataframe_head
    , phs_dataframe_tail
    , phs_dataframe_schema
    , phs_dataframe_to_string
    , phs_dataframe_to_ipc_bytes
    , phs_dataframe_from_ipc_bytes
    , phs_dataframe_write_ipc_file
    , phs_dataframe_read_ipc_file
    , phs_scan_csv
    , phs_scan_parquet
    , phs_lazyframe_collect
    , phs_lazyframe_filter
    , phs_lazyframe_select
    , phs_lazyframe_with_columns
    , phs_lazyframe_sort
    , phs_lazyframe_limit
    , phs_expr_col
    , phs_expr_lit_bool
    , phs_expr_lit_int
    , phs_expr_lit_double
    , phs_expr_lit_text
    , phs_expr_alias
    , phs_expr_binary
    , phs_expr_not
    ) where

import Data.Int (Int64)
import Data.Word (Word32, Word64)
import Foreign (FunPtr, Ptr)
import Foreign.C (CChar, CDouble (..), CInt (..), CSize (..), CUChar)

-- | Opaque Rust-owned DataFrame handle.
data RawDataFrame

-- | Opaque Rust-owned LazyFrame handle.
data RawLazyFrame

-- | Opaque Rust-owned expression handle.
data RawExpr

-- | Opaque Rust-owned error handle.
data RawError

-- | Opaque Rust-owned byte buffer.
data RawBytes

foreign import ccall unsafe "phs_error_code"
    phs_error_code :: Ptr RawError -> IO CInt

foreign import ccall unsafe "phs_error_message"
    phs_error_message :: Ptr RawError -> IO (Ptr CChar)

foreign import ccall unsafe "phs_error_free"
    phs_error_free :: Ptr RawError -> IO ()

foreign import ccall unsafe "phs_bytes_data"
    phs_bytes_data :: Ptr RawBytes -> IO (Ptr CUChar)

foreign import ccall unsafe "phs_bytes_len"
    phs_bytes_len :: Ptr RawBytes -> IO CSize

foreign import ccall unsafe "phs_bytes_free"
    phs_bytes_free :: Ptr RawBytes -> IO ()

foreign import ccall unsafe "&phs_dataframe_free"
    phs_dataframe_free_funptr :: FunPtr (Ptr RawDataFrame -> IO ())

foreign import ccall unsafe "&phs_lazyframe_free"
    phs_lazyframe_free_funptr :: FunPtr (Ptr RawLazyFrame -> IO ())

foreign import ccall unsafe "&phs_expr_free"
    phs_expr_free_funptr :: FunPtr (Ptr RawExpr -> IO ())

foreign import ccall safe "phs_read_csv"
    phs_read_csv :: Ptr CChar -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_read_parquet"
    phs_read_parquet :: Ptr CChar -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_shape"
    phs_dataframe_shape :: Ptr RawDataFrame -> Ptr Word64 -> Ptr Word64 -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_head"
    phs_dataframe_head :: Ptr RawDataFrame -> Word64 -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_tail"
    phs_dataframe_tail :: Ptr RawDataFrame -> Word64 -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_schema"
    phs_dataframe_schema :: Ptr RawDataFrame -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_to_string"
    phs_dataframe_to_string :: Ptr RawDataFrame -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_to_ipc_bytes"
    phs_dataframe_to_ipc_bytes :: Ptr RawDataFrame -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_from_ipc_bytes"
    phs_dataframe_from_ipc_bytes :: Ptr CUChar -> CSize -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_write_ipc_file"
    phs_dataframe_write_ipc_file :: Ptr CChar -> Ptr RawDataFrame -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_read_ipc_file"
    phs_dataframe_read_ipc_file :: Ptr CChar -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_scan_csv"
    phs_scan_csv :: Ptr CChar -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_scan_parquet"
    phs_scan_parquet :: Ptr CChar -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_lazyframe_collect"
    phs_lazyframe_collect :: Ptr RawLazyFrame -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_lazyframe_filter"
    phs_lazyframe_filter :: Ptr RawLazyFrame -> Ptr RawExpr -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_lazyframe_select"
    phs_lazyframe_select :: Ptr RawLazyFrame -> Ptr (Ptr RawExpr) -> CSize -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_lazyframe_with_columns"
    phs_lazyframe_with_columns :: Ptr RawLazyFrame -> Ptr (Ptr RawExpr) -> CSize -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_lazyframe_sort"
    phs_lazyframe_sort :: Ptr RawLazyFrame -> Ptr (Ptr CChar) -> CSize -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_lazyframe_limit"
    phs_lazyframe_limit :: Ptr RawLazyFrame -> Word32 -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_expr_col"
    phs_expr_col :: Ptr CChar -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_expr_lit_bool"
    phs_expr_lit_bool :: CInt -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_expr_lit_int"
    phs_expr_lit_int :: Int64 -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_expr_lit_double"
    phs_expr_lit_double :: CDouble -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_expr_lit_text"
    phs_expr_lit_text :: Ptr CChar -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_expr_alias"
    phs_expr_alias :: Ptr RawExpr -> Ptr CChar -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_expr_binary"
    phs_expr_binary :: CInt -> Ptr RawExpr -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_expr_not"
    phs_expr_not :: Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt
```

- [ ] **Step 2: Create `src/Polars/Internal/Result.hs`**

```haskell
{- |
Module      : Polars.Internal.Result
Description : Convert Rust status/error outputs into typed Haskell errors.
-}
module Polars.Internal.Result
    ( consumeError
    , decodeErrorCode
    ) where

import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Foreign (Ptr, nullPtr)
import Foreign.C (CInt (..))

import Polars.Error (PolarsError (..), PolarsErrorCode (..))
import Polars.Internal.Raw (RawError, phs_error_code, phs_error_free, phs_error_message)

decodeErrorCode :: CInt -> PolarsErrorCode
decodeErrorCode code =
    case code of
        1 -> PolarsFailure
        2 -> InvalidArgument
        3 -> Utf8Error
        4 -> PanicError
        n -> UnknownError (fromIntegral n)

consumeError :: CInt -> Ptr RawError -> IO PolarsError
consumeError status errPtr
    | errPtr == nullPtr =
        pure $ PolarsError (decodeErrorCode status) "foreign call failed without error detail"
    | otherwise = do
        code <- phs_error_code errPtr
        messagePtr <- phs_error_message errPtr
        message <-
            if messagePtr == nullPtr
                then pure T.empty
                else TE.decodeUtf8 <$> BS.packCString messagePtr
        phs_error_free errPtr
        pure $ PolarsError (decodeErrorCode code) message
```

- [ ] **Step 3: Create `src/Polars/Internal/Managed.hs`**

```haskell
{- |
Module      : Polars.Internal.Managed
Description : Managed Haskell wrappers around Rust-owned Polars handles.
-}
module Polars.Internal.Managed
    ( DataFrame (..)
    , LazyFrame (..)
    , ManagedExpr (..)
    , mkDataFrame
    , mkLazyFrame
    , mkManagedExpr
    , withDataFrame
    , withLazyFrame
    , withManagedExpr
    ) where

import Foreign (ForeignPtr, Ptr, newForeignPtr, nullPtr, withForeignPtr)

import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))
import Polars.Internal.Raw
    ( RawDataFrame
    , RawExpr
    , RawLazyFrame
    , phs_dataframe_free_funptr
    , phs_expr_free_funptr
    , phs_lazyframe_free_funptr
    )

newtype DataFrame = DataFrame (ForeignPtr RawDataFrame)
newtype LazyFrame = LazyFrame (ForeignPtr RawLazyFrame)
newtype ManagedExpr = ManagedExpr (ForeignPtr RawExpr)

mkDataFrame :: Ptr RawDataFrame -> IO (Either PolarsError DataFrame)
mkDataFrame ptr
    | ptr == nullPtr = pure $ Left $ PolarsError InvalidArgument "Rust returned a null DataFrame pointer"
    | otherwise = Right . DataFrame <$> newForeignPtr phs_dataframe_free_funptr ptr

mkLazyFrame :: Ptr RawLazyFrame -> IO (Either PolarsError LazyFrame)
mkLazyFrame ptr
    | ptr == nullPtr = pure $ Left $ PolarsError InvalidArgument "Rust returned a null LazyFrame pointer"
    | otherwise = Right . LazyFrame <$> newForeignPtr phs_lazyframe_free_funptr ptr

mkManagedExpr :: Ptr RawExpr -> IO (Either PolarsError ManagedExpr)
mkManagedExpr ptr
    | ptr == nullPtr = pure $ Left $ PolarsError InvalidArgument "Rust returned a null Expr pointer"
    | otherwise = Right . ManagedExpr <$> newForeignPtr phs_expr_free_funptr ptr

withDataFrame :: DataFrame -> (Ptr RawDataFrame -> IO a) -> IO a
withDataFrame (DataFrame fp) = withForeignPtr fp

withLazyFrame :: LazyFrame -> (Ptr RawLazyFrame -> IO a) -> IO a
withLazyFrame (LazyFrame fp) = withForeignPtr fp

withManagedExpr :: ManagedExpr -> (Ptr RawExpr -> IO a) -> IO a
withManagedExpr (ManagedExpr fp) = withForeignPtr fp
```

- [ ] **Step 4: Create `src/Polars/Internal/CString.hs`**

```haskell
{- |
Module      : Polars.Internal.CString
Description : Scoped UTF-8 CString helpers for the Polars FFI.
-}
module Polars.Internal.CString
    ( withTextCString
    , withFilePathCString
    ) where

import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Foreign.C (CString)

withTextCString :: T.Text -> (CString -> IO a) -> IO a
withTextCString text action =
    BS.useAsCString (TE.encodeUtf8 text) action

withFilePathCString :: FilePath -> (CString -> IO a) -> IO a
withFilePathCString path action =
    BS.useAsCString (TE.encodeUtf8 (T.pack path)) action
```

- [ ] **Step 5: Create `src/Polars/Internal/Bytes.hs`**

```haskell
{- |
Module      : Polars.Internal.Bytes
Description : Copy Rust-owned byte buffers into Haskell-owned ByteStrings.
-}
module Polars.Internal.Bytes
    ( copyAndFreeBytes
    ) where

import qualified Data.ByteString as BS
import Foreign (Ptr, castPtr, nullPtr)
import Foreign.C (CSize (..))

import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))
import Polars.Internal.Raw (RawBytes, phs_bytes_data, phs_bytes_free, phs_bytes_len)

copyAndFreeBytes :: Ptr RawBytes -> IO (Either PolarsError BS.ByteString)
copyAndFreeBytes ptr
    | ptr == nullPtr = pure $ Left $ PolarsError InvalidArgument "Rust returned a null byte buffer"
    | otherwise = do
        len <- phs_bytes_len ptr
        dataPtr <- phs_bytes_data ptr
        bytes <- BS.packCStringLen (castPtr dataPtr, fromIntegral (len :: CSize))
        phs_bytes_free ptr
        pure (Right bytes)
```

- [ ] **Step 6: Create `src/Polars/Internal/Expr.hs`**

```haskell
{- |
Module      : Polars.Internal.Expr
Description : Compile pure Haskell expressions into temporary Rust expression handles.
-}
module Polars.Internal.Expr
    ( compileExpr
    , withCompiledExprs
    , withTextArray
    ) where

import Control.Exception (bracket)
import Data.Int (Int64)
import qualified Data.Text as T
import Foreign (Ptr, alloca, allocaArray, peek, poke, pokeArray, nullPtr, withArray)
import Foreign.C (CDouble (..), CInt (..), CSize (..), CString)

import Polars.Error (PolarsError)
import Polars.Expr (BinaryOperator (..), Expr (..))
import Polars.Internal.CString (withTextCString)
import Polars.Internal.Managed (ManagedExpr, mkManagedExpr, withManagedExpr)
import Polars.Internal.Raw
    ( RawError
    , RawExpr
    , phs_expr_alias
    , phs_expr_binary
    , phs_expr_col
    , phs_expr_lit_bool
    , phs_expr_lit_double
    , phs_expr_lit_int
    , phs_expr_lit_text
    , phs_expr_not
    )
import Polars.Internal.Result (consumeError)

opCode :: BinaryOperator -> CInt
opCode operator =
    case operator of
        Eq -> 1
        NotEq -> 2
        Gt -> 3
        GtEq -> 4
        Lt -> 5
        LtEq -> 6
        And -> 7
        Or -> 8
        Add -> 9
        Subtract -> 10
        Multiply -> 11
        Divide -> 12

makeExpr :: (Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError ManagedExpr)
makeExpr call =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- call outPtr errPtr
            if status == 0
                then peek outPtr >>= mkManagedExpr
                else peek errPtr >>= consumeError status >>= pure . Left

compileExpr :: Expr -> IO (Either PolarsError ManagedExpr)
compileExpr expr =
    case expr of
        Column name ->
            withTextCString name $ \cName ->
                makeExpr $ \out err -> phs_expr_col cName out err
        LiteralBool value ->
            makeExpr $ \out err -> phs_expr_lit_bool (if value then 1 else 0) out err
        LiteralInt value ->
            makeExpr $ \out err -> phs_expr_lit_int value out err
        LiteralDouble value ->
            makeExpr $ \out err -> phs_expr_lit_double (CDouble value) out err
        LiteralText value ->
            withTextCString value $ \cValue ->
                makeExpr $ \out err -> phs_expr_lit_text cValue out err
        Alias name inner -> do
            compiled <- compileExpr inner
            case compiled of
                Left err -> pure (Left err)
                Right managed ->
                    withManagedExpr managed $ \ptr ->
                        withTextCString name $ \cName ->
                            makeExpr $ \out err -> phs_expr_alias ptr cName out err
        Binary operator left right -> do
            compiledLeft <- compileExpr left
            compiledRight <- compileExpr right
            case (compiledLeft, compiledRight) of
                (Right leftManaged, Right rightManaged) ->
                    withManagedExpr leftManaged $ \leftPtr ->
                        withManagedExpr rightManaged $ \rightPtr ->
                            makeExpr $ \out err -> phs_expr_binary (opCode operator) leftPtr rightPtr out err
                (Left err, _) -> pure (Left err)
                (_, Left err) -> pure (Left err)
        Not inner -> do
            compiled <- compileExpr inner
            case compiled of
                Left err -> pure (Left err)
                Right managed ->
                    withManagedExpr managed $ \ptr ->
                        makeExpr $ \out err -> phs_expr_not ptr out err

withCompiledExprs :: [Expr] -> (Ptr (Ptr RawExpr) -> CSize -> IO (Either PolarsError a)) -> IO (Either PolarsError a)
withCompiledExprs exprs action = do
    compiled <- traverse compileExpr exprs
    case sequence compiled of
        Left err -> pure (Left err)
        Right managedExprs -> do
            let withPtrs [] run = run []
                withPtrs (item : rest) run =
                    withManagedExpr item $ \ptr ->
                        withPtrs rest $ \ptrs -> run (ptr : ptrs)
            withPtrs managedExprs $ \ptrs ->
                withArray ptrs $ \arrayPtr -> action arrayPtr (fromIntegral (length ptrs))

withTextArray :: [T.Text] -> (Ptr CString -> CSize -> IO a) -> IO a
withTextArray values action =
    go values []
  where
    go [] acc = withArray (reverse acc) $ \ptr -> action ptr (fromIntegral (length acc))
    go (value : rest) acc = withTextCString value $ \cValue -> go rest (cValue : acc)
```

- [ ] **Step 7: Run Haskell build to verify expected public-module failures remain**

Run:

```bash
stack build --fast
```

Expected: FAIL mentioning missing public modules such as `Polars.DataFrame`, because Task 6 creates them.

- [ ] **Step 8: Commit the Haskell internal task**

Run:

```bash
jj status || true
jj commit -m "feat: add haskell ffi internals" || true
```

Expected in a jj repository: a commit is created. Expected in the current checkout: jj reports that the directory is not a jj repo.

---

## Task 6: Implement Haskell public API modules

**Files:**
- Create: `src/Polars/Error.hs`
- Create: `src/Polars/Schema.hs`
- Create: `src/Polars/Expr.hs`
- Create: `src/Polars/Operators.hs`
- Create: `src/Polars/DataFrame.hs`
- Create: `src/Polars/LazyFrame.hs`
- Create: `src/Polars/IPC.hs`
- Create: `src/Polars.hs`
- Modify: `app/Main.hs`

- [ ] **Step 1: Create `src/Polars/Error.hs`**

```haskell
{- |
Module      : Polars.Error
Description : Typed errors returned by the Polars Haskell binding.
-}
module Polars.Error
    ( PolarsErrorCode (..)
    , PolarsError (..)
    ) where

import qualified Data.Text as T

data PolarsErrorCode
    = PolarsFailure
    | InvalidArgument
    | Utf8Error
    | PanicError
    | UnknownError !Int
    deriving (Eq, Show)

data PolarsError = PolarsError
    { polarsErrorCode :: !PolarsErrorCode
    , polarsErrorMessage :: !T.Text
    }
    deriving (Eq, Show)
```

- [ ] **Step 2: Create `src/Polars/Schema.hs`**

```haskell
{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.Schema
Description : DataFrame schema types and decoding helpers.
-}
module Polars.Schema
    ( DataType (..)
    , Field (..)
    , parseSchemaBytes
    ) where

import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE

import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))

data DataType
    = Boolean
    | Int8
    | Int16
    | Int32
    | Int64
    | UInt8
    | UInt16
    | UInt32
    | UInt64
    | Float32
    | Float64
    | Utf8
    | Date
    | Datetime
    | Duration
    | Time
    | Binary
    | Null
    | Categorical
    | UnknownType !T.Text
    deriving (Eq, Show)

data Field = Field
    { fieldName :: !T.Text
    , fieldType :: !DataType
    }
    deriving (Eq, Show)

parseSchemaBytes :: BS.ByteString -> Either PolarsError [Field]
parseSchemaBytes bytes =
    parsePairs (dropTrailingEmpty (BS.split 0 bytes))
  where
    dropTrailingEmpty chunks =
        case reverse chunks of
            empty : rest | BS.null empty -> reverse rest
            _ -> chunks

    parsePairs [] = Right []
    parsePairs (nameBytes : typeBytes : rest) = do
        name <- decode nameBytes
        dtypeText <- decode typeBytes
        fields <- parsePairs rest
        Right (Field name (parseDataType dtypeText) : fields)
    parsePairs [_] = Left $ PolarsError InvalidArgument "schema byte buffer contained an odd number of fields"

    decode chunk =
        case TE.decodeUtf8' chunk of
            Left err -> Left $ PolarsError InvalidArgument (T.pack (show err))
            Right text -> Right text

parseDataType :: T.Text -> DataType
parseDataType text =
    case text of
        "Boolean" -> Boolean
        "Int8" -> Int8
        "Int16" -> Int16
        "Int32" -> Int32
        "Int64" -> Int64
        "UInt8" -> UInt8
        "UInt16" -> UInt16
        "UInt32" -> UInt32
        "UInt64" -> UInt64
        "Float32" -> Float32
        "Float64" -> Float64
        "String" -> Utf8
        "Utf8" -> Utf8
        "Date" -> Date
        "Datetime" -> Datetime
        "Duration" -> Duration
        "Time" -> Time
        "Binary" -> Binary
        "Null" -> Null
        "Categorical" -> Categorical
        other -> UnknownType other
```

- [ ] **Step 3: Create `src/Polars/Expr.hs`**

```haskell
{- |
Module      : Polars.Expr
Description : Pure expression AST for Polars lazy queries.
-}
module Polars.Expr
    ( Expr (..)
    , BinaryOperator (..)
    , col
    , litBool
    , litInt
    , litDouble
    , litText
    , alias
    , not_
    ) where

import Data.Int (Int64)
import qualified Data.Text as T

data Expr
    = Column !T.Text
    | LiteralBool !Bool
    | LiteralInt !Int64
    | LiteralDouble !Double
    | LiteralText !T.Text
    | Alias !T.Text !Expr
    | Binary !BinaryOperator !Expr !Expr
    | Not !Expr
    deriving (Eq, Show)

data BinaryOperator
    = Eq
    | NotEq
    | Gt
    | GtEq
    | Lt
    | LtEq
    | And
    | Or
    | Add
    | Subtract
    | Multiply
    | Divide
    deriving (Eq, Show)

col :: T.Text -> Expr
col = Column

litBool :: Bool -> Expr
litBool = LiteralBool

litInt :: Int64 -> Expr
litInt = LiteralInt

litDouble :: Double -> Expr
litDouble = LiteralDouble

litText :: T.Text -> Expr
litText = LiteralText

alias :: T.Text -> Expr -> Expr
alias = Alias

not_ :: Expr -> Expr
not_ = Not
```

- [ ] **Step 4: Create `src/Polars/Operators.hs`**

```haskell
{- |
Module      : Polars.Operators
Description : Infix operators for pure Polars expressions.
-}
module Polars.Operators
    ( (.==)
    , (.!=)
    , (.>)
    , (.>=)
    , (.<)
    , (.<=)
    , (.&&)
    , (.||)
    , (+.)
    , (-.)
    , (*.)
    , (/.)
    ) where

import Polars.Expr (BinaryOperator (..), Expr (Binary))

infix 4 .==, .!=, .>, .>=, .<, .<=
infixr 3 .&&
infixr 2 .||
infixl 6 +., -.
infixl 7 *., /.

(.==), (.!=), (.>), (.>=), (.<), (.<=), (.&&), (.||), (+.), (-.), (*.), (/.) :: Expr -> Expr -> Expr
(.==) = Binary Eq
(.!=) = Binary NotEq
(.>) = Binary Gt
(.>=) = Binary GtEq
(.<) = Binary Lt
(.<=) = Binary LtEq
(.&&) = Binary And
(.||) = Binary Or
(+.) = Binary Add
(-.) = Binary Subtract
(*.) = Binary Multiply
(/.) = Binary Divide
```

- [ ] **Step 5: Create `src/Polars/DataFrame.hs`**

```haskell
{-# LANGUAGE ScopedTypeVariables #-}

{- |
Module      : Polars.DataFrame
Description : Safe eager DataFrame operations backed by Rust Polars handles.
-}
module Polars.DataFrame
    ( DataFrame
    , readCsv
    , readParquet
    , height
    , width
    , shape
    , schema
    , head
    , tail
    , toText
    , toIpcBytes
    , fromIpcBytes
    ) where

import Prelude hiding (head, tail)

import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Word (Word64)
import Foreign (Ptr, alloca, castPtr, nullPtr, peek, poke, withForeignPtr)
import Foreign.C (CInt (..), CSize (..), CUChar)

import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))
import Polars.Internal.Bytes (copyAndFreeBytes)
import Polars.Internal.CString (withFilePathCString)
import Polars.Internal.Managed (DataFrame, mkDataFrame, withDataFrame)
import Polars.Internal.Raw
    ( RawBytes
    , RawDataFrame
    , RawError
    , phs_dataframe_from_ipc_bytes
    , phs_dataframe_head
    , phs_dataframe_schema
    , phs_dataframe_shape
    , phs_dataframe_tail
    , phs_dataframe_to_ipc_bytes
    , phs_dataframe_to_string
    , phs_read_csv
    , phs_read_parquet
    )
import Polars.Internal.Result (consumeError)
import Polars.Schema (Field, parseSchemaBytes)

readCsv :: FilePath -> IO (Either PolarsError DataFrame)
readCsv path = withFilePathCString path $ \cPath -> dataframeOut $ phs_read_csv cPath

readParquet :: FilePath -> IO (Either PolarsError DataFrame)
readParquet path = withFilePathCString path $ \cPath -> dataframeOut $ phs_read_parquet cPath

height :: DataFrame -> IO (Either PolarsError Int)
height df = fmap fst <$> shape df

width :: DataFrame -> IO (Either PolarsError Int)
width df = fmap snd <$> shape df

shape :: DataFrame -> IO (Either PolarsError (Int, Int))
shape df =
    withDataFrame df $ \dfPtr ->
        alloca $ \rowsPtr ->
            alloca $ \colsPtr ->
                alloca $ \errPtr -> do
                    poke errPtr nullPtr
                    status <- phs_dataframe_shape dfPtr rowsPtr colsPtr errPtr
                    if status == 0
                        then do
                            rows <- peek rowsPtr
                            cols <- peek colsPtr
                            pure $ Right (fromIntegral (rows :: Word64), fromIntegral (cols :: Word64))
                        else peek errPtr >>= consumeError status >>= pure . Left

schema :: DataFrame -> IO (Either PolarsError [Field])
schema df = do
    bytesResult <- dataframeBytes df phs_dataframe_schema
    pure $ bytesResult >>= parseSchemaBytes

head :: Int -> DataFrame -> IO (Either PolarsError DataFrame)
head n df
    | n < 0 = pure $ Left $ PolarsError InvalidArgument "head count must be non-negative"
    | otherwise = withDataFrame df $ \dfPtr -> dataframeOut $ phs_dataframe_head dfPtr (fromIntegral n)

tail :: Int -> DataFrame -> IO (Either PolarsError DataFrame)
tail n df
    | n < 0 = pure $ Left $ PolarsError InvalidArgument "tail count must be non-negative"
    | otherwise = withDataFrame df $ \dfPtr -> dataframeOut $ phs_dataframe_tail dfPtr (fromIntegral n)

toText :: DataFrame -> IO (Either PolarsError T.Text)
toText df = do
    bytesResult <- dataframeBytes df phs_dataframe_to_string
    pure $ TE.decodeUtf8 <$> bytesResult

toIpcBytes :: DataFrame -> IO (Either PolarsError BS.ByteString)
toIpcBytes df = dataframeBytes df phs_dataframe_to_ipc_bytes

fromIpcBytes :: BS.ByteString -> IO (Either PolarsError DataFrame)
fromIpcBytes bytes =
    BS.useAsCStringLen bytes $ \(dataPtr, len) ->
        alloca $ \outPtr ->
            alloca $ \errPtr -> do
                poke outPtr nullPtr
                poke errPtr nullPtr
                status <- phs_dataframe_from_ipc_bytes (castPtr dataPtr :: Ptr CUChar) (fromIntegral len :: CSize) outPtr errPtr
                if status == 0
                    then peek outPtr >>= mkDataFrame
                    else peek errPtr >>= consumeError status >>= pure . Left

dataframeOut :: (Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError DataFrame)
dataframeOut call =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- call outPtr errPtr
            if status == 0
                then peek outPtr >>= mkDataFrame
                else peek errPtr >>= consumeError status >>= pure . Left

dataframeBytes :: DataFrame -> (Ptr RawDataFrame -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError BS.ByteString)
dataframeBytes df call =
    withDataFrame df $ \dfPtr ->
        alloca $ \outPtr ->
            alloca $ \errPtr -> do
                poke outPtr nullPtr
                poke errPtr nullPtr
                status <- call dfPtr outPtr errPtr
                if status == 0
                    then peek outPtr >>= copyAndFreeBytes
                    else peek errPtr >>= consumeError status >>= pure . Left
```

- [ ] **Step 6: Create `src/Polars/LazyFrame.hs`**

```haskell
{- |
Module      : Polars.LazyFrame
Description : Safe lazy query operations backed by Rust Polars LazyFrame handles.
-}
module Polars.LazyFrame
    ( LazyFrame
    , scanCsv
    , scanParquet
    , collect
    , select
    , filter
    , withColumns
    , sort
    , limit
    ) where

import Prelude hiding (filter)

import qualified Data.Text as T
import Data.Word (Word32)
import Foreign (Ptr, alloca, nullPtr, peek, poke)
import Foreign.C (CInt (..), CSize (..))

import Polars.DataFrame (DataFrame)
import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))
import Polars.Expr (Expr)
import Polars.Internal.CString (withFilePathCString)
import Polars.Internal.Expr (compileExpr, withCompiledExprs, withTextArray)
import Polars.Internal.Managed (LazyFrame, mkDataFrame, mkLazyFrame, withLazyFrame, withManagedExpr)
import Polars.Internal.Raw
    ( RawDataFrame
    , RawError
    , RawLazyFrame
    , phs_lazyframe_collect
    , phs_lazyframe_filter
    , phs_lazyframe_limit
    , phs_lazyframe_select
    , phs_lazyframe_sort
    , phs_lazyframe_with_columns
    , phs_scan_csv
    , phs_scan_parquet
    )
import Polars.Internal.Result (consumeError)

scanCsv :: FilePath -> IO (Either PolarsError LazyFrame)
scanCsv path = withFilePathCString path $ \cPath -> lazyFrameOut $ phs_scan_csv cPath

scanParquet :: FilePath -> IO (Either PolarsError LazyFrame)
scanParquet path = withFilePathCString path $ \cPath -> lazyFrameOut $ phs_scan_parquet cPath

collect :: LazyFrame -> IO (Either PolarsError DataFrame)
collect lf =
    withLazyFrame lf $ \lfPtr ->
        alloca $ \outPtr ->
            alloca $ \errPtr -> do
                poke outPtr nullPtr
                poke errPtr nullPtr
                status <- phs_lazyframe_collect lfPtr outPtr errPtr
                if status == 0
                    then peek outPtr >>= mkDataFrame
                    else peek errPtr >>= consumeError status >>= pure . Left

select :: [Expr] -> LazyFrame -> IO (Either PolarsError LazyFrame)
select exprs lf =
    withLazyFrame lf $ \lfPtr ->
        withCompiledExprs exprs $ \exprArray len -> lazyFrameOut $ phs_lazyframe_select lfPtr exprArray len

filter :: Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
filter predicate lf = do
    compiled <- compileExpr predicate
    case compiled of
        Left err -> pure (Left err)
        Right managed ->
            withLazyFrame lf $ \lfPtr ->
                withManagedExpr managed $ \exprPtr ->
                    lazyFrameOut $ phs_lazyframe_filter lfPtr exprPtr

withColumns :: [Expr] -> LazyFrame -> IO (Either PolarsError LazyFrame)
withColumns exprs lf =
    withLazyFrame lf $ \lfPtr ->
        withCompiledExprs exprs $ \exprArray len -> lazyFrameOut $ phs_lazyframe_with_columns lfPtr exprArray len

sort :: [T.Text] -> LazyFrame -> IO (Either PolarsError LazyFrame)
sort names lf =
    withLazyFrame lf $ \lfPtr ->
        withTextArray names $ \nameArray len -> lazyFrameOut $ phs_lazyframe_sort lfPtr nameArray len

limit :: Word32 -> LazyFrame -> IO (Either PolarsError LazyFrame)
limit n lf = withLazyFrame lf $ \lfPtr -> lazyFrameOut $ phs_lazyframe_limit lfPtr n

lazyFrameOut :: (Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError LazyFrame)
lazyFrameOut call =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- call outPtr errPtr
            if status == 0
                then peek outPtr >>= mkLazyFrame
                else peek errPtr >>= consumeError status >>= pure . Left
```

- [ ] **Step 7: Create `src/Polars/IPC.hs`**

```haskell
{- |
Module      : Polars.IPC
Description : Arrow IPC byte and file helpers for DataFrame interchange.
-}
module Polars.IPC
    ( toIpcBytes
    , fromIpcBytes
    , writeIpcFile
    , readIpcFile
    ) where

import qualified Data.ByteString as BS
import Foreign (alloca, nullPtr, peek, poke)
import Foreign.C (CInt (..))

import Polars.DataFrame (DataFrame, fromIpcBytes, toIpcBytes)
import Polars.Error (PolarsError)
import Polars.Internal.CString (withFilePathCString)
import Polars.Internal.Managed (mkDataFrame, withDataFrame)
import Polars.Internal.Raw (RawDataFrame, RawError, phs_dataframe_read_ipc_file, phs_dataframe_write_ipc_file)
import Polars.Internal.Result (consumeError)

writeIpcFile :: FilePath -> DataFrame -> IO (Either PolarsError ())
writeIpcFile path df =
    withDataFrame df $ \dfPtr ->
        withFilePathCString path $ \cPath ->
            alloca $ \errPtr -> do
                poke errPtr nullPtr
                status <- phs_dataframe_write_ipc_file cPath dfPtr errPtr
                if status == 0
                    then pure (Right ())
                    else peek errPtr >>= consumeError status >>= pure . Left

readIpcFile :: FilePath -> IO (Either PolarsError DataFrame)
readIpcFile path =
    withFilePathCString path $ \cPath ->
        alloca $ \outPtr ->
            alloca $ \errPtr -> do
                poke outPtr nullPtr
                poke errPtr nullPtr
                status <- phs_dataframe_read_ipc_file cPath outPtr errPtr
                if status == 0
                    then peek outPtr >>= mkDataFrame
                    else peek errPtr >>= consumeError status >>= pure . Left
```

- [ ] **Step 8: Create `src/Polars.hs`**

```haskell
{- |
Module      : Polars
Description : Convenience re-export module for the Polars Haskell binding MVP.
-}
module Polars
    ( module Polars.DataFrame
    , module Polars.Error
    , module Polars.Expr
    , module Polars.IPC
    , module Polars.LazyFrame
    , module Polars.Operators
    , module Polars.Schema
    ) where

import Polars.DataFrame
import Polars.Error
import Polars.Expr
import Polars.IPC
import Polars.LazyFrame
import Polars.Operators
import Polars.Schema
```

- [ ] **Step 9: Replace `app/Main.hs` with a tiny smoke executable**

```haskell
module Main (main) where

import qualified Polars as Pl

main :: IO ()
main = do
    result <- Pl.readCsv "test/data/people.csv"
    case result of
        Left err -> print err
        Right df -> print =<< Pl.shape df
```

- [ ] **Step 10: Remove the scaffold module `src/Lib.hs`**

Run:

```bash
rm -f src/Lib.hs
```

Expected: `src/Lib.hs` is removed because the library now exposes `Polars` modules.

- [ ] **Step 11: Run Haskell tests**

Run:

```bash
stack test --fast
```

Expected: PASS. The build hook should compile the Rust adapter before GHC links the test executable.

- [ ] **Step 12: Commit the public Haskell API task**

Run:

```bash
jj status || true
jj commit -m "feat: expose safe polars haskell api" || true
```

Expected in a jj repository: a commit is created. Expected in the current checkout: jj reports that the directory is not a jj repo.

---

## Task 7: Add example, README quickstart, and verification commands

**Files:**
- Create: `examples/iris.hs`
- Modify: `README.md`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Create `examples/iris.hs`**

```haskell
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import qualified Polars as Pl

main :: IO ()
main = do
    scanResult <- Pl.scanCsv "test/data/people.csv"
    case scanResult of
        Left err -> print err
        Right lf0 -> do
            filtered <- Pl.filter (Pl.col "age" Pl..> Pl.litInt 35) lf0
            case filtered of
                Left err -> print err
                Right lf1 -> do
                    selected <- Pl.select [Pl.col "name"] lf1
                    case selected of
                        Left err -> print err
                        Right lf2 -> do
                            collected <- Pl.collect lf2
                            case collected of
                                Left err -> print err
                                Right df -> do
                                    print =<< Pl.shape df
                                    rendered <- Pl.toText df
                                    either print putStrLn rendered
```

- [ ] **Step 2: Replace `README.md` with the MVP quickstart**

```markdown
# polars-hs

`polars-hs` provides Haskell bindings to the Rust Polars dataframe engine.

## Build matrix

```yaml
resolver: nightly-2026-04-26
compiler: ghc-9.12.2
```

The Rust adapter pins the Polars `0.53.0` crate family in `rust/polars-hs-ffi/Cargo.lock`.

## Requirements

- Stack 3.9 or newer
- GHC 9.12.2 through Stack
- Rust toolchain with Cargo
- A C linker that can link Rust static libraries with GHC

## Quickstart

```haskell
{-# LANGUAGE OverloadedStrings #-}

import qualified Polars as Pl

main :: IO ()
main = do
    result <- Pl.scanCsv "test/data/people.csv"
    case result of
        Left err -> print err
        Right lf0 -> do
            filtered <- Pl.filter (Pl.col "age" Pl..> Pl.litInt 35) lf0
            selected <- either (pure . Left) (Pl.select [Pl.col "name"]) filtered
            collected <- either (pure . Left) Pl.collect selected
            case collected of
                Left err -> print err
                Right df -> print =<< Pl.shape df
```

## Development commands

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
cargo clippy --manifest-path rust/polars-hs-ffi/Cargo.toml -- -D warnings
stack test --fast
hlint src app test
```

## Architecture

The package uses a Rust adapter crate that exposes a small `phs_*` C ABI. Haskell wraps Rust-owned DataFrame and LazyFrame handles in `ForeignPtr` finalizers and returns `Either PolarsError a` for recoverable failures. Expressions are a pure Haskell AST and are compiled into temporary Rust expression handles at FFI call sites.
```

- [ ] **Step 3: Update `CHANGELOG.md`**

```markdown
# Changelog for `polars-hs`

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to the
[Haskell Package Versioning Policy](https://pvp.haskell.org/).

## Unreleased

### Added

- Rust Polars adapter crate with `phs_*` C ABI.
- Safe Haskell DataFrame and LazyFrame handles.
- CSV and Parquet readers.
- Lazy scan, filter, select, sort, limit, and collect operations.
- Pure Haskell expression AST.
- Arrow IPC byte and file round-trip helpers.
- Hspec integration tests for the MVP API.

## 0.1.0.0 - YYYY-MM-DD
```

- [ ] **Step 4: Run all verification commands**

Run:

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
cargo clippy --manifest-path rust/polars-hs-ffi/Cargo.toml -- -D warnings
stack test --fast
hlint src app test
```

Expected: all commands PASS.

- [ ] **Step 5: Commit docs and verification task**

Run:

```bash
jj status || true
jj commit -m "docs: add polars binding quickstart" || true
```

Expected in a jj repository: a commit is created. Expected in the current checkout: jj reports that the directory is not a jj repo.

---

## Self-Review Checklist

### Spec coverage

- Rust adapter crate: Task 2, Task 3, Task 4.
- Stable `phs_*` C ABI: Task 2 through Task 4.
- Haskell raw FFI: Task 5.
- Managed `ForeignPtr` handles: Task 5.
- Typed `PolarsError`: Task 5 and Task 6.
- Pure expression AST: Task 5 and Task 6.
- Eager CSV/Parquet operations: Task 3 and Task 6.
- Lazy scan/filter/select/collect operations: Task 4 and Task 6.
- IPC byte/file round-trip: Task 3, Task 6, Task 7.
- Module documentation: Task 5 and Task 6 include Haddock headers.
- Verification commands: Task 7.

### Completeness scan

Every code step contains concrete file content, and each verification step names the exact command and expected result.

### Type consistency

- Rust opaque types are `phs_dataframe`, `phs_lazyframe`, `phs_expr`, `phs_error`, and `phs_bytes`.
- Haskell raw types are `RawDataFrame`, `RawLazyFrame`, `RawExpr`, `RawError`, and `RawBytes`.
- Public Haskell managed types are `DataFrame`, `LazyFrame`, and pure `Expr`.
- Error conversion consistently uses `consumeError`.
- Rust byte buffers consistently use `phs_bytes_*` and Haskell `copyAndFreeBytes`.

### Known implementation checkpoint

If static Rust linking reports missing native libraries, run:

```bash
cd rust/polars-hs-ffi
cargo rustc --release --lib -- --print native-static-libs
```

Then add the printed system libraries to `package.yaml` under `library.extra-libraries` while keeping `polars_hs_ffi` first.
