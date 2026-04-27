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
