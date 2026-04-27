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
