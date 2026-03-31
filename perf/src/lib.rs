#![cfg(feature = "agave-unstable-api")]
pub mod data_budget;
pub mod deduper;
pub mod packet;
pub mod recycled_vec;
pub mod recycler;
pub mod sigverify;
#[cfg(feature = "dev-context-only-utils")]
pub mod test_tx;
pub mod thread;
mod trace;

#[macro_use]
extern crate log;

#[cfg(test)]
extern crate assert_matches;

#[macro_use]
extern crate solana_metrics;

#[cfg_attr(feature = "stable-abi", macro_use)]
#[cfg(feature = "stable-abi")]
extern crate solana_frozen_abi_macro;

use {
    crate::packet::{PacketRef, PacketRefMut},
    solana_short_vec::decode_shortu16_len,
    solana_signature::SIGNATURE_BYTES,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PacketError {
    InvalidLen,
    InvalidShortVec,
    InvalidSignatureLen,
}

fn is_rosetta_emulated() -> bool {
    #[cfg(target_os = "macos")]
    {
        use std::str::FromStr;
        std::process::Command::new("sysctl")
            .args(["-in", "sysctl.proc_translated"])
            .output()
            .map_err(|_| ())
            .and_then(|output| String::from_utf8(output.stdout).map_err(|_| ()))
            .and_then(|stdout| u8::from_str(stdout.trim()).map_err(|_| ()))
            .map(|enabled| enabled == 1)
            .unwrap_or(false)
    }
    #[cfg(not(target_os = "macos"))]
    {
        false
    }
}

pub fn report_target_features() {
    // Validator binaries built on a machine with AVX support will generate invalid opcodes
    // when run on machines without AVX causing a non-obvious process abort.  Instead detect
    // the mismatch and error cleanly.
    if !is_rosetta_emulated() {
        #[cfg(all(
            any(target_arch = "x86", target_arch = "x86_64"),
            build_target_feature_avx
        ))]
        {
            if is_x86_feature_detected!("avx") {
                info!("AVX detected");
            } else {
                error!(
                    "Incompatible CPU detected: missing AVX support. Please build from source on \
                     the target"
                );
                std::process::abort();
            }
        }

        #[cfg(all(
            any(target_arch = "x86", target_arch = "x86_64"),
            build_target_feature_avx2
        ))]
        {
            if is_x86_feature_detected!("avx2") {
                info!("AVX2 detected");
            } else {
                error!(
                    "Incompatible CPU detected: missing AVX2 support. Please build from source on \
                     the target"
                );
                std::process::abort();
            }
        }
    }
}

pub fn get_signature_from_packet<'a>(
    packet: &'a PacketRef<'a>,
) -> Result<&'a [u8; SIGNATURE_BYTES], PacketError> {
    let packet_data = packet.data(..).ok_or(PacketError::InvalidLen)?;
    let (num_signatures, signature_offset) =
        decode_shortu16_len(packet_data).map_err(|_| PacketError::InvalidShortVec)?;
    if num_signatures == 0 {
        return Err(PacketError::InvalidSignatureLen);
    }

    let signature_end = signature_offset
        .checked_add(SIGNATURE_BYTES)
        .ok_or(PacketError::InvalidLen)?;
    let signature = packet
        .data(signature_offset..signature_end)
        .ok_or(PacketError::InvalidSignatureLen)?;
    <&[u8; SIGNATURE_BYTES]>::try_from(signature).map_err(|_| PacketError::InvalidSignatureLen)
}

pub fn get_signature_from_packet_mut<'a>(
    packet: &'a PacketRefMut<'a>,
) -> Result<&'a [u8; SIGNATURE_BYTES], PacketError> {
    let packet_data = packet.data(..).ok_or(PacketError::InvalidLen)?;
    let (num_signatures, signature_offset) =
        decode_shortu16_len(packet_data).map_err(|_| PacketError::InvalidShortVec)?;
    if num_signatures == 0 {
        return Err(PacketError::InvalidSignatureLen);
    }

    let signature_end = signature_offset
        .checked_add(SIGNATURE_BYTES)
        .ok_or(PacketError::InvalidLen)?;
    let signature = packet
        .data(signature_offset..signature_end)
        .ok_or(PacketError::InvalidSignatureLen)?;
    <&[u8; SIGNATURE_BYTES]>::try_from(signature).map_err(|_| PacketError::InvalidSignatureLen)
}
