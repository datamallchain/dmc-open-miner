use std::{
    cmp::Ordering,
    str::FromStr
};
use base58::{FromBase58, ToBase58};
use libsecp256k1::{
    RecoveryId, 
    Signature, 
    PublicKey, 
    SecretKey, 
    curve::{ECMultGenContext, Scalar}
};
use rand::Rng;
use ripemd::Digest;
use dmc_tools_common::*;

#[repr(u8)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum EosKeyType {
    K1 = 0,
    R1 = 1,
    Wa = 2,
}

fn ripemd160(data: &[u8]) -> Vec<u8> {
    let mut rip = ripemd::Ripemd160::default();
    rip.update(data);
    rip.finalize().to_vec()[0..4].to_vec()
}

fn digest_suffix_ripemd160(data: &[u8], suffix: &str) -> Vec<u8> {
    let mut tmp = Vec::from(data);
    tmp.append(&mut suffix.as_bytes().to_vec());
    ripemd160(tmp.as_slice())
}

fn string_to_key(s: &str, _ty: EosKeyType, suffix: &str) -> DmcResult<Vec<u8>> {
    let data = s.from_base58().map_err(|e| {
        dmc_err!(DmcErrorCode::InvalidParam, "{} parse err {:?}", s, e)
    })?;

    let digest = digest_suffix_ripemd160(&data[0..data.len()-4], suffix);
    if digest.as_slice().cmp(&data[data.len()-4..]) != Ordering::Equal {
        return Err(dmc_err!(DmcErrorCode::CryptoError, "checksum doesn't match"));
    }
    Ok(data[0..data.len()-4].to_vec())
}

fn key_to_string(key_data: &[u8], suffix: &str, prefix: &str) -> String {
    let mut digest = digest_suffix_ripemd160(key_data, suffix);
    let mut tmp = Vec::from(key_data);
    tmp.append(&mut digest);
    prefix.to_string() + tmp.to_base58().as_str()
}

pub struct EosPrivateKey {
    pub key_type: EosKeyType,
    pub key: SecretKey,
}

impl EosPrivateKey {
    pub fn gen_key() -> Self {
        let private_key = SecretKey::random(&mut rand::thread_rng());
        Self {
            key_type: EosKeyType::K1,
            key: private_key
        }
    }

    pub fn gen_key_from_rng<R:Rng>(rng: &mut R) -> Self {
        let private_key = SecretKey::random(rng);
        Self {
            key_type: EosKeyType::K1,
            key: private_key
        }
    }

    pub fn to_legacy_string(&self) -> DmcResult<String> {
        match self.key_type {
            EosKeyType::K1 => {
                let mut buf: Vec<u8> = Vec::with_capacity(libsecp256k1::util::SECRET_KEY_SIZE + 5);
                buf.resize(libsecp256k1::util::SECRET_KEY_SIZE + 5, 0);
                buf[0] = 128u8;
                unsafe {
                    std::ptr::copy(self.key.serialize().as_ptr(), buf[1..].as_mut_ptr(), libsecp256k1::util::SECRET_KEY_SIZE);
                }
                let mut sha256 = sha2::Sha256::new();
                sha256.update(&buf[0..libsecp256k1::util::SECRET_KEY_SIZE+1]);
                let hash = sha256.finalize().to_vec();
                let mut sha256 = sha2::Sha256::new();
                sha256.update(hash.as_slice());
                let digest = sha256.finalize().to_vec();

                unsafe {
                    std::ptr::copy(digest[0..4].as_ptr(), buf[1+libsecp256k1::util::SECRET_KEY_SIZE..].as_mut_ptr(), 4);
                }
                Ok(buf.to_base58())
            }
            EosKeyType::R1 => {
                Err(dmc_err!(DmcErrorCode::NotSupport, "Key format not supported in legacy conversion"))
            }
            EosKeyType::Wa => {
                Err(dmc_err!(DmcErrorCode::NotSupport, "Key format not supported in legacy conversion"))
            }
        }
    }

    pub fn to_string(&self) -> DmcResult<String> {
        match self.key_type {
            EosKeyType::K1 => {
                Ok(key_to_string(self.key.serialize().as_slice(), "K1", "PVT_K1_"))
            }
            EosKeyType::R1 => {
                Ok(key_to_string(self.key.serialize().as_slice(), "R1", "PVT_R1_"))
            }
            EosKeyType::Wa => {
                Err(dmc_err!(DmcErrorCode::NotSupport, "unrecognized private key format"))
            }
        }
    }

    pub fn get_public_key(&self) -> EosPublicKey {
        EosPublicKey {
            key_type: self.key_type.clone(),
            key: PublicKey::from_secret_key(&self.key)
        }
    }

    fn is_canonical(sig_data: &[u8]) -> bool {
        (sig_data[1] & 0x80 == 0) && !(sig_data[1] == 0 && (sig_data[2] & 0x80 == 0)) && (sig_data[33] & 0x80 == 0) && !(sig_data[33] == 0 && (sig_data[34] & 0x80 == 0))
    }

    pub fn sign(&self, data: &[u8], should_hash: bool) -> DmcResult<EosSignature> {
        let msg = if should_hash {
            let mut sha256 = sha2::Sha256::new();
            sha256.update(data);
            libsecp256k1::Message::parse_slice(sha256.finalize().as_slice())
        } else {
            libsecp256k1::Message::parse_slice(data)
        }.map_err(|_| {
            dmc_err!(DmcErrorCode::InvalidParam, "parse message failed.")
        })?;

        if self.key_type == EosKeyType::K1 {
            let mut tries = 1;
            let sign_context = ECMultGenContext::new_boxed();
            let dmc_sign = loop {
                let mut nonce = Scalar::default();
                nonce.set_int(tries);

                let (sigr, sigs, recid) = sign_context.sign_raw(&self.key.clone().into(), &msg.0, &nonce).unwrap();
                let (sign, recovery_id) = (Signature { r: sigr, s: sigs }, RecoveryId::parse(recid).unwrap());
                let dmc_sign = EosSignature {
                    key_type: self.key_type,
                    signature: sign,
                    recovery_id
                };
                if Self::is_canonical(dmc_sign.to_binary().as_slice()) {
                    break dmc_sign;
                }
                tries += 1;
            };
            Ok(dmc_sign)
        } else {
            let (sign, recovery_id) = libsecp256k1::sign(&msg, &self.key);
            Ok(EosSignature {
                key_type: self.key_type,
                signature: sign,
                recovery_id
            })
        }
    }
}

impl FromStr for EosPrivateKey {
    type Err = DmcError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("PVT_R1_") {
            let key = string_to_key(&s["PVT_R1_".len()..], EosKeyType::R1, "R1")?;
            Ok(Self {
                key_type: EosKeyType::R1,
                key: SecretKey::parse_slice(key.as_slice()).map_err(|e| {
                    dmc_err!(DmcErrorCode::Failed, "secretkey parse_slice failed.{}", e)
                })?
            })
        } else if s.starts_with("PVT_K1_") {
            let key = string_to_key(&s["PVT_K1_".len()..], EosKeyType::R1, "K1")?;
            Ok(Self {
                key_type: EosKeyType::K1,
                key: SecretKey::parse_slice(key.as_slice()).map_err(|e| {
                    dmc_err!(DmcErrorCode::Failed, "secretkey parse_slice failed.{}", e)
                })?
            })
        } else {
            let whole = s.from_base58().map_err(|e| {
                dmc_err!(DmcErrorCode::InvalidParam, "{} parse err {:?}", s, e)
            })?;
            Ok(Self {
                key_type: EosKeyType::K1,
                key: SecretKey::parse_slice(&whole[1..libsecp256k1::util::SECRET_KEY_SIZE+1]).map_err(|e| {
                    dmc_err!(DmcErrorCode::Failed, "secretkey parse_slice failed.{}", e)
                })?
            })
        }
    }
}

pub struct EosPublicKey {
    pub key_type: EosKeyType,
    pub key: PublicKey,
}

impl EosPublicKey {
    pub fn to_legacy_string(&self) -> DmcResult<String> {
        match self.key_type {
            EosKeyType::K1 => {
                Ok(key_to_string(self.key.serialize_compressed().as_slice(), "", "DM"))
            }
            EosKeyType::R1 => {
                Err(dmc_err!(DmcErrorCode::NotSupport, "Key format not supported in legacy conversion"))
            }
            EosKeyType::Wa => {
                Err(dmc_err!(DmcErrorCode::NotSupport, "Key format not supported in legacy conversion"))
            }
        }
    }

    pub fn to_string(&self) -> String {
        match self.key_type {
            EosKeyType::K1 => {
                key_to_string(self.key.serialize_compressed().as_slice(), "K1", "PUB_K1_")
            }
            EosKeyType::R1 => {
                key_to_string(self.key.serialize_compressed().as_slice(), "R1", "PUB_R1_")
            }
            EosKeyType::Wa => {
                key_to_string(self.key.serialize_compressed().as_slice(), "WA", "PUB_WA_")
            }
        }
    }
}

impl FromStr for EosPublicKey {
    type Err = DmcError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("DM") {
            let whole = s["DM".len()..].from_base58().map_err(|e| {
                dmc_err!(DmcErrorCode::InvalidParam, "{} parse err {:?}", s, e)
            })?;

            let key_data = &whole[..libsecp256k1::util::COMPRESSED_PUBLIC_KEY_SIZE];
            let digest = ripemd160(key_data);
            if digest.as_slice().cmp(&whole[libsecp256k1::util::COMPRESSED_PUBLIC_KEY_SIZE..]) != Ordering::Equal {
                return Err(dmc_err!(DmcErrorCode::Failed, "checksum doesn't match"));
            }
            Ok(Self {
                key_type: EosKeyType::K1,
                key: PublicKey::parse_slice(key_data, None).map_err(|e| {
                    dmc_err!(DmcErrorCode::Failed, "public key parse_slice failed.{}", e)
                })?
            })
        } else if s.starts_with("PUB_K1_") {
            let key = string_to_key(&s["PUB_K1_".len()..], EosKeyType::K1, "K1")?;
            Ok(Self {
                key_type: EosKeyType::K1,
                key: PublicKey::parse_slice(key.as_slice(), None).map_err(|e| {
                    dmc_err!(DmcErrorCode::Failed, "public key parse_slice failed.{}", e)
                })?
            })
        } else if s.starts_with("PUB_R1_") {
            let key = string_to_key(&s["PUB_R1_".len()..], EosKeyType::K1, "R1")?;
            Ok(Self {
                key_type: EosKeyType::R1,
                key: PublicKey::parse_slice(key.as_slice(), None).map_err(|e| {
                    dmc_err!(DmcErrorCode::Failed, "public key parse_slice failed.{}", e)
                })?
            })
        } else if s.starts_with("PUB_WA_") {
            let key = string_to_key(&s["PUB_WA_".len()..], EosKeyType::K1, "WA")?;
            Ok(Self {
                key_type: EosKeyType::Wa,
                key: PublicKey::parse_slice(key.as_slice(), None).map_err(|e| {
                    dmc_err!(DmcErrorCode::Failed, "public key parse_slice failed.{}", e)
                })?
            })
        } else {
            Err(dmc_err!(DmcErrorCode::InvalidParam, "unrecognized public key format"))
        }
    }
}

pub struct EosSignature {
    key_type: EosKeyType,
    signature: Signature,
    recovery_id: RecoveryId,
}

impl EosSignature {
    pub fn to_string(&self) -> String {
        match self.key_type {
            EosKeyType::K1 => {
                key_to_string(self.to_binary().as_slice(), "K1", "SIG_K1_")
            }
            EosKeyType::R1 => {
                key_to_string(self.to_binary().as_slice(), "R1", "SIG_R1_")
            }
            EosKeyType::Wa => {
                key_to_string(self.to_binary().as_slice(), "WA", "SIG_WA_")
            }
        }
    }

    pub fn to_binary(&self) -> Vec<u8> {
        let _ = self.signature.serialize();
        let mut eosio_recovery_param = 0;
        if self.key_type == EosKeyType::K1 || self.key_type == EosKeyType::R1 {
            eosio_recovery_param = self.recovery_id.serialize() + 27;
            if self.recovery_id.serialize() <= 3 {
                eosio_recovery_param += 4;
            }
        } else if self.key_type == EosKeyType::Wa {
            eosio_recovery_param = self.recovery_id.serialize();
        }
        let mut buf = vec![eosio_recovery_param];
        buf.resize(65, 0);
        buf[1..].copy_from_slice(self.signature.serialize().as_slice());
        buf
    }

    pub fn get_type(&self) -> EosKeyType {
        self.key_type
    }

    pub fn verify(&self, data: &[u8], public_key: &EosPublicKey, should_hash: bool) -> DmcResult<bool> {
        let msg = if should_hash {
            let mut sha256 = sha2::Sha256::new();
            sha256.update(data);
            libsecp256k1::Message::parse_slice(sha256.finalize().as_slice())
        } else {
            libsecp256k1::Message::parse_slice(data)
        }.map_err(|_| {
            dmc_err!(DmcErrorCode::InvalidParam, "parse message failed.")
        })?;

        Ok(libsecp256k1::verify(&msg, &self.signature, &public_key.key))
    }
}

impl FromStr for EosSignature {
    type Err = DmcError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (_, sign) = if s.starts_with("SIG_K1_") {
            (EosKeyType::K1, string_to_key(&s[7..], EosKeyType::K1, "K1")?)
        } else if s.starts_with("SIG_R1_") {
            (EosKeyType::R1, string_to_key(&s[7..], EosKeyType::R1, "R1")?)
        } else if s.starts_with("SIG_WA_") {
            (EosKeyType::Wa, string_to_key(&s[7..], EosKeyType::Wa, "WA")?)
        } else {
            return Err(dmc_err!(DmcErrorCode::InvalidParam, "unrecognized signature format"));
        };

        let mut recovery_id = sign[0];
        recovery_id -= 31;
        let mut key_data = [0u8; 64];
        key_data.copy_from_slice(&sign[1..]);
        let sign = Signature::parse_standard(&key_data).map_err(|e| {
            dmc_err!(DmcErrorCode::InvalidParam, "parse signature failed {}", e)
        })?;
        Ok(Self {
            key_type: EosKeyType::K1,
            signature: sign,
            recovery_id: RecoveryId::parse(recovery_id).map_err(|e| {
                dmc_err!(DmcErrorCode::InvalidParam, "parse recovery id err {}", e)
            })?
        })
    }
}
