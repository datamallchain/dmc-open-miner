use std::str::FromStr;
use serde::{Serialize, Deserialize};
use dmc_tools_common::*;
use crate::{
    buffer::*
};


pub trait EosSerialize {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()>;
}

pub trait EosDeserialize: Sized {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self>;
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AbiDef {
    pub version: String,
    pub types: Vec<TypeDef>,
    pub structs: Vec<StructDef>,
    pub actions: Vec<ActionDef>,
    pub tables: Vec<TableDef>,
    pub ricardian_clauses: Vec<ClausePair>,
    pub error_messages: Vec<ErrorMessage>,
    pub abi_extensions: Vec<ExtensionsEntry>,
    pub variants: Option<Vec<VariantDef>>,
    pub action_results: Option<Vec<ActionResult>>,
    pub kv_tables: Option<KvTable>
}

impl AbiDef {
    pub fn parse(raw_abi: &[u8]) -> DmcResult<Self> {
        let mut buf = EosSerialRead::new(raw_abi);
        let version = buf.get_string()?;
        if !version.starts_with("eosio::abi/1.") {
            return Err(dmc_err!(DmcErrorCode::NotSupport, "version {}", version))
        }

        buf.restart_read();

        AbiDef::eos_deserialize(&mut buf)
    }

    pub fn to_raw_abi(&self) -> DmcResult<Vec<u8>> {
        let mut ser_buf = EosSerialWrite::new();
        self.eos_serialize(&mut ser_buf)?;
        Ok(ser_buf.into())
    }
}

impl EosSerialize for AbiDef {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        buf.push_string(self.version.as_str());
        let len = self.types.len();
        buf.push_var_u32(len as u32);
        for ty in self.types.iter() {
            ty.eos_serialize(buf)?;
        }
        buf.push_var_u32(self.structs.len() as u32);
        for st in self.structs.iter() {
            st.eos_serialize(buf)?;
        }
        buf.push_var_u32(self.actions.len() as u32);
        for action in self.actions.iter() {
            action.eos_serialize(buf)?;
        }
        buf.push_var_u32(self.tables.len() as u32);
        for table in self.tables.iter() {
            table.eos_serialize(buf)?;
        }
        buf.push_var_u32(self.ricardian_clauses.len() as u32);
        for clause in self.ricardian_clauses.iter() {
            clause.eos_serialize(buf)?;
        }
        buf.push_var_u32(self.error_messages.len() as u32);
        for message in self.error_messages.iter() {
            message.eos_serialize(buf)?;
        }
        buf.push_var_u32(self.abi_extensions.len() as u32);
        for ext in self.abi_extensions.iter() {
            ext.eos_serialize(buf)?;
        }
        if self.variants.is_some() {
            buf.push_var_u32(self.variants.as_ref().unwrap().len() as u32);
            for var in self.variants.as_ref().unwrap().iter() {
                var.eos_serialize(buf)?;
            }

            if self.action_results.is_some() {
                buf.push_var_u32(self.action_results.as_ref().unwrap().len() as u32);
                for result in self.action_results.as_ref().unwrap().iter() {
                    result.eos_serialize(buf)?;
                }
                if self.kv_tables.is_some() {
                    self.kv_tables.as_ref().unwrap().eos_serialize(buf)?;
                }
            }
        }
        Ok(())
    }
}

impl EosDeserialize for AbiDef {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        let version = buf.get_string()?;
        let len = buf.get_var_u32()?;
        let mut types = Vec::new();
        for _ in 0..len {
            types.push(TypeDef::eos_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut structs = Vec::new();
        for _ in 0..len {
            structs.push(StructDef::eos_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut actions = Vec::new();
        for _ in 0..len {
            actions.push(ActionDef::eos_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut tables = Vec::new();
        for _ in 0..len {
            tables.push(TableDef::eos_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut ricardian_clauses = Vec::new();
        for _ in 0..len {
            ricardian_clauses.push(ClausePair::eos_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut error_messages = Vec::new();
        for _ in 0..len {
            error_messages.push(ErrorMessage::eos_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut abi_extensions = Vec::new();
        for _ in 0..len {
            abi_extensions.push(ExtensionsEntry::eos_deserialize(buf)?);
        }
        let (variants, action_results, kv_tables) = if buf.have_read_data() {
            let len = buf.get_var_u32()?;
            let mut variants = Vec::new();
            for _ in 0..len {
                variants.push(VariantDef::eos_deserialize(buf)?);
            }
            let (action_results, kv_tables) = if buf.have_read_data() {
                let len = buf.get_var_u32()?;
                let mut action_results = Vec::new();
                for _ in 0..len {
                    action_results.push(ActionResult::eos_deserialize(buf)?);
                }
                let kv_tables = if buf.have_read_data() {
                    let kv_tables = KvTable::eos_deserialize(buf)?;
                    Some(kv_tables)
                } else {
                    None
                };
                (Some(action_results), kv_tables)
            } else {
                (None, None)
            };
            (Some(variants), action_results, kv_tables)
        } else {
            (None, None, None)
        };
        Ok(Self {
            version,
            types,
            structs,
            actions,
            tables,
            ricardian_clauses,
            error_messages,
            abi_extensions,
            variants,
            action_results,
            kv_tables
        })
    }
}


impl EosSerialize for TimePointSec {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        let secs = self.as_secs()?;
        buf.push_u32(secs as u32);
        Ok(())
    }
}

impl EosDeserialize for TimePointSec {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        let secs = buf.get_u32()? as i64;
        Ok(Self::from_secs(secs))
    }
}



#[derive(Serialize, Deserialize, Clone)]
pub struct TypeDef {
    pub new_type_name: String,
    #[serde(rename="type")]
    pub ty: String,
}

impl EosSerialize for TypeDef {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        buf.push_string(self.new_type_name.as_str());
        buf.push_string(self.ty.as_str());
        Ok(())
    }
}

impl EosDeserialize for TypeDef {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        Ok(Self {
            new_type_name: buf.get_string()?,
            ty: buf.get_string()?,
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StructDef {
    pub name: String,
    pub base: String,
    pub fields: Vec<FieldDef>,
}

impl EosSerialize for StructDef {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        buf.push_string(self.name.as_str());
        buf.push_string(self.base.as_str());
        buf.push_var_u32(self.fields.len() as u32);
        for field in self.fields.iter() {
            field.eos_serialize(buf)?;
        }
        Ok(())
    }
}

impl EosDeserialize for StructDef {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        let name = buf.get_string()?;
        let base = buf.get_string()?;
        let len = buf.get_var_u32()?;
        let mut fields = Vec::new();
        for _ in 0..len {
            fields.push(FieldDef::eos_deserialize(buf)?);
        }
        Ok(Self {
            name,
            base,
            fields
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ActionDef {
    pub name: Name,
    #[serde(rename="type")]
    pub ty: String,
    pub ricardian_contract: String,
}

impl EosSerialize for ActionDef {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        self.name.eos_serialize(buf)?;
        buf.push_string(self.ty.as_str());
        buf.push_string(self.ricardian_contract.as_str());
        Ok(())
    }
}

impl EosDeserialize for ActionDef {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        Ok(Self {
            name: Name::eos_deserialize(buf)?,
            ty: buf.get_string()?,
            ricardian_contract: buf.get_string()?
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TableDef {
    pub name: Name,
    pub index_type: String,
    pub key_names: Vec<String>,
    pub key_types: Vec<String>,
    #[serde(rename="type")]
    pub ty: String,
}

impl EosSerialize for TableDef {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        self.name.eos_serialize(buf)?;
        buf.push_string(self.index_type.as_str());
        buf.push_var_u32(self.key_names.len() as u32);
        for key_name in self.key_names.iter() {
            buf.push_string(key_name);
        }
        buf.push_var_u32(self.key_types.len() as u32);
        for key_type in self.key_types.iter() {
            buf.push_string(key_type);
        }
        buf.push_string(self.ty.as_str());
        Ok(())
    }
}

impl EosDeserialize for TableDef {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        let name = Name::eos_deserialize(buf)?;
        let index_type = buf.get_string()?;
        let len = buf.get_var_u32()?;
        let mut key_names = Vec::new();
        for _ in 0..len {
            key_names.push(buf.get_string()?);
        }
        let len = buf.get_var_u32()?;
        let mut key_types = Vec::new();
        for _ in 0..len {
            key_types.push(buf.get_string()?);
        }
        let ty = buf.get_string()?;
        Ok(Self {
            name,
            index_type,
            key_names,
            key_types,
            ty
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ClausePair {
    pub id: String,
    pub body: String,
}

impl EosSerialize for ClausePair {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        buf.push_string(self.id.as_str());
        buf.push_string(self.body.as_str());
        Ok(())
    }
}

impl EosDeserialize for ClausePair {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        Ok(Self {
            id: buf.get_string()?,
            body: buf.get_string()?
        })
    }
}
#[derive(Serialize, Deserialize, Clone)]
pub struct ErrorMessage {
    pub error_code: u64,
    pub error_msg: String,
}

impl EosSerialize for ErrorMessage {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        buf.push_u64(self.error_code);
        buf.push_string(self.error_msg.as_str());
        Ok(())
    }
}

impl EosDeserialize for ErrorMessage {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        Ok(Self {
            error_code: buf.get_u64()?,
            error_msg: buf.get_string()?,
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ExtensionsEntry {
    pub tag: u16,
    pub value: Vec<u8>,
}

impl EosSerialize for ExtensionsEntry {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        buf.push_u16(self.tag);
        buf.push_bytes(self.value.as_slice());
        Ok(())
    }
}

impl EosDeserialize for ExtensionsEntry {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        Ok(Self {
            tag: buf.get_u16()?,
            value: buf.get_bytes()?.to_vec()
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct VariantDef {
    pub name: String,
    pub types: Vec<String>,
}

impl EosSerialize for VariantDef {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        buf.push_string(self.name.as_str());
        buf.push_var_u32(self.types.len() as u32);
        for ty in self.types.iter() {
            buf.push_string(ty);
        }
        Ok(())
    }
}

impl EosDeserialize for VariantDef {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        let name = buf.get_string()?;
        let len = buf.get_var_u32()?;
        let mut types = Vec::new();
        for _ in 0..len {
            types.push(buf.get_string()?);
        }
        Ok(Self {
            name,
            types
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ActionResult {
    pub name: Name,
    #[serde(rename="type")]
    pub ty: String,
}

impl EosSerialize for ActionResult {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        self.name.eos_serialize(buf)?;
        buf.push_string(self.ty.as_str());
        Ok(())
    }
}

impl EosDeserialize for ActionResult {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        Ok(Self {
            name: Name::eos_deserialize(buf)?,
            ty: buf.get_string()?
        })
    }
}
#[derive(Serialize, Deserialize, Clone)]
pub struct KvTable {
    pub name: Option<Name>,
    pub kv_table_entry_def: Option<KvTableEntryDef>,
}

impl EosSerialize for KvTable {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        let mut len = 0;
        if self.name.is_some() {
            len += 1;
        }
        if self.kv_table_entry_def.is_some() {
            len += 1;
        }
        buf.push_var_u32(len);
        if self.name.is_some() {
            buf.push_string("name");
            Name::eos_serialize(self.name.as_ref().unwrap(), buf)?;
        }
        if self.kv_table_entry_def.is_some() {
            buf.push_string("kv_table_entry_def");
            self.kv_table_entry_def.as_ref().unwrap().eos_serialize(buf)?;
        }
        Ok(())
    }
}

impl EosDeserialize for KvTable {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        let len = buf.get_var_u32()?;
        if len == 0 {
            Ok(Self {
                name: None,
                kv_table_entry_def: None,
            })
        } else {
            let mut name = None;
            let mut kv_table_entry_def = None;
            for _ in 0..len {
                let key = buf.get_string()?;
                if key.as_str() == "name" {
                    name = Some(Name::eos_deserialize(buf)?);
                } else if key.as_str() == "kv_table_entry_def" {
                    kv_table_entry_def = Some(KvTableEntryDef::eos_deserialize(buf)?)
                }
            }
            Ok(Self {
                name,
                kv_table_entry_def
            })
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct FieldDef {
    pub name: String,
    #[serde(rename="type")]
    pub ty: String,
}

impl EosSerialize for FieldDef {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        buf.push_string(self.name.as_str());
        buf.push_string(self.ty.as_str());
        Ok(())
    }
}

impl EosDeserialize for FieldDef {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        let name = buf.get_string()?;
        let ty = buf.get_string()?;
        Ok(Self { name, ty })
    }
}
#[derive(Serialize, Deserialize, Clone)]
pub struct SecondaryIndexDef {
    #[serde(rename="type")]
    ty: String
}

impl EosSerialize for SecondaryIndexDef {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        buf.push_string(self.ty.as_str());
        Ok(())
    }
}

impl EosDeserialize for SecondaryIndexDef {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        buf.get_string().map(|ty| Self {ty})
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SecondaryIndices {
    pub name: Option<Name>,
    pub secondary_index_def: Option<SecondaryIndexDef>
}

impl EosSerialize for SecondaryIndices {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        let mut len = 0;
        if self.name.is_some() {
            len += 1;
        }
        if self.secondary_index_def.is_some() {
            len += 1;
        }
        buf.push_var_u32(len);
        if self.name.is_some() {
            buf.push_string("name");
            Name::eos_serialize(self.name.as_ref().unwrap(), buf)?;
        }
        if self.secondary_index_def.is_some() {
            buf.push_string("secondary_index_def");
            self.secondary_index_def.as_ref().unwrap().eos_serialize(buf)?;
        }
        Ok(())
    }
}

impl EosDeserialize for SecondaryIndices {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        let len = buf.get_var_u32()?;
        if len == 0 {
            Ok(Self {
                name: None,
                secondary_index_def: None,
            })
        } else {
            let mut name = None;
            let mut secondary_index_def = None;
            for _ in 0..len {
                let key = buf.get_string()?;
                if key.as_str() == "name" {
                    name = Some(Name::eos_deserialize(buf)?);
                } else if key.as_str() == "secondary_index_def" {
                    secondary_index_def = Some(SecondaryIndexDef::eos_deserialize(buf)?)
                }
            }
            Ok(Self {
                name,
                secondary_index_def
            })
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct KvTableEntryDef {
    pub ty: String,
    pub primary_index: PrimaryKeyIndexDef,
    pub secondary_indices: SecondaryIndices,
}

impl EosSerialize for KvTableEntryDef {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        buf.push_string(self.ty.as_str());
        self.primary_index.eos_serialize(buf)?;
        self.secondary_indices.eos_serialize(buf)?;
        Ok(())
    }
}

impl EosDeserialize for KvTableEntryDef {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        Ok(Self {
            ty: buf.get_string()?,
            primary_index: PrimaryKeyIndexDef::eos_deserialize(buf)?,
            secondary_indices: SecondaryIndices::eos_deserialize(buf)?,
        })
    }
}
#[derive(Serialize, Deserialize, Clone)]
pub struct PrimaryKeyIndexDef {

}

impl EosSerialize for PrimaryKeyIndexDef {
    fn eos_serialize(&self, _buf: &mut EosSerialWrite) -> DmcResult<()> {
        Ok(())
    }
}

impl EosDeserialize for PrimaryKeyIndexDef {
    fn eos_deserialize(_buf: &mut EosSerialRead) -> DmcResult<Self> {
        Ok(Self{})
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ResourcePayer {
    pub payer: Name,
    pub max_net_bytes: u64,
    pub max_cpu_us: u64,
    pub max_memory_bytes: u64,
}

impl EosSerialize for ResourcePayer {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        Name::eos_serialize(&self.payer, buf)?;
        buf.push_u64(self.max_net_bytes);
        buf.push_u64(self.max_cpu_us);
        buf.push_u64(self.max_memory_bytes);
        Ok(())
    }
}

impl EosDeserialize for ResourcePayer {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        Ok(Self {
            payer: Name::eos_deserialize(buf)?,
            max_net_bytes: buf.get_u64()?,
            max_cpu_us: buf.get_u64()?,
            max_memory_bytes: buf.get_u64()?
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PermissionLevel {
    pub actor: Name,
    pub permission: Name,
}
pub type Authorization = PermissionLevel;

impl EosSerialize for PermissionLevel {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        Name::eos_serialize(&self.actor, buf)?;
        Name::eos_serialize(&self.permission, buf)?;
        Ok(())
    }
}

impl EosDeserialize for PermissionLevel {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        Ok(Self {
            actor: Name::eos_deserialize(buf)?,
            permission: Name::eos_deserialize(buf)?
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Action {
    pub account: Name,
    pub name: Name,
    pub authorization: Vec<PermissionLevel>,
    pub data: Vec<u8>,
    #[serde(skip)]
    pub hex_data: Option<String>
}

impl EosSerialize for Action {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        Name::eos_serialize(&self.account, buf)?;
        Name::eos_serialize(&self.name, buf)?;
        buf.push_var_u32(self.authorization.len() as u32);
        for per in self.authorization.iter() {
            per.eos_serialize(buf)?;
        }
        buf.push_bytes(self.data.as_slice());
        Ok(())
    }
}

impl EosDeserialize for Action {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        let account = Name::eos_deserialize(buf)?;
        let name = Name::eos_deserialize(buf)?;
        let len = buf.get_var_u32()?;
        let mut authorization = Vec::new();
        for _ in 0..len {
            authorization.push(PermissionLevel::eos_deserialize(buf)?);
        }
        let data = buf.get_bytes()?.to_vec();
        Ok(Self {
            account,
            name,
            authorization,
            data,
            hex_data: None,
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TransExtension {
    #[serde(rename="type")]
    pub ty: u16,
    pub data: Vec<u8>,
}

impl EosSerialize for TransExtension  {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        buf.push_u16(self.ty);
        buf.push_bytes(self.data.as_slice());
        Ok(())
    }
}

impl EosDeserialize for TransExtension {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        Ok(Self {
            ty: buf.get_u16()?,
            data: buf.get_bytes()?.to_vec()
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TransHeader {
    pub expiration: TimePointSec,
    pub ref_block_num: u16,
    pub ref_block_prefix: u32,
    pub max_net_usage_words: u32,
    pub max_cpu_usage_ms: u8,
    pub delay_sec: u32,
}

impl EosSerialize for TransHeader {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        self.expiration.eos_serialize(buf)?;
        buf.push_u16(self.ref_block_num);
        buf.push_u32(self.ref_block_prefix);
        buf.push_var_u32(self.max_net_usage_words);
        buf.push_u8(self.max_cpu_usage_ms);
        buf.push_var_u32(self.delay_sec);
        Ok(())
    }
}

impl EosDeserialize for TransHeader {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        Ok(Self {
            expiration: TimePointSec::eos_deserialize(buf)?,
            ref_block_num: buf.get_u16()?,
            ref_block_prefix: buf.get_u32()?,
            max_net_usage_words: buf.get_var_u32()?,
            max_cpu_usage_ms: buf.get_u8()?,
            delay_sec: buf.get_var_u32()?
        })
    }
}


#[derive(Serialize, Deserialize, Clone)]
pub struct Transaction {
    #[serde(flatten)]
    pub header: TransHeader, 
    pub context_free_actions: Vec<Action>,
    pub actions: Vec<Action>,
    pub transaction_extensions: Vec<TransExtension>,
}

impl EosSerialize for Transaction {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        self.header.eos_serialize(buf)?;
        buf.push_var_u32(self.context_free_actions.len() as u32);
        for action in self.context_free_actions.iter() {
            action.eos_serialize(buf)?;
        }
        buf.push_var_u32(self.actions.len() as u32);
        for action in self.actions.iter() {
            action.eos_serialize(buf)?;
        }
        buf.push_var_u32(self.transaction_extensions.len() as u32);
        for extenstion in self.transaction_extensions.iter() {
            extenstion.eos_serialize(buf)?;
        }

        Ok(())
    }
}

impl EosDeserialize for Transaction {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        let header = TransHeader::eos_deserialize(buf)?;
        let len = buf.get_var_u32()?;
        let mut context_free_actions = Vec::new();
        for _ in 0..len {
            context_free_actions.push(Action::eos_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut actions = Vec::new();
        for _ in 0..len {
            actions.push(Action::eos_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut transaction_extensions = Vec::new();
        for _ in 0..len {
            transaction_extensions.push(TransExtension::eos_deserialize(buf)?);
        }

        Ok(Self {
            header, 
            context_free_actions,
            actions,
            transaction_extensions,
        })
    }
}


pub type Name = String;

impl EosSerialize for Name {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        buf.push_name(self.as_str())?;
        Ok(())
    }
}

impl EosDeserialize for Name {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        buf.get_name()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(into = "String", try_from = "String")]
pub struct Asset {
    pub amount: f64,
    pub symbol: Name, 
    pub precision: u8
}

impl Asset {
    pub fn int(amount: u64, symbol: &str) -> Self {
        Self {
            amount: amount as f64,
            symbol: symbol.to_owned(), 
            precision: 0
        }
    }

    pub fn with_precision(amount: f64, symbol: &str, precision: u8) -> Self {
        assert!(precision <= 8);
        Self {
            amount,
            symbol: symbol.to_owned(), 
            precision
        }
    }
}

impl Into<String> for Asset {
    fn into(self) -> String {
        self.to_string()
    }
}

impl ToString for Asset {
    fn to_string(&self) -> String {
        if self.precision > 0 {
            let amount_str = format!("{:.08}", self.amount);
            let reserved = &amount_str[..amount_str.len() - (8 - self.precision) as usize];
            format!("{} {}", reserved, self.symbol)
        } else {
            format!("{} {}", self.amount as u64, self.symbol)
        }
    }
}

impl TryFrom<String> for Asset {
    type Error = DmcError;

    fn try_from(s: String) -> DmcResult<Self> {
        Self::from_str(&s)
    }
}

impl FromStr for Asset {
    type Err = DmcError;

    fn from_str(s: &str) -> DmcResult<Self> {
        let parts: Vec<&str> = s.split(" ").collect();
        if parts.len() != 2 {
            return Err(dmc_err!(DmcErrorCode::InvalidInput, "invalid extend asset {}", s));
        }

        let float_parts: Vec<&str> = parts[0].split(".").collect();
        let precision = if float_parts.len() == 1 {
            0
        } else {
            float_parts[0].len()
        };

        let amount = f64::from_str(&parts[0])?;
        let symbol = parts[1];

        Ok(Self {
            amount,
            symbol: symbol.to_owned(), 
            precision: precision as u8
        })
    }
}

impl EosSerialize for Asset {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        buf.push_asset(&self.to_string())?;
        Ok(())
    }
}

impl EosDeserialize for Asset {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        Self::from_str(&buf.get_asset()?)
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExtendedAsset {
    pub quantity: Asset,
    pub contract: Name,
}

impl ExtendedAsset {
    pub fn new(contract: &str, quantity: Asset) -> Self {
        Self {
            quantity, 
            contract: contract.to_owned()
        }
    }
}


impl EosSerialize for ExtendedAsset {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        self.quantity.eos_serialize(buf)?;
        buf.push_name(self.contract.as_str())?;
        Ok(())
    }
}

impl EosDeserialize for ExtendedAsset {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        Ok(Self {
            quantity: Asset::eos_deserialize(buf)?,
            contract: buf.get_name()?
        })
    }
}

pub struct EosIncreaseOptions {
    pub owner: Name, 
    pub asset: ExtendedAsset, 
    pub miner: Name,
}

impl EosSerialize for EosIncreaseOptions {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        self.owner.eos_serialize(buf)?;
        self.asset.eos_serialize(buf)?;
        self.miner.eos_serialize(buf)?;
        Ok(())
    }
}

pub struct EosMintOptions {
    pub owner: Name, 
    pub asset: ExtendedAsset
}

impl EosSerialize for EosMintOptions {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        self.owner.eos_serialize(buf)?;
        self.asset.eos_serialize(buf)?;
        Ok(())
    }
}

pub struct EosBillOptions {
    pub owner: Name,
    pub asset: ExtendedAsset,
    pub price: f64, 
    pub expire_on: TimePointSec, 
    pub deposit_ratio: u64, 
    pub memo: String,
}


impl EosSerialize for EosBillOptions {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        self.owner.eos_serialize(buf)?;
        self.asset.eos_serialize(buf)?;
        buf.push_f64(self.price);
        self.expire_on.eos_serialize(buf)?;
        buf.push_u64(self.deposit_ratio);
        buf.push_string(self.memo.as_str());
        Ok(())
    }
}

pub struct EosUnbillOptions {
    pub owner: Name,
    pub bill_id: u64, 
    pub memo: String,
}


impl EosSerialize for EosUnbillOptions {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        self.owner.eos_serialize(buf)?;
        buf.push_u64(self.bill_id);
        buf.push_string(self.memo.as_str());
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub enum EosOrderPriceRange {
    TwentyPercent = 1,
    ThirtyPercent = 2,
    NoLimit = 3,
}


 // ACTION addmerkle(name sender, uint64_t order_id, checksum256 merkle_root, uint64_t data_block_count)
 pub struct EosPrepareOrderOptions {
    pub owner: Name, 
    pub order_id: u64, 
    pub merkle_root: HashValue, 
    pub data_block_count: u64
 }

 impl EosSerialize for EosPrepareOrderOptions {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        self.owner.eos_serialize(buf)?;
        buf.push_u64(self.order_id);
        buf.push_array(self.merkle_root.as_slice());
        buf.push_u64(self.data_block_count);
        Ok(())
    }
}

// ACTION order(name owner, uint64_t bill_id, uint64_t benchmark_price, PriceRangeType price_range, uint64_t epoch, extended_asset asset, extended_asset reserve, string memo);
pub struct EosOrderOptions {
    pub owner: Name, 
    pub bill_id: u64,
    pub benchmark_price: u64, 
    pub price_range: EosOrderPriceRange, 
    pub epoch: u64, 
    pub asset: ExtendedAsset, 
    pub reserve: ExtendedAsset, 
    pub memo: String
}

impl EosSerialize for EosOrderOptions {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        self.owner.eos_serialize(buf)?;
        buf.push_u64(self.bill_id);
        buf.push_u64(self.benchmark_price);
        buf.push_u8(self.price_range as u8);
        buf.push_u64(self.epoch);
        self.asset.eos_serialize(buf)?;
        self.reserve.eos_serialize(buf)?;
        buf.push_string(self.memo.as_str());
        Ok(())
    }
}


// ACTION reqchallenge(name sender, uint64_t order_id, uint64_t data_id, checksum256 hash_data, std::string nonce);
 pub struct EosChallengeOptions {
    pub owner: Name, 
    pub order_id: u64, 
    pub data_id: u64, 
    pub hash_data: HashValue, 
    pub nonce: String
 }

impl EosSerialize for EosChallengeOptions {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        self.owner.eos_serialize(buf)?;
        buf.push_u64(self.order_id);
        buf.push_u64(self.data_id);
        buf.push_array(self.hash_data.as_slice());
        buf.push_string(self.nonce.as_str());
        Ok(())
    }
}

// ACTION anschallenge(name sender, uint64_t order_id, checksum256 reply_hash);
pub struct EosAnsChallengeOptions {
    pub owner: Name, 
    pub order_id: u64, 
    pub reply_hash: HashValue, 
}

impl EosSerialize for EosAnsChallengeOptions {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        self.owner.eos_serialize(buf)?;
        buf.push_u64(self.order_id);
        buf.push_array(self.reply_hash.as_slice());
        Ok(())
    }
}


// ACTION arbitration(name sender, uint64_t order_id, const std::vector<char>& data, std::vector<checksum256> cut_merkle);
pub struct EosArbitrationOptions {
    pub owner: Name, 
    pub order_id: u64, 
    pub data: DmcData, 
    pub cur_merkle: Vec<HashValue>
}

impl EosSerialize for EosArbitrationOptions {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        self.owner.eos_serialize(buf)?;
        buf.push_u64(self.order_id);
        buf.push_bytes (self.data.as_slice());
        buf.push_var_u32(self.cur_merkle.len() as u32);
        for path in &self.cur_merkle {
            buf.push_array(path.as_slice());
        }
        Ok(())
    }
}

#[derive(Deserialize, Clone)]
pub struct EosAssetRow {
    pub primary: u32,
    pub balance: ExtendedAsset
}

#[derive(Deserialize, Clone)]
pub struct EosBillTableRow {
    pub bill_id: u64, 
    pub owner: Name, 
    pub matched: ExtendedAsset, 
    pub unmatched: ExtendedAsset,
    pub price: u64, 
    pub created_at: TimePointSec, 
    pub updated_at: TimePointSec,
    pub expire_on: TimePointSec, 
    pub deposit_ratio: u64
}

#[derive(Deserialize, Clone)]
#[serde(try_from = "u8")]
pub enum EosOrderState {
    Waiting = 0,
    Deliver = 1,
    PreEnd = 2,
    PreCont = 3,
    End = 4,
    Cancel = 5,
    PreCancel = 6,
}

impl TryFrom<u8> for EosOrderState {
    type Error = DmcError;

    fn try_from(v: u8) -> DmcResult<Self> {
        match v {
            0 => Ok(Self::Waiting), 
            1 => Ok(Self::Deliver), 
            2 => Ok(Self::PreEnd), 
            3 => Ok(Self::PreCont), 
            4 => Ok(Self::End), 
            5 => Ok(Self::Cancel), 
            6 => Ok(Self::PreCancel), 
            _ => Err(DmcError::new(DmcErrorCode::InvalidData, "invalid order state"))
        }
    }
}

#[derive(Deserialize, Clone)]
pub struct EosOrderTableRow {
    pub order_id: u64, 
    pub user: Name, 
    pub miner: Name, 
    pub bill_id: u64, 
    pub user_pledge: ExtendedAsset, 
    pub miner_lock_pst: ExtendedAsset, 
    pub miner_lock_dmc: ExtendedAsset, 
    pub price: ExtendedAsset, 
    pub settlement_pledge: ExtendedAsset, 
    pub lock_pledge: ExtendedAsset, 
    pub state: EosOrderState, 
    pub deliver_start_date: TimePointSec,
    pub latest_settlement_date: TimePointSec,
    pub miner_lock_rsi: ExtendedAsset, 
    pub miner_rsi: ExtendedAsset, 
    pub user_rsi: ExtendedAsset, 
    pub deposit: ExtendedAsset, 
    pub epoch: u64, 
    pub deposit_valid: TimePointSec,
    pub cancel_date: TimePointSec,
}

#[derive(Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[serde(try_from = "u8")]
pub enum EosChallengeState {
    Prepare = 0,
    Consistent = 1,
    Cancel = 2,
    Request = 3,
    Answer = 4,
    ArbitrationMinerPay = 5,
    ArbitrationUserPay = 6,
    Timeout = 7,
}

impl EosChallengeState {
    pub(crate) fn is_end(&self) -> bool {
        match self {
            EosChallengeState::Consistent | EosChallengeState::Answer | EosChallengeState::ArbitrationMinerPay | EosChallengeState::ArbitrationUserPay => true,
            _ => false,
        }
    }

    // 表示是否是一个有效的challenge状态。DMC的challenge是order状态的一部分
    pub(crate) fn is_valid(&self) -> bool {
        match self {
            EosChallengeState::Prepare | EosChallengeState::Consistent => false,
            _ => true
        }
    }
}

pub(crate) fn make_dmc_challenge_id(order_id: u32, challenge_times: u32) -> u64 {
    let high = order_id.to_be_bytes();
    let low = challenge_times.to_be_bytes();
    u64::from_be_bytes([high[0], high[1], high[2], high[3], low[0], low[1], low[2], low[3]])
}

impl TryFrom<u8> for EosChallengeState {
    type Error = DmcError;

    fn try_from(v: u8) -> DmcResult<Self> {
        match v {
            0 => Ok(Self::Prepare), 
            1 => Ok(Self::Consistent), 
            2 => Ok(Self::Cancel), 
            3 => Ok(Self::Request), 
            4 => Ok(Self::Answer), 
            5 => Ok(Self::ArbitrationMinerPay), 
            6 => Ok(Self::ArbitrationUserPay), 
            7 => Ok(Self::Timeout), 
            _ => Err(DmcError::new(DmcErrorCode::InvalidData, "invalid order state"))
        }
    }
}

#[derive(Deserialize, Clone)]
pub struct EosChallengeTableRow {
    pub order_id: u64, 
    pub pre_merkle_root: HashValue,
    pub pre_data_block_count: u64, 
    pub merkle_root: HashValue, 
    pub data_block_count: u64, 
    pub merkle_submitter: Name, 
    pub data_id: u64, 
    pub hash_data: HashValue, 
    pub challenge_times: u64, 
    pub nonce: String, 
    pub state: EosChallengeState, 
    pub user_lock: ExtendedAsset, 
    pub miner_pay: ExtendedAsset, 
    pub challenge_date: TimePointSec, 
    pub challenger: Name
}

#[derive(Deserialize, Clone)]
pub struct EosBenchmarkPriceRow {
    pub prices: Vec<String>,
    pub benchmark_price: String
}

#[derive(Deserialize, Clone)]
pub struct EosChainConfigRow {
    pub key: Name,
    pub value: u64
}


#[derive(Deserialize)]
pub struct KeyWeight {
    pub key: String,
    pub weight: u16,
}

impl EosSerialize for KeyWeight {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        buf.push_public_key(self.key.as_str())?;
        buf.push_u16(self.weight);
        Ok(())
    }
}

impl EosDeserialize for KeyWeight {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        Ok(Self {key: buf.get_public_key()?, weight: buf.get_u16()?})
    }
}


#[derive(Deserialize)]
pub struct PermissionLevelWeight {
    pub permission: PermissionLevel,
    pub weight: u16,
}

impl EosSerialize for PermissionLevelWeight {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        self.permission.eos_serialize(buf)?;
        buf.push_u16(self.weight);
        Ok(())
    }
}

impl EosDeserialize for PermissionLevelWeight {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        Ok(Self {
            permission: PermissionLevel::eos_deserialize(buf)?,
            weight: buf.get_u16()?
        })
    }
}


#[derive(Deserialize)]
pub struct WaitWeight {
    pub wait_sec: u32,
    pub weight: u16,
}

impl EosSerialize for WaitWeight {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        buf.push_u32(self.wait_sec);
        buf.push_u16(self.weight);
        Ok(())
    }
}

impl EosDeserialize for WaitWeight {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        Ok(Self {
            wait_sec: buf.get_u32()?,
            weight: buf.get_u16()?
        })
    }
}


#[derive(Deserialize)]
pub struct Authority {
    pub threshold: u32,
    pub keys: Vec<KeyWeight>,
    pub accounts: Vec<PermissionLevelWeight>,
    pub waits: Vec<WaitWeight>,
}

impl EosSerialize for Authority {
    fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
        buf.push_u32(self.threshold);
        buf.push_var_u32(self.keys.len() as u32);
        for key in self.keys.iter() {
            key.eos_serialize(buf)?;
        }
        buf.push_var_u32(self.accounts.len() as u32);
        for account in self.accounts.iter() {
            account.eos_serialize(buf)?;
        }
        buf.push_var_u32(self.waits.len() as u32);
        for wait in self.waits.iter() {
            wait.eos_serialize(buf)?;
        }
        Ok(())
    }
}

impl EosDeserialize for Authority {
    fn eos_deserialize(buf: &mut EosSerialRead) -> DmcResult<Self> {
        let threshold = buf.get_u32()?;
        let len = buf.get_var_u32()?;
        let mut keys = Vec::new();
        for _ in 0..len {
            keys.push(KeyWeight::eos_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut accounts = Vec::new();
        for _ in 0..len {
            accounts.push(PermissionLevelWeight::eos_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut waits = Vec::new();
        for _ in 0..len {
            waits.push(WaitWeight::eos_deserialize(buf)?);
        }
        Ok(Self {
            threshold,
            keys,
            accounts,
            waits
        })
    }
}

