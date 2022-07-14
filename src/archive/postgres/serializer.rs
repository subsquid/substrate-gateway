use crate::archive::fields::{ExtrinsicFields, EventFields};
use crate::archive::selection::CallDataSelection;
use crate::entities::{Call, Event, Extrinsic};
use serde::ser::SerializeStruct;

pub struct ExtrinsicSerializer<'a> {
    pub extrinsic: &'a Extrinsic,
    pub fields: &'a ExtrinsicFields,
}

impl<'a> serde::Serialize for ExtrinsicSerializer<'a> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let fields = self.fields.selected_fields();
        let mut state = serializer.serialize_struct("Extrinsic", fields.len() + 2)?;
        state.serialize_field("id", &self.extrinsic.id)?;
        state.serialize_field("pos", &self.extrinsic.pos)?;
        for field in fields {
            match field {
                "index_in_block" => state.serialize_field("index_in_block", &self.extrinsic.index_in_block)?,
                "version" => state.serialize_field("version", &self.extrinsic.version)?,
                "signature" => state.serialize_field("signature", &self.extrinsic.signature)?,
                "call_id" => state.serialize_field("call_id", &self.extrinsic.call_id)?,
                "fee" => state.serialize_field("fee", &self.extrinsic.fee)?,
                "tip" => state.serialize_field("tip", &self.extrinsic.tip)?,
                "success" => state.serialize_field("success", &self.extrinsic.success)?,
                "error" => state.serialize_field("error", &self.extrinsic.error)?,
                "hash" => state.serialize_field("hash", &self.extrinsic.hash)?,
                _ => panic!("unexpected field"),
            };
        }
        state.end()
    }
}

pub struct EventSerializer<'a> {
    pub event: &'a Event,
    pub fields: &'a EventFields,
}

impl<'a> serde::Serialize for EventSerializer<'a> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let fields = self.fields.selected_fields();
        let mut state = serializer.serialize_struct("Event", fields.len() + 3)?;
        state.serialize_field("id", &self.event.id)?;
        state.serialize_field("pos", &self.event.pos)?;
        state.serialize_field("name", &self.event.name)?;
        for field in fields {
            match field {
                "index_in_block" => state.serialize_field("index_in_block", &self.event.index_in_block)?,
                "phase" => state.serialize_field("phase", &self.event.phase)?,
                "extrinsic_id" => state.serialize_field("extrinsic_id", &self.event.extrinsic_id)?,
                "call_id" => state.serialize_field("call_id", &self.event.call_id)?,
                "args" => state.serialize_field("args", &self.event.args)?,
                _ => panic!("unexpected field"),
            };
        }
        state.end()
    }
}

pub struct CallSerializer<'a> {
    pub call: &'a Call,
    pub fields: &'a CallDataSelection,
}

impl<'a> serde::Serialize for CallSerializer<'a> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let fields = self.fields.selected_fields();
        let mut state = serializer.serialize_struct("Call", fields.len() + 4)?;
        state.serialize_field("id", &self.call.id)?;
        state.serialize_field("pos", &self.call.pos)?;
        state.serialize_field("name", &self.call.name)?;
        state.serialize_field("success", &self.call.success)?;
        for field in fields {
            match field {
                "error" => state.serialize_field("error", &self.call.error)?,
                "origin" => state.serialize_field("origin", &self.call.origin)?,
                "args" => state.serialize_field("args", &self.call.args)?,
                "parent_id" => state.serialize_field("parent_id", &self.call.parent_id)?,
                "extrinsic_id" => state.serialize_field("extrinsic_id", &self.call.extrinsic_id)?,
                _ => panic!("unexpected field"),
            };
        }
        state.end()
    }
}
