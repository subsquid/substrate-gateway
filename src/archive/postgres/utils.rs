use std::collections::HashMap;
use serde_json::{Value, Map};

// removes duplicates and merge fields between two entities with the same id
pub fn unify_and_merge(values: Vec<Value>, fields: Vec<&str>) -> Vec<Value> {
    let mut instances_by_id = HashMap::new();
    for value in values {
        let id = value.get("id").unwrap().clone().as_str().unwrap().to_string();
        instances_by_id.entry(id).or_insert_with(Vec::new).push(value);
    }
    instances_by_id.values()
        .into_iter()
        .map(|duplicates| {
            let mut object = Map::new();
            for field in &fields {
                let instance = duplicates.into_iter()
                    .find(|instance| instance.get(field).is_some());
                if let Some(instance) = instance{
                    object.insert(field.to_string(), instance.get(field).unwrap().clone());
                }
            }
            Value::from(object)
        })
        .collect()
}
