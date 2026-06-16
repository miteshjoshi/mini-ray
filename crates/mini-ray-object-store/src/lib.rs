use mini_ray_core::{decode, encode, MiniRayError, ObjectId, ObjectRef, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Default)]
pub struct InMemoryObjectStore {
    objects: Arc<RwLock<HashMap<ObjectId, Vec<u8>>>>,
}

impl InMemoryObjectStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn put_bytes(&self, id: ObjectId, bytes: Vec<u8>) -> ObjectId {
        self.objects.write().await.insert(id, bytes);
        id
    }

    pub async fn put<T>(&self, value: T) -> Result<ObjectRef<T>>
    where
        T: Serialize + Send + Sync + 'static,
    {
        let id = ObjectId::new();
        let bytes = encode(&value)?;
        self.put_bytes(id, bytes).await;
        Ok(ObjectRef::new(id))
    }

    pub async fn get_bytes(&self, id: ObjectId) -> Result<Vec<u8>> {
        self.objects
            .read()
            .await
            .get(&id)
            .cloned()
            .ok_or(MiniRayError::MissingObject(id))
    }

    pub async fn get<T>(&self, object_ref: ObjectRef<T>) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let bytes = self.get_bytes(object_ref.id()).await?;
        decode(&bytes)
    }

    pub async fn contains(&self, id: ObjectId) -> bool {
        self.objects.read().await.contains_key(&id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mini_ray_core::ObjectRef;

    #[tokio::test]
    async fn put_and_get_round_trip() {
        let store = InMemoryObjectStore::new();
        let object_ref = store.put(41u64).await.unwrap();
        let value: u64 = store.get(object_ref).await.unwrap();
        assert_eq!(value, 41);
    }

    #[tokio::test]
    async fn missing_object_returns_error() {
        let store = InMemoryObjectStore::new();
        let err = store.get_bytes(ObjectId::new()).await.unwrap_err();
        assert!(matches!(err, MiniRayError::MissingObject(_)));
    }

    #[tokio::test]
    async fn decode_error_is_explicit() {
        let store = InMemoryObjectStore::new();
        let id = ObjectId::new();
        store.put_bytes(id, vec![1, 2, 3]).await;

        let err = store.get(ObjectRef::<String>::new(id)).await.unwrap_err();
        assert!(matches!(err, MiniRayError::Decode(_)));
    }
}
