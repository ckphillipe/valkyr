//! Value routing primitives used by Valkyr servers and clients.

mod broker;
pub mod duration;
mod error;
pub mod line_protocol;
mod memory;
mod miss_cache;
mod pattern;
mod protocol;
mod registry;
mod route;
mod security;
mod store;

pub use broker::{
    AUTH_NAMESPACE, Broker, BrokerOutcome, Dispatch, LEASE_NAMESPACE, PendingMutation,
    ProvideOptions, RequestContext, SECURITY_NAMESPACE, SecurityKeyRecord,
};
pub use error::{Error, Result};
pub use memory::{MemoryStore, MemoryStoreConfig};
pub use pattern::{Capture, Pattern};
pub use protocol::{Command, Response, ServerCommand, ServerResult, SetEntry, Stats};
pub use registry::{
    BatchStoreMatch, ConnectionId, ProviderRegistration, Registry, StoreRegistration,
};
pub use route::{
    Key, KeyPattern, NamespaceContext, NamespacePattern, Route, validate_context_move,
};
pub use security::{
    ApiKeyAuthenticator, AuthInfo, AuthLookup, AuthManager, AuthRecord, AuthSource, Operation,
    Permission, PermissionOperation, Role, SimpleAuthenticator, StoreAuthenticator, ValueCipher,
};
pub use store::{CompositeStore, Store, StoredValue};
