//! Optional C ABI for the native Valkyr client.
//!
//! Functions returning a string allocate it with Rust's allocator. Call
//! [`valkyr_string_free`] exactly once for every non-null returned string.

use crate::Client;
use serde_json::Value;
use std::{
    ffi::{CStr, CString, c_char, c_int},
    ptr,
    sync::Mutex,
    time::Duration,
};
use tokio::runtime::Runtime;
use valkyr_core::{Key, KeyPattern, NamespaceContext};

pub const VALKYR_OK: c_int = 0;
pub const VALKYR_ERROR_INVALID_ARGUMENT: c_int = -1;
pub const VALKYR_ERROR_CONNECTION: c_int = -2;
pub const VALKYR_ERROR_AUTHENTICATION: c_int = -3;
pub const VALKYR_ERROR_SERVER: c_int = -4;
pub const VALKYR_ERROR_JSON: c_int = -5;
pub const VALKYR_ERROR_INTERNAL: c_int = -6;

struct ClientState {
    runtime: Runtime,
    client: Client,
}

/// Opaque C handle. It serializes calls because a native protocol connection
/// is request ordered.
pub struct ValkyrClient {
    state: Mutex<ClientState>,
    last_error: Mutex<String>,
}

fn read_string(value: *const c_char) -> Result<String, c_int> {
    if value.is_null() {
        return Err(VALKYR_ERROR_INVALID_ARGUMENT);
    }
    // SAFETY: callers must provide a valid NUL-terminated UTF-8 C string for
    // the duration of this call; invalid UTF-8 is rejected below.
    unsafe { CStr::from_ptr(value) }
        .to_str()
        .map(str::to_owned)
        .map_err(|_| VALKYR_ERROR_INVALID_ARGUMENT)
}

fn classify(error: &crate::ClientError) -> c_int {
    match error {
        crate::ClientError::Connection(_)
        | crate::ClientError::Closed
        | crate::ClientError::RequestTimeout => VALKYR_ERROR_CONNECTION,
        crate::ClientError::Authentication(_) => VALKYR_ERROR_AUTHENTICATION,
        crate::ClientError::Server(_) => VALKYR_ERROR_SERVER,
        crate::ClientError::Protocol(_) => VALKYR_ERROR_JSON,
        _ => VALKYR_ERROR_INTERNAL,
    }
}

fn fail(client: &ValkyrClient, code: c_int, error: impl ToString) -> c_int {
    if let Ok(mut last_error) = client.last_error.lock() {
        *last_error = error.to_string();
    }
    code
}

fn client_ref<'a>(client: *mut ValkyrClient) -> Result<&'a ValkyrClient, c_int> {
    if client.is_null() {
        return Err(VALKYR_ERROR_INVALID_ARGUMENT);
    }
    // SAFETY: the pointer is only dereferenced after a null check. The handle
    // is opaque and must have been returned by `valkyr_client_new`.
    Ok(unsafe { &*client })
}

/// Connect and authenticate a client. Returns null on failure.
#[unsafe(no_mangle)]
pub extern "C" fn valkyr_client_new(
    address: *const c_char,
    api_key: *const c_char,
) -> *mut ValkyrClient {
    let Ok(address) = read_string(address) else {
        return ptr::null_mut();
    };
    let Ok(api_key) = read_string(api_key) else {
        return ptr::null_mut();
    };
    let Ok(runtime) = Runtime::new() else {
        return ptr::null_mut();
    };
    let connected = runtime.block_on(async {
        let client = Client::connect(address).await?;
        client.authenticate(api_key, None).await?;
        Ok::<_, crate::ClientError>(client)
    });
    match connected {
        Ok(client) => Box::into_raw(Box::new(ValkyrClient {
            state: Mutex::new(ClientState { runtime, client }),
            last_error: Mutex::new(String::new()),
        })),
        Err(_) => ptr::null_mut(),
    }
}

/// Free a client handle. Passing null is allowed.
#[unsafe(no_mangle)]
pub extern "C" fn valkyr_client_free(client: *mut ValkyrClient) {
    if !client.is_null() {
        // SAFETY: the caller transfers ownership of a handle obtained from
        // `valkyr_client_new` and must not use it again after this call.
        drop(unsafe { Box::from_raw(client) });
    }
}

/// Free a string returned by [`valkyr_client_get`] or
/// [`valkyr_client_last_error`]. Passing null is allowed.
#[unsafe(no_mangle)]
pub extern "C" fn valkyr_string_free(value: *mut c_char) {
    if !value.is_null() {
        // SAFETY: the pointer must have been allocated by CString::into_raw in
        // this module and is consumed exactly once here.
        drop(unsafe { CString::from_raw(value) });
    }
}

/// Return an owned copy of the most recent error message for this handle.
#[unsafe(no_mangle)]
pub extern "C" fn valkyr_client_last_error(client: *mut ValkyrClient) -> *mut c_char {
    let Ok(client) = client_ref(client) else {
        return ptr::null_mut();
    };
    let error = client
        .last_error
        .lock()
        .map(|error| error.clone())
        .unwrap_or_default();
    CString::new(error).map_or(ptr::null_mut(), CString::into_raw)
}

/// Get a value. On success `out_json` receives an allocated JSON string.
#[unsafe(no_mangle)]
pub extern "C" fn valkyr_client_get(
    client: *mut ValkyrClient,
    namespace: *const c_char,
    key: *const c_char,
    out_json: *mut *mut c_char,
) -> c_int {
    if out_json.is_null() {
        return VALKYR_ERROR_INVALID_ARGUMENT;
    }
    // SAFETY: `out_json` is checked for null and initialized before any error
    // path returns, so callers never observe an uninitialized pointer.
    unsafe { *out_json = ptr::null_mut() };
    let Ok(client) = client_ref(client) else {
        return VALKYR_ERROR_INVALID_ARGUMENT;
    };
    let (Ok(namespace), Ok(key)) = (read_string(namespace), read_string(key)) else {
        return fail(
            client,
            VALKYR_ERROR_INVALID_ARGUMENT,
            "namespace and key must be UTF-8 strings",
        );
    };
    let (Ok(namespace), Ok(key)) = (NamespaceContext::new(namespace), Key::new(key)) else {
        return fail(
            client,
            VALKYR_ERROR_INVALID_ARGUMENT,
            "namespace and key cannot be empty",
        );
    };
    let Ok(state) = client.state.lock() else {
        return fail(client, VALKYR_ERROR_INTERNAL, "client state is poisoned");
    };
    match state.runtime.block_on(state.client.get(namespace, key)) {
        Ok(value) => match CString::new(value.to_string()) {
            Ok(value) => {
                // SAFETY: `out_json` was checked above and remains valid for
                // this synchronous call according to the C API contract.
                unsafe { *out_json = value.into_raw() };
                VALKYR_OK
            }
            Err(_) => fail(client, VALKYR_ERROR_INTERNAL, "JSON result contained NUL"),
        },
        Err(error) => fail(client, classify(&error), error),
    }
}

/// Set a JSON value. A TTL of zero means no expiration.
#[unsafe(no_mangle)]
pub extern "C" fn valkyr_client_set(
    client: *mut ValkyrClient,
    namespace: *const c_char,
    key: *const c_char,
    json: *const c_char,
    ttl_seconds: u64,
) -> c_int {
    let Ok(client) = client_ref(client) else {
        return VALKYR_ERROR_INVALID_ARGUMENT;
    };
    let (Ok(namespace), Ok(key), Ok(json)) =
        (read_string(namespace), read_string(key), read_string(json))
    else {
        return fail(
            client,
            VALKYR_ERROR_INVALID_ARGUMENT,
            "namespace, key, and JSON must be UTF-8 strings",
        );
    };
    let (Ok(namespace), Ok(key), Ok(value)) = (
        NamespaceContext::new(namespace),
        Key::new(key),
        serde_json::from_str::<Value>(&json),
    ) else {
        return fail(
            client,
            VALKYR_ERROR_JSON,
            "invalid namespace, key, or JSON value",
        );
    };
    let Ok(state) = client.state.lock() else {
        return fail(client, VALKYR_ERROR_INTERNAL, "client state is poisoned");
    };
    match state.runtime.block_on(state.client.set(
        namespace,
        key,
        value,
        (ttl_seconds != 0).then(|| Duration::from_secs(ttl_seconds)),
    )) {
        Ok(()) => VALKYR_OK,
        Err(error) => fail(client, classify(&error), error),
    }
}

/// Delete one key or, when `key_pattern` is null, all keys in a namespace.
#[unsafe(no_mangle)]
pub extern "C" fn valkyr_client_delete(
    client: *mut ValkyrClient,
    namespace: *const c_char,
    key_pattern: *const c_char,
) -> c_int {
    let Ok(client) = client_ref(client) else {
        return VALKYR_ERROR_INVALID_ARGUMENT;
    };
    let Ok(namespace) = read_string(namespace)
        .and_then(|value| NamespaceContext::new(value).map_err(|_| VALKYR_ERROR_INVALID_ARGUMENT))
    else {
        return fail(
            client,
            VALKYR_ERROR_INVALID_ARGUMENT,
            "namespace cannot be empty",
        );
    };
    let pattern = if key_pattern.is_null() {
        Ok(None)
    } else {
        read_string(key_pattern).and_then(|value| {
            KeyPattern::new(value)
                .map(Some)
                .map_err(|_| VALKYR_ERROR_INVALID_ARGUMENT)
        })
    };
    let Ok(pattern) = pattern else {
        return fail(
            client,
            VALKYR_ERROR_INVALID_ARGUMENT,
            "key pattern cannot be empty",
        );
    };
    let Ok(state) = client.state.lock() else {
        return fail(client, VALKYR_ERROR_INTERNAL, "client state is poisoned");
    };
    match state
        .runtime
        .block_on(state.client.delete(namespace, pattern))
    {
        Ok(()) => VALKYR_OK,
        Err(error) => fail(client, classify(&error), error),
    }
}

/// Move every value from one namespace to another.
#[unsafe(no_mangle)]
pub extern "C" fn valkyr_client_move(
    client: *mut ValkyrClient,
    source: *const c_char,
    destination: *const c_char,
) -> c_int {
    let Ok(client) = client_ref(client) else {
        return VALKYR_ERROR_INVALID_ARGUMENT;
    };
    let (Ok(source), Ok(destination)) = (read_string(source), read_string(destination)) else {
        return fail(
            client,
            VALKYR_ERROR_INVALID_ARGUMENT,
            "source and destination must be UTF-8 strings",
        );
    };
    let (Ok(source), Ok(destination)) = (
        NamespaceContext::new(source),
        NamespaceContext::new(destination),
    ) else {
        return fail(
            client,
            VALKYR_ERROR_INVALID_ARGUMENT,
            "source and destination cannot be empty",
        );
    };
    let Ok(state) = client.state.lock() else {
        return fail(client, VALKYR_ERROR_INTERNAL, "client state is poisoned");
    };
    match state
        .runtime
        .block_on(state.client.move_namespace(source, destination))
    {
        Ok(()) => VALKYR_OK,
        Err(error) => fail(client, classify(&error), error),
    }
}
