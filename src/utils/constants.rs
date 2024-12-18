use std::{env as std_env, sync::LazyLock};

use dotenvy::dotenv;
use secrecy::Secret;

pub const MAX_DB_CONS: u32 = 100;

pub mod env {
    pub const DATABASE_URL_ENV_VAR: &str = "DATABASE_URL";
}

pub static DATABASE_URL: LazyLock<Secret<String>> = LazyLock::new(|| {
    dotenv().ok();
    let secret = std_env::var(env::DATABASE_URL_ENV_VAR)
        .expect("DATABASE_URL_ENV_VAR must be set.");
    if secret.is_empty() {
        panic!("DATABASE_URL_ENV_VAR must not be empty.");
    }
    Secret::new(secret)
});