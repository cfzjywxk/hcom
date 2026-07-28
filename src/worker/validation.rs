use anyhow::{Result, bail};
use std::collections::BTreeSet;
use std::path::{Component, Path};

pub(crate) const MAX_ITEMS: usize = 256;
pub(crate) const MAX_TEXT_BYTES: usize = 64 * 1024;
pub(crate) const MAX_PATH_BYTES: usize = 4096;

pub(crate) fn validate_text(
    label: &str,
    value: &str,
    max_bytes: usize,
    allow_newlines: bool,
) -> Result<()> {
    if value.is_empty() || value.len() > max_bytes {
        bail!("{label} must contain between 1 and {max_bytes} bytes");
    }
    if value.chars().any(|character| {
        character == '\u{1b}'
            || character == '\r'
            || ('\u{80}'..='\u{9f}').contains(&character)
            || (character.is_control() && character != '\n' && character != '\t')
            || (!allow_newlines && matches!(character, '\n' | '\t'))
    }) {
        bail!("{label} contains forbidden terminal control characters");
    }
    Ok(())
}

pub(crate) fn validate_list<T>(label: &str, values: &[T]) -> Result<()> {
    if values.len() > MAX_ITEMS {
        bail!("{label} exceeds the bounded item count");
    }
    Ok(())
}

pub(crate) fn validate_unique_texts(
    label: &str,
    values: &[String],
    max_bytes: usize,
    allow_newlines: bool,
) -> Result<()> {
    validate_list(label, values)?;
    let mut unique = BTreeSet::new();
    for value in values {
        validate_text(label, value, max_bytes, allow_newlines)?;
        if !unique.insert(value) {
            bail!("{label} entries must be unique");
        }
    }
    Ok(())
}

pub(crate) fn validate_git_oid(label: &str, value: &str) -> Result<()> {
    if !matches!(value.len(), 40 | 64)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("{label} must be a full lowercase Git object ID");
    }
    Ok(())
}

pub(crate) fn validate_sha256(label: &str, value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("{label} must be a lowercase sha256 digest");
    }
    Ok(())
}

pub(crate) fn validate_relative_path(label: &str, value: &str) -> Result<()> {
    validate_text(label, value, MAX_PATH_BYTES, false)?;
    let path = Path::new(value);
    if path.is_absolute()
        || !path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        bail!("{label} must be a safe workspace-relative path");
    }
    Ok(())
}

pub(crate) fn validate_opaque_id(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
    {
        bail!("{label} is not a bounded opaque identifier");
    }
    Ok(())
}
