use std::io::Read as IoRead;
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::types::CommandError;

/// Compute the SHA-256 hex digest of a file.
pub(crate) fn compute_file_sha256(path: &Path) -> Result<String, CommandError> {
    let mut f = std::fs::File::open(path)
        .map_err(|e| CommandError::Io(format!("Cannot open '{}': {e}", path.display())))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = std::io::Read::read(&mut f, &mut buf)
            .map_err(|e| CommandError::Io(format!("Read error on '{}': {e}", path.display())))?;
        if n == 0 { break; }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Resolve the path to the `dacpac-extract` sidecar binary.
///
/// Search order:
/// 1. `DACPAC_EXTRACT_BIN` env var (test / CI override).
/// 2. Next to the current executable (Tauri dev + production bundle).
/// 3. `src-tauri/binaries/` relative to the crate manifest (dev fallback via
///    `CARGO_MANIFEST_DIR`, embedded at compile time).
fn dacpac_extract_bin() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("DACPAC_EXTRACT_BIN") {
        return std::path::PathBuf::from(p);
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let candidate = dir.join("dacpac-extract");
            if candidate.exists() {
                return candidate;
            }
        }
    }
    // Dev fallback: binaries/ next to Cargo.toml (CARGO_MANIFEST_DIR is compile-time).
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("binaries")
        .join("dacpac-extract")
}

/// Extract DDL from a `.dacpac` file via the `dacpac-extract` DacFx sidecar.
/// Writes `procedures.sql`, `views.sql`, `functions.sql`, `tables.sql` to `ddl_dir`.
pub(crate) fn extract_ddl_from_dacpac(dacpac_path: &Path, ddl_dir: &Path) -> Result<(), CommandError> {
    log::info!("[extract_ddl_from_dacpac] extracting via DacFx sidecar: {} → {}",
        dacpac_path.display(), ddl_dir.display());

    std::fs::create_dir_all(ddl_dir)
        .map_err(|e| CommandError::Io(format!("Cannot create DDL dir: {e}")))?;

    let bin = dacpac_extract_bin();
    if !bin.exists() {
        return Err(CommandError::External(format!(
            "dacpac-extract sidecar not found at '{}'. \
             Run scripts/build-dacpac-extract.sh to build it.",
            bin.display()
        )));
    }

    let output = std::process::Command::new(&bin)
        .arg(dacpac_path)
        .arg(ddl_dir)
        .output()
        .map_err(|e| CommandError::External(format!("Failed to launch dacpac-extract: {e}")))?;

    let stderr = String::from_utf8_lossy(&output.stderr);
    if output.status.success() {
        log::info!("[extract_ddl_from_dacpac] sidecar: {}", stderr.trim());
        Ok(())
    } else {
        log::error!("[extract_ddl_from_dacpac] sidecar failed: {stderr}");
        Err(CommandError::External(format!("dacpac-extract failed: {stderr}")))
    }
}

/// Extract DDL from a `.zip` source file (Fabric Warehouse, Snowflake, etc.) into `ddl_dir`.
/// The ZIP is expected to contain `.sql` files; they are organized by name pattern into
/// the canonical DDL output files, or written to `other.sql` if unclassified.
pub(crate) fn extract_ddl_from_zip(zip_path: &Path, ddl_dir: &Path) -> Result<(), CommandError> {
    log::info!("[extract_ddl_from_zip] extracting from {} → {}", zip_path.display(), ddl_dir.display());

    let file = std::fs::File::open(zip_path)
        .map_err(|e| CommandError::Io(format!("Cannot open zip: {e}")))?;
    let mut archive = zip::ZipArchive::new(file)
        .map_err(|e| CommandError::Io(format!("Not a valid ZIP: {e}")))?;

    std::fs::create_dir_all(ddl_dir)
        .map_err(|e| CommandError::Io(format!("Cannot create DDL dir: {e}")))?;

    let mut by_type: std::collections::HashMap<&str, Vec<String>> = std::collections::HashMap::new();
    by_type.insert("tables", Vec::new());
    by_type.insert("procedures", Vec::new());
    by_type.insert("views", Vec::new());
    by_type.insert("functions", Vec::new());
    by_type.insert("other", Vec::new());

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)
            .map_err(|e| CommandError::Io(format!("ZIP read error: {e}")))?;
        if entry.is_dir() { continue; }
        let name = entry.name().to_lowercase();
        if !name.ends_with(".sql") { continue; }

        let mut content = String::new();
        entry.read_to_string(&mut content).map_err(|e| CommandError::Io(format!("ZIP entry read error: {e}")))?;

        let bucket = if name.contains("procedure") || name.contains("proc") || name.contains("/sp/") {
            "procedures"
        } else if name.contains("view") || name.contains("/vw/") {
            "views"
        } else if name.contains("function") || name.contains("/fn/") || name.contains("/udf/") {
            "functions"
        } else if name.contains("table") || name.contains("/tbl/") {
            "tables"
        } else {
            "other"
        };

        by_type.entry(bucket).or_default().push(format!("-- {}\n{}\nGO\n\n", entry.name(), content));
    }

    for (key, blocks) in &by_type {
        if !blocks.is_empty() || *key != "other" {
            let path = ddl_dir.join(format!("{key}.sql"));
            write_ddl_file(path, &format!("-- {key}\n-- Generated by Migration Utility\n\n"), blocks)?;
        }
    }

    log::info!(
        "[extract_ddl_from_zip] extracted: {} proc, {} view, {} fn, {} table, {} other",
        by_type["procedures"].len(), by_type["views"].len(), by_type["functions"].len(),
        by_type["tables"].len(), by_type["other"].len()
    );
    Ok(())
}

/// Write a DDL output file: header + joined content blocks.
fn write_ddl_file(path: impl AsRef<Path>, header: &str, blocks: &[String]) -> Result<(), CommandError> {
    let mut content = String::with_capacity(header.len() + blocks.iter().map(|b| b.len()).sum::<usize>());
    content.push_str(header);
    for block in blocks {
        content.push_str(block);
    }
    std::fs::write(&path, content)
        .map_err(|e| CommandError::Io(format!("Cannot write {}: {e}", path.as_ref().display())))
}

/// Returns `Ok(false)` if DDL is current, `Ok(true)` if stale/missing, `Err` if check itself fails.
pub(crate) fn check_ddl_stale(
    metadata_path: &Path,
    source_dir: &Path,
    ddl_dir: &Path,
) -> Result<bool, CommandError> {
    if !metadata_path.exists() {
        return Err(CommandError::Validation(format!(
            "metadata.json not found at '{}' — project may not have been properly created",
            metadata_path.display()
        )));
    }

    let content = std::fs::read_to_string(metadata_path)
        .map_err(|e| CommandError::Io(format!("Cannot read metadata.json: {e}")))?;
    let metadata: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| CommandError::Io(format!("Cannot parse metadata.json: {e}")))?;

    let expected_sha = metadata["sourceSha256"].as_str().unwrap_or("");
    let source_filename = metadata["sourceFilename"].as_str().unwrap_or("");

    if source_filename.is_empty() || expected_sha.is_empty() {
        log::warn!("[check_ddl_stale] metadata.json missing sourceFilename or sourceSha256 — treating DDL as stale");
        return Ok(true);
    }

    let source_path = source_dir.join(source_filename);
    if !source_path.exists() {
        log::warn!("[check_ddl_stale] source file '{}' not found — DDL stale", source_path.display());
        return Ok(true);
    }

    let actual_sha = compute_file_sha256(&source_path)?;
    if actual_sha != expected_sha {
        log::warn!("[check_ddl_stale] source SHA256 mismatch (expected={expected_sha}, actual={actual_sha}) — DDL stale");
        return Ok(true);
    }

    // Check that at least one DDL file exists and contains real content
    // (not the "no column definitions found" stub from the old XML parser).
    let ddl_files = ["procedures.sql", "views.sql", "functions.sql", "tables.sql"];
    let has_ddl = ddl_files.iter().any(|f| ddl_dir.join(f).exists());
    if !has_ddl {
        log::warn!("[check_ddl_stale] no DDL files found in '{}' — DDL stale", ddl_dir.display());
        return Ok(true);
    }
    let has_stub = ddl_files.iter().any(|f| {
        let path = ddl_dir.join(f);
        std::fs::read_to_string(&path)
            .map(|s| s.contains("no column definitions found"))
            .unwrap_or(false)
    });
    if has_stub {
        log::warn!("[check_ddl_stale] DDL contains stub content from old XML parser — re-extracting");
        return Ok(true);
    }

    Ok(false)
}

/// Extract the source database name from a DacPac file by reading DacMetadata.xml.
/// DacPac files are ZIP archives; DacMetadata.xml contains the `<Name>` element.
/// Uses the `zip` crate directly instead of shelling out to `unzip` (which is unavailable on Windows).
pub(crate) fn dacpac_db_name(dacpac_path: &str) -> Result<String, CommandError> {
    log::debug!("[dacpac_db_name] reading DacMetadata.xml from {dacpac_path}");
    let file = std::fs::File::open(dacpac_path)
        .map_err(|e| CommandError::Io(format!("Cannot open DacPac '{}': {e}", dacpac_path)))?;
    let mut archive = zip::ZipArchive::new(file)
        .map_err(|e| CommandError::External(format!("Not a valid DacPac (ZIP): {e}")))?;
    let mut entry = archive.by_name("DacMetadata.xml")
        .map_err(|_| CommandError::External("DacMetadata.xml not found in DacPac — is this a valid DacPac?".into()))?;
    let mut xml = String::new();
    entry.read_to_string(&mut xml)
        .map_err(|e| CommandError::Io(format!("Failed to read DacMetadata.xml: {e}")))?;
    // Parse <Name>...</Name> from the XML (DacMetadata.xml is simple and well-formed).
    for line in xml.lines() {
        let trimmed = line.trim();
        if let Some(inner) = trimmed.strip_prefix("<Name>").and_then(|s| s.strip_suffix("</Name>")) {
            if !inner.is_empty() {
                log::debug!("[dacpac_db_name] db_name={inner}");
                return Ok(inner.to_string());
            }
        }
    }
    Err(CommandError::External("<Name> not found in DacMetadata.xml — is this a valid DacPac?".into()))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dacpac_db_name_extracts_name_from_zip() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let dacpac_path = dir.path().join("test.dacpac");

        // Build a minimal ZIP containing DacMetadata.xml with a <Name> element.
        let file = std::fs::File::create(&dacpac_path).unwrap();
        let mut zip_writer = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        zip_writer.start_file("DacMetadata.xml", options).unwrap();
        zip_writer
            .write_all(b"<?xml version=\"1.0\"?>\n<DacMetadata>\n  <Name>Contoso_DW</Name>\n</DacMetadata>")
            .unwrap();
        zip_writer.finish().unwrap();

        let result = dacpac_db_name(dacpac_path.to_str().unwrap()).unwrap();
        assert_eq!(result, "Contoso_DW");
    }

    #[test]
    fn dacpac_db_name_errors_on_missing_metadata() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let dacpac_path = dir.path().join("empty.dacpac");

        let file = std::fs::File::create(&dacpac_path).unwrap();
        let mut zip_writer = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        zip_writer.start_file("model.xml", options).unwrap();
        zip_writer.write_all(b"<root/>").unwrap();
        zip_writer.finish().unwrap();

        let result = dacpac_db_name(dacpac_path.to_str().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("DacMetadata.xml not found"));
    }
}
