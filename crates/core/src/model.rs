//! The model BLOB: a versioned, self-describing snapshot of a trained [`drain::Drain`].
//!
//! Layout: the 8-byte magic `DRAIN3M\x01`, then the [`drain::Snapshot`] (config, prefix tree,
//! clusters, LRU order) encoded with `postcard`. Decoding validates the snapshot, so a
//! corrupt or foreign BLOB is an error, never a crash.

use drain::{Drain, Snapshot};
use duckdb_neo::{Result, error::Error};
use rustc_hash::FxHashMap;

/// Magic prefix; the last byte is the format version.
pub const MAGIC: &[u8; 8] = b"DRAIN3M\x01";

/// Serialize a trained miner.
pub fn encode(drain: &Drain) -> Result<Vec<u8>> {
    let mut out = MAGIC.to_vec();
    let body = postcard::to_stdvec(&drain.snapshot())
        .map_err(|e| Error::internal(format!("drain3: cannot encode model: {e}")))?;
    out.extend_from_slice(&body);
    Ok(out)
}

/// Deserialize and validate a model BLOB.
pub fn decode(bytes: &[u8]) -> Result<Drain> {
    let body = bytes.strip_prefix(MAGIC.as_slice()).ok_or_else(|| {
        if bytes.starts_with(b"DRAIN3M") {
            Error::invalid_input("drain3: model BLOB has an unsupported format version")
        } else {
            Error::invalid_input("drain3: not a drain3 model BLOB (create one with drain_train)")
        }
    })?;
    let snap: Snapshot = postcard::from_bytes(body)
        .map_err(|e| Error::invalid_input(format!("drain3: corrupt model BLOB: {e}")))?;
    Drain::from_snapshot(snap).map_err(|e| Error::invalid_input(format!("drain3: corrupt model BLOB: {e}")))
}

/// Merge `source` into `target` by replaying each source template once, weighted by its size.
/// Returns the map from source cluster ids to target cluster ids.
///
/// Used only when DuckDB trains partial models on several threads (no `ORDER BY` in the
/// aggregate). Drain is order-dependent, so the result approximates training on the
/// concatenated input; with `ORDER BY` DuckDB feeds one ordered stream and this is never needed.
pub fn merge(target: &mut Drain, source: &Drain) -> Result<FxHashMap<u64, u64>> {
    let mut map = FxHashMap::default();
    let mut extra: Vec<(u64, u64)> = Vec::new();
    let mut clusters: Vec<_> = source.clusters().collect();
    clusters.sort_by_key(|c| c.id().get());
    for c in clusters {
        let t = target.train(c.template());
        map.insert(c.id().get(), t.cluster_id.get());
        if c.size() > 1 {
            extra.push((t.cluster_id.get(), c.size() - 1));
        }
    }
    add_sizes(target, &extra)?;
    Ok(map)
}

/// Add `(cluster id, count)` to cluster sizes (snapshot round trip; ids that were evicted are skipped).
pub fn add_sizes(target: &mut Drain, extra: &[(u64, u64)]) -> Result<()> {
    if extra.is_empty() {
        return Ok(());
    }
    let mut add: FxHashMap<u64, u64> = FxHashMap::default();
    for &(id, n) in extra {
        *add.entry(id).or_default() += n;
    }
    let mut snap = target.snapshot();
    for c in &mut snap.clusters {
        if let Some(n) = add.get(&c.id) {
            c.size += n;
        }
    }
    *target = Drain::from_snapshot(snap).map_err(|e| Error::internal(format!("drain3: merge: {e}")))?;
    Ok(())
}
