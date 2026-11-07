//! Scalar functions: `drain_match`, `drain_clusters`, `drain_params`.
//!
//! Matching never changes the model, so these run on every DuckDB thread in parallel.
//! Each worker decodes a model BLOB once and reuses it while the BLOB stays the same.

use drain::{Drain, Search};
use duckdb_neo::{
    AsDuckDB, DuckDBType, Result,
    bind_arguments::BindArguments,
    connection::Context,
    data_chunk::VectorCollection,
    error::Error,
    extension::Extension,
    signature::{FunctionDocumentationBuilder, FunctionParameter, FunctionParameterKind, SignatureBuilder},
    scalar::{ScalarCallbacks, ScalarFunctionBuilder, ScalarInitCallbacks},
    types::{BlobValue, List},
    vector::{Unknown, Vector},
};
use regex::Regex;
use rustc_hash::FxHashMap;

use crate::{CATEGORY, model};

/// A decoded model, kept per worker and keyed by its BLOB bytes.
#[derive(Default)]
pub struct ModelCache {
    bytes: Vec<u8>,
    drain: Option<Drain>,
}

impl ModelCache {
    /// The decoded model for `bytes`. `chunk_hit` short-cuts the byte comparison for rows of
    /// one chunk that point at the same BLOB (a constant or repeated model column).
    fn get(&mut self, bytes: &[u8], chunk_hit: &mut Option<(usize, usize)>) -> Result<&Drain> {
        let key = (bytes.as_ptr() as usize, bytes.len());
        let same = *chunk_hit == Some(key) || (self.drain.is_some() && self.bytes == bytes);
        if !same {
            self.drain = Some(model::decode(bytes)?);
            self.bytes.clear();
            self.bytes.extend_from_slice(bytes);
        }
        *chunk_hit = Some(key);
        Ok(self.drain.as_ref().expect("decoded above"))
    }
}

// ---------------------------------------------------------------- drain_match

/// Result of `drain_match`.
#[derive(Debug, AsDuckDB)]
pub struct Match {
    pub cluster_id: u64,
    pub template: String,
    pub params: List<String>,
}

struct MatchFn;

fn parse_search(s: &str) -> Result<Search> {
    match s.to_ascii_lowercase().as_str() {
        "never" => Ok(Search::Never),
        "fallback" => Ok(Search::Fallback),
        "always" => Ok(Search::Always),
        _ => Err(Error::invalid_input(format!(
            "drain_match: search must be 'never', 'fallback' or 'always', got '{s}'"
        ))),
    }
}

#[derive(PartialEq)]
pub struct SearchBind(Search);

impl ScalarInitCallbacks for MatchFn {
    fn init(&self, _bind_data: Option<&Self::BindData>, _context: &Context) -> Result<Self::InitData> {
        Ok(ModelCache::default())
    }
}

impl duckdb_neo::scalar::ScalarBindCallbacks for MatchFn {
    fn bind(&self, _context: &Context, arguments: BindArguments<'_>) -> Result<Self::BindData> {
        let search = match arguments.index("search")? {
            None => Search::Never,
            Some(i) => match arguments.value(i)? {
                None => return Err(Error::invalid_input("drain_match: search must be a constant")),
                Some(v) => match v.get::<String>()? {
                    None => Search::Never,
                    Some(s) => parse_search(&s)?,
                },
            },
        };
        Ok(SearchBind(search))
    }
}

impl ScalarCallbacks for MatchFn {
    type BindData = SearchBind;
    type InitData = ModelCache;

    fn exec(
        &self,
        bind_data: Option<&Self::BindData>,
        init_data: Option<&mut Self::InitData>,
        _context: &Context,
        vectors: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let search = bind_data.map_or(Search::Never, |b| b.0);
        let cache = init_data.ok_or_else(|| Error::internal("drain_match: missing state"))?;
        let rows = vectors.row_count()?;
        let models = vectors.get_vector_at::<BlobValue>(0)?.reader()?;
        let lines = vectors.get_vector_at::<String>(1)?.reader()?;

        let mut out = output.cast::<Match>()?;
        out.set_size(rows)?;
        // First pass: match, collecting results, so the params list child can be sized exactly.
        let mut hit = None;
        let mut results: Vec<Option<(u64, usize, Vec<&str>)>> = Vec::with_capacity(rows);
        let mut total = 0usize;
        let mut templates: Vec<String> = Vec::new();
        let mut template_of: FxHashMap<u64, usize> = FxHashMap::default();
        let mut model_gen = 0usize;
        let mut last_model: Option<(usize, usize)> = None;
        for row in 0..rows {
            let (Some(m), Some(line)) = (models.get(row)?, lines.get(row)?) else {
                results.push(None);
                continue;
            };
            let key = (m.as_ptr() as usize, m.len());
            let drain = cache.get(m, &mut hit)?;
            if last_model != Some(key) {
                // A different model BLOB: cluster ids are not comparable across models.
                last_model = Some(key);
                model_gen += 1;
                template_of.clear();
            }
            let _ = model_gen;
            match drain.parse_with(line, search) {
                None => results.push(None),
                Some(p) => {
                    let id = p.cluster_id.get();
                    let t = *template_of.entry(id).or_insert_with(|| {
                        templates.push(drain.cluster(p.cluster_id).map(|c| c.template().to_string()).unwrap_or_default());
                        templates.len() - 1
                    });
                    total += p.params.len();
                    results.push(Some((id, t, p.params.iter().map(|x| x.value).collect())));
                }
            }
        }
        {
            let mut children = out
                .children_mut()
                .ok_or_else(|| Error::internal("drain_match: STRUCT result without children"))?;
            let mut params = children.get_vector_at_mut::<List<String>>(2)?;
            let base = params.child_offset();
            // SAFETY: exactly `total` elements are written below.
            unsafe { params.set_child_size(base + total)? };
        }
        let mut writer = out.writer()?;
        for (row, r) in results.into_iter().enumerate() {
            match r {
                None => writer.set(row, None)?,
                Some((id, t, params)) => writer.set(
                    row,
                    Some(Match {
                        cluster_id: id,
                        template: templates[t].clone(),
                        params: params.into_iter().map(|s| Some(s.to_string())).collect(),
                    }),
                )?,
            }
        }
        Ok(())
    }
}

// ------------------------------------------------------------- drain_clusters

/// One row of `drain_clusters`.
#[derive(Debug, AsDuckDB)]
pub struct ClusterRow {
    pub cluster_id: u64,
    pub size: u64,
    pub template: String,
}

struct ClustersFn;

impl ScalarInitCallbacks for ClustersFn {
    fn init(&self, _bind_data: Option<&Self::BindData>, _context: &Context) -> Result<Self::InitData> {
        Ok(ModelCache::default())
    }
}

impl ScalarCallbacks for ClustersFn {
    type BindData = ();
    type InitData = ModelCache;

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        init_data: Option<&mut Self::InitData>,
        _context: &Context,
        vectors: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let cache = init_data.ok_or_else(|| Error::internal("drain_clusters: missing state"))?;
        let rows = vectors.row_count()?;
        let models = vectors.get_vector_at::<BlobValue>(0)?.reader()?;
        let mut hit = None;
        let mut lists: Vec<Option<Vec<ClusterRow>>> = Vec::with_capacity(rows);
        let mut total = 0;
        for row in 0..rows {
            match models.get(row)? {
                None => lists.push(None),
                Some(m) => {
                    let d = cache.get(m, &mut hit)?;
                    let mut v: Vec<ClusterRow> = d
                        .clusters()
                        .map(|c| ClusterRow {
                            cluster_id: c.id().get(),
                            size: c.size(),
                            template: c.template().to_string(),
                        })
                        .collect();
                    v.sort_by_key(|c| c.cluster_id);
                    total += v.len();
                    lists.push(Some(v));
                }
            }
        }
        let mut out = output.cast::<List<ClusterRow>>()?;
        out.set_size(rows)?;
        // SAFETY: exactly `total` elements are written below.
        unsafe { out.set_child_size(out.child_offset() + total)? };
        let mut writer = out.writer()?;
        for (row, l) in lists.into_iter().enumerate() {
            writer.set(row, l.map(|v| List(v.into_iter().map(Some).collect())))?;
        }
        Ok(())
    }
}

// --------------------------------------------------------------- drain_params

/// Compile a template into an anchored regex with one group per parameter.
///
/// `<*>` and `<NAME>` tokens (NAME = upper-case letters, digits, `_`, the logpai mask
/// convention) become lazy `(.+?)` groups, literal text is escaped, and runs of whitespace
/// match any whitespace, like logpai/Drain3's `extract_parameters` (non-exact mode).
pub fn template_regex(template: &str, delimiters: &[String]) -> Option<Regex> {
    let mask = Regex::new(r"<\*>|<[A-Z0-9_]+>").expect("static regex");
    let mut re = String::from(r"(?s)^\s*");
    for (i, tok) in template.split_whitespace().enumerate() {
        if i > 0 {
            re.push_str(r"\s+");
        }
        let mut last = 0;
        for m in mask.find_iter(tok) {
            re.push_str(&regex::escape(&tok[last..m.start()]));
            re.push_str("(.+?)");
            last = m.end();
        }
        re.push_str(&regex::escape(&tok[last..]));
    }
    re.push_str(r"\s*$");
    let _ = delimiters;
    Regex::new(&re).ok()
}

#[derive(PartialEq)]
pub struct ParamsBind {
    delimiters: Vec<String>,
}

#[derive(Default)]
pub struct RegexCache {
    map: FxHashMap<String, Option<Regex>>,
}

const REGEX_CACHE_MAX: usize = 4096;

struct ParamsFn;

impl duckdb_neo::scalar::ScalarBindCallbacks for ParamsFn {
    fn bind(&self, _context: &Context, arguments: BindArguments<'_>) -> Result<Self::BindData> {
        let mut delimiters = Vec::new();
        if let Some(i) = arguments.index("extra_delimiters")? {
            let v = arguments
                .value(i)?
                .ok_or_else(|| Error::invalid_input("drain_params: extra_delimiters must be a constant"))?;
            if let Some(list) = v.get::<List<String>>()? {
                for d in list.0 {
                    match d {
                        Some(d) if !d.is_empty() => delimiters.push(d),
                        _ => {
                            return Err(Error::invalid_input(
                                "drain_params: extra_delimiters must not contain NULL or ''",
                            ));
                        }
                    }
                }
            }
        }
        Ok(ParamsBind { delimiters })
    }
}

impl ScalarInitCallbacks for ParamsFn {
    fn init(&self, _bind_data: Option<&Self::BindData>, _context: &Context) -> Result<Self::InitData> {
        Ok(RegexCache::default())
    }
}

impl ScalarCallbacks for ParamsFn {
    type BindData = ParamsBind;
    type InitData = RegexCache;

    fn exec(
        &self,
        bind_data: Option<&Self::BindData>,
        init_data: Option<&mut Self::InitData>,
        _context: &Context,
        vectors: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let delimiters: &[String] = bind_data.map_or(&[], |b| &b.delimiters);
        let cache = init_data.ok_or_else(|| Error::internal("drain_params: missing state"))?;
        let rows = vectors.row_count()?;
        let templates = vectors.get_vector_at::<String>(0)?.reader()?;
        let lines = vectors.get_vector_at::<String>(1)?.reader()?;
        let mut results: Vec<Option<Vec<String>>> = Vec::with_capacity(rows);
        let mut total = 0;
        let mut buf = String::new();
        for row in 0..rows {
            let (Some(t), Some(line)) = (templates.get(row)?, lines.get(row)?) else {
                results.push(None);
                continue;
            };
            if cache.map.len() >= REGEX_CACHE_MAX && !cache.map.contains_key(t) {
                cache.map.clear();
            }
            let re = cache
                .map
                .entry(t.to_string())
                .or_insert_with(|| template_regex(t, delimiters));
            let Some(re) = re else {
                results.push(None);
                continue;
            };
            let subject: &str = if delimiters.is_empty() {
                line
            } else {
                buf.clear();
                buf.push_str(line);
                for d in delimiters {
                    buf = buf.replace(d.as_str(), " ");
                }
                &buf
            };
            match re.captures(subject) {
                None => results.push(None),
                Some(caps) => {
                    let v: Vec<String> = caps.iter().skip(1).map(|m| m.map_or("", |m| m.as_str()).to_string()).collect();
                    total += v.len();
                    results.push(Some(v));
                }
            }
        }
        let mut out = output.cast::<List<String>>()?;
        out.set_size(rows)?;
        // SAFETY: exactly `total` elements are written below.
        unsafe { out.set_child_size(out.child_offset() + total)? };
        let mut writer = out.writer()?;
        for (row, r) in results.into_iter().enumerate() {
            writer.set(row, r.as_ref().map(|v| List(v.iter().map(|s| Some(s.as_str())).collect())))?;
        }
        Ok(())
    }
}

pub fn register(extension: &Extension) -> Result<()> {
    ScalarFunctionBuilder::new(
        "drain_match",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<BlobValue>("model"))
            .param(FunctionParameter::new::<String>("message"))
            .param(FunctionParameter::with_default("search", "never".to_string()).kind(FunctionParameterKind::Named))
            .return_type(Match::logical_type(extension)?)
            .documentation(
                FunctionDocumentationBuilder::new()
                    .description(
                        "Match a message against a trained model without changing it. Returns \
                         {cluster_id, template, params}, or NULL when no cluster matches. \
                         search := 'never' (default, logpai's match), 'fallback' or 'always'.",
                    )
                    .add_example(
                        "Match new logs against yesterday's model",
                        "SELECT drain_match(m.model, l.message).template FROM logs l, models m",
                    )
                    .add_category(CATEGORY),
            ),
        MatchFn,
    )
    .with_bind()
    .with_init()
    .register(extension)?;

    ScalarFunctionBuilder::new(
        "drain_clusters",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<BlobValue>("model"))
            .return_type(List::<ClusterRow>::logical_type(extension)?)
            .documentation(
                FunctionDocumentationBuilder::new()
                    .description("The clusters of a model as a list of {cluster_id, size, template}, by id.")
                    .add_example(
                        "Top templates",
                        "SELECT c.* FROM (SELECT unnest(drain_clusters(drain_train(message ORDER BY ts))) AS c FROM logs) ORDER BY c.size DESC",
                    )
                    .add_category(CATEGORY),
            ),
        ClustersFn,
    )
    .with_init()
    .register(extension)?;

    ScalarFunctionBuilder::new(
        "drain_params",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("template"))
            .param(FunctionParameter::new::<String>("message"))
            .param(
                FunctionParameter::with_default("extra_delimiters", List::<String>(Vec::new()))
                    .kind(FunctionParameterKind::Named),
            )
            .return_type(List::<String>::logical_type(extension)?)
            .documentation(
                FunctionDocumentationBuilder::new()
                    .description(
                        "The variable parts of a message under a template: one value per <*> or <MASK> \
                         token, in order. NULL when the message does not fit the template. Same rules as \
                         logpai/Drain3's extract_parameters.",
                    )
                    .add_example(
                        "Extract",
                        "SELECT drain_params('user <*> logged in from <IP>', 'user bob logged in from 10.0.0.1')",
                    )
                    .add_category(CATEGORY),
            ),
        ParamsFn,
    )
    .with_bind()
    .with_init()
    .register(extension)
}
