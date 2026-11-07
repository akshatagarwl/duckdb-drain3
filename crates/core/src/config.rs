//! Named parameters shared by `drain_train` and `drain_train_ids`, mapped onto [`drain::Config`].

use std::num::NonZeroUsize;

use duckdb_neo::{
    AsDuckDB, FromValue, Result,
    bind_arguments::BindArguments,
    error::Error,
    signature::{FunctionParameter, FunctionParameterKind, SignatureBuilder},
    types::List,
    value::Value,
};

/// One masking rule, as passed in `masks := [{'pattern': ..., 'mask': ...}]`.
#[derive(Debug, AsDuckDB)]
pub struct MaskArg {
    pub pattern: Option<String>,
    pub mask: Option<String>,
}

/// SQL type of the `masks` parameter.
pub const MASKS_TYPE: &str = "STRUCT(pattern VARCHAR, mask VARCHAR)[]";

fn named(p: FunctionParameter) -> FunctionParameter {
    p.kind(FunctionParameterKind::Named)
}

/// Add the Drain configuration parameters (all named, all optional) to a signature.
///
/// Defaults are logpai/Drain3's: `sim_threshold 0.4`, `depth 4`, `max_children 100`,
/// unlimited clusters, no extra delimiters, no masking, `<*>`, numeric tokens parametrized.
pub fn add_params(
    sig: SignatureBuilder,
    link: &impl duckdb_neo::connection::FFILink,
) -> Result<SignatureBuilder> {
    Ok(sig
        .param(named(FunctionParameter::with_default(
            "sim_threshold",
            0.4_f64,
        )))
        .param(named(FunctionParameter::with_default("depth", 4_i64)))
        .param(named(FunctionParameter::with_default(
            "max_children",
            100_i64,
        )))
        .param(named(FunctionParameter::with_default(
            "max_clusters",
            0_i64,
        )))
        .param(named(FunctionParameter::with_default(
            "extra_delimiters",
            List::<String>(Vec::new()),
        )))
        .param(named(
            FunctionParameter::with_type(
                "masks",
                duckdb_neo::logical_type::LogicalType::from_text(link, MASKS_TYPE)?,
            )
            .default_value(List::<MaskArg>(Vec::new())),
        ))
        .param(named(FunctionParameter::with_default(
            "param_str",
            "<*>".to_string(),
        )))
        .param(named(FunctionParameter::with_default(
            "parametrize_numeric_tokens",
            true,
        ))))
}

fn arg(args: &BindArguments<'_>, name: &str) -> Result<Option<Value>> {
    let Some(index) = args.index(name)? else {
        return Ok(None);
    };
    match args.value(index)? {
        Some(v) => Ok(Some(v)),
        None => Err(Error::invalid_input(format!("{name} must be a constant"))),
    }
}

fn get<T: FromValue>(args: &BindArguments<'_>, name: &str) -> Result<Option<T>> {
    match arg(args, name)? {
        Some(v) => T::from_value(&v),
        None => Ok(None),
    }
}

fn int(args: &BindArguments<'_>, name: &str, min: i64) -> Result<Option<usize>> {
    match get::<i64>(args, name)? {
        None => Ok(None),
        Some(v) if v < min => Err(Error::invalid_input(format!(
            "{name} must be >= {min}, got {v}"
        ))),
        Some(v) => usize::try_from(v)
            .map(Some)
            .map_err(|_| Error::invalid_input(format!("{name} is too large: {v}"))),
    }
}

/// Read the named parameters into a validated [`drain::Config`].
pub fn from_bind(args: &BindArguments<'_>) -> Result<drain::Config> {
    let mut cfg = drain::Config::default();
    if let Some(v) = get::<f64>(args, "sim_threshold")? {
        cfg.sim_threshold = v;
    }
    if let Some(v) = int(args, "depth", 3)? {
        cfg.depth = v;
    }
    if let Some(v) = int(args, "max_children", 1)? {
        cfg.max_children = v;
    }
    if let Some(v) = int(args, "max_clusters", 0)? {
        cfg.max_clusters = NonZeroUsize::new(v);
    }
    if let Some(list) = get::<List<String>>(args, "extra_delimiters")? {
        cfg.extra_delimiters = list
            .0
            .into_iter()
            .map(|d| {
                d.ok_or_else(|| Error::invalid_input("extra_delimiters must not contain NULL"))
            })
            .collect::<Result<_>>()?;
        if cfg.extra_delimiters.iter().any(String::is_empty) {
            return Err(Error::invalid_input("extra_delimiters must not contain ''"));
        }
    }
    if let Some(list) = get::<List<MaskArg>>(args, "masks")? {
        cfg.masking = list
            .0
            .into_iter()
            .map(|m| match m {
                Some(MaskArg {
                    pattern: Some(pattern),
                    mask: Some(mask),
                }) => Ok(drain::MaskingInstruction::new(pattern, mask)),
                _ => Err(Error::invalid_input(
                    "masks entries need a non-NULL pattern and mask",
                )),
            })
            .collect::<Result<_>>()?;
    }
    if let Some(v) = get::<String>(args, "param_str")? {
        cfg.param_str = v;
    }
    if let Some(v) = get::<bool>(args, "parametrize_numeric_tokens")? {
        cfg.parametrize_numeric_tokens = v;
    }
    // Validate now (compiles the masks too) so errors surface at bind time.
    drain::Drain::new(cfg.clone()).map_err(|e| Error::invalid_input(format!("drain3: {e}")))?;
    Ok(cfg)
}
