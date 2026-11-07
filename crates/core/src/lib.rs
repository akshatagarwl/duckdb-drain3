//! `drain3`: Drain log template mining inside DuckDB, backed by the
//! [`drain3`](https://crates.io/crates/drain3) crate (logpai/Drain3-identical clusters).
//!
//! | Function | Kind |
//! |---|---|
//! | `drain_parse(table, message_column, order_by := ...)` | table macro |
//! | `drain_train(message ORDER BY ...)` | aggregate, model BLOB |
//! | `drain_train_ids(message ORDER BY ...)` | aggregate, model + per-row cluster ids |
//! | `drain_match(model, message)` | scalar |
//! | `drain_clusters(model)` | scalar |
//! | `drain_params(template, message)` | scalar |

use duckdb_neo::{
    Result,
    extension::Extension,
    macro_function::{MacroBody, MacroFunctionBuilder},
    signature::{FunctionDocumentationBuilder, FunctionParameter, SignatureBuilder},
};

pub mod config;
pub mod model;
pub mod scalar;
pub mod train;

/// The category every function of this extension is documented under in `duckdb_functions()`.
pub const CATEGORY: &str = "drain3";

/// Body of the `drain_parse` table macro.
///
/// Rows are numbered by `order_by` (ties and the default by scan order), trained in that
/// order by `drain_train_ids`, and each row gets the cluster it was assigned while training
/// plus that cluster's final template, exactly like running logpai/Drain3 over the sequence.
const DRAIN_PARSE_SQL: &str = r#"
WITH __drain_scan AS (
    SELECT *, row_number() OVER () AS __drain_scan_no
    FROM query_table(tbl)
),
__drain_src AS (
    SELECT *,
           row_number() OVER (
               ORDER BY COLUMNS(lambda c: c = coalesce(order_by, '__drain_scan_no')), __drain_scan_no
           ) AS __drain_row
    FROM __drain_scan
),
__drain_model AS (
    SELECT drain_train_ids(
               COLUMNS(lambda c: c = message_column)::VARCHAR,
               sim_threshold := sim_threshold,
               depth := depth,
               max_children := max_children,
               max_clusters := max_clusters,
               extra_delimiters := extra_delimiters,
               masks := masks,
               param_str := param_str,
               parametrize_numeric_tokens := parametrize_numeric_tokens
               ORDER BY __drain_row
           ) AS __drain_r
    FROM __drain_src
),
__drain_out AS (
    SELECT __drain_src.* EXCLUDE (__drain_scan_no),
           __drain_r.cluster_ids[__drain_row] AS cluster_id,
           __drain_r.templates[__drain_r.cluster_ids[__drain_row]::BIGINT] AS template
    FROM __drain_src, __drain_model
)
SELECT * EXCLUDE (__drain_row),
       drain_params(template, COLUMNS(lambda c: c = message_column)::VARCHAR, extra_delimiters := extra_delimiters) AS params
FROM __drain_out
ORDER BY __drain_row
"#;

fn register_parse(extension: &Extension) -> Result<()> {
    let sig = SignatureBuilder::new()
        .param(FunctionParameter::new::<String>("tbl"))
        .param(FunctionParameter::new::<String>("message_column"))
        .param(FunctionParameter::with_default("order_by", Option::<String>::None))
        .param(FunctionParameter::with_default("sim_threshold", 0.4_f64))
        .param(FunctionParameter::with_default("depth", 4_i64))
        .param(FunctionParameter::with_default("max_children", 100_i64))
        .param(FunctionParameter::with_default("max_clusters", 0_i64))
        .param(FunctionParameter::with_default(
            "extra_delimiters",
            duckdb_neo::types::List::<String>(Vec::new()),
        ))
        .param(
            FunctionParameter::with_type(
                "masks",
                duckdb_neo::logical_type::LogicalType::from_text(extension, config::MASKS_TYPE)?,
            )
            .default_value(duckdb_neo::types::List::<config::MaskArg>(Vec::new())),
        )
        .param(FunctionParameter::with_default("param_str", "<*>".to_string()))
        .param(FunctionParameter::with_default("parametrize_numeric_tokens", true))
        .documentation(
            FunctionDocumentationBuilder::new()
                .description(
                    "Parse a table or view of log lines with Drain: every row plus cluster_id, template \
                     (the cluster's final template) and params. Rows are trained one by one in order_by \
                     order (default: scan order), single-threaded, exactly like logpai/Drain3.",
                )
                .add_example("Parse a table", "FROM drain_parse('logs', 'message', order_by := 'ts')")
                .add_category(CATEGORY),
        );
    MacroFunctionBuilder::new(
        "drain_parse",
        sig,
        MacroBody::Table(DRAIN_PARSE_SQL.to_string()),
    )
    .register(extension)
}

/// Register everything the extension provides.
pub fn register(extension: &Extension) -> Result<()> {
    train::register(extension)?;
    scalar::register(extension)?;
    register_parse(extension)?;
    Ok(())
}
