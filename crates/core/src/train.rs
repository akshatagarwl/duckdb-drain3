//! Training aggregates: `drain_train` (model BLOB) and `drain_train_ids` (model plus per-row ids).
//!
//! Drain is order-dependent: the same lines in another order can give other clusters.
//! Write `drain_train(msg ORDER BY ts)`; with an `ORDER BY` DuckDB feeds the rows of a
//! group to one state in that order, and the model equals logpai/Drain3 trained on the
//! same sequence. Without it, threads train partial models that `combine` merges
//! approximately (see [`crate::model::merge`]).

use drain::Drain;
use duckdb_neo::{
    AsDuckDB, DuckDBType, Result,
    aggregate::{AggregateCallbacks, AggregateFunctionBuilder, ReturnTypeHandle, States},
    bind_arguments::BindArguments,
    connection::Context,
    data_chunk::VectorCollection,
    enums::AggregateFunctionProperty,
    error::Error,
    extension::Extension,
    logical_type::LogicalType,
    signature::{FunctionDocumentationBuilder, FunctionParameter, SignatureBuilder},
    types::{BlobValue, List},
    vector::{Unknown, Vector},
};

use crate::{CATEGORY, config, model};

fn new_drain(cfg: &drain::Config) -> Drain {
    // The config was validated in bind.
    Drain::new(cfg.clone()).expect("config validated at bind")
}

struct Train;

impl AggregateCallbacks for Train {
    type BindData = drain::Config;
    type StateItem = Option<Drain>;

    fn bind(
        &self,
        _context: &Context,
        arguments: BindArguments<'_>,
        _result_type: ReturnTypeHandle<'_>,
    ) -> Result<Self::BindData> {
        config::from_bind(&arguments)
    }

    fn init(&self, _bind_data: Option<&Self::BindData>) -> Self::StateItem {
        None
    }

    fn update(
        &self,
        bind_data: Option<&Self::BindData>,
        data: &mut VectorCollection,
        mut states: States<'_, Self::StateItem>,
    ) -> Result<()> {
        let cfg = bind_data.ok_or_else(|| Error::internal("drain_train: missing bind data"))?;
        let input = data.get_vector_at::<String>(0)?;
        for (row, value) in input.reader()?.iter().enumerate() {
            if let Some(line) = value {
                states[row]
                    .get_or_insert_with(|| new_drain(cfg))
                    .train(line);
            }
        }
        Ok(())
    }

    fn combine(
        &self,
        _bind_data: Option<&Self::BindData>,
        source: &States<'_, Self::StateItem>,
        mut target: States<'_, Self::StateItem>,
    ) -> Result<()> {
        for i in 0..target.len() {
            let Some(src) = &source[i] else { continue };
            match &mut target[i] {
                slot @ None => *slot = Some(src.clone()),
                Some(dst) => {
                    model::merge(dst, src)?;
                }
            }
        }
        Ok(())
    }

    fn finalize(
        &self,
        _bind_data: Option<&Self::BindData>,
        states: &[&Self::StateItem],
        result: Vector<'_, Unknown>,
        result_offset: usize,
    ) -> Result<()> {
        let mut result = result.cast::<BlobValue>()?;
        let mut writer = result.writer()?;
        for (i, state) in states.iter().enumerate() {
            match state {
                Some(d) => writer.set(result_offset + i, Some(model::encode(d)?.as_slice()))?,
                None => writer.set(result_offset + i, None)?,
            }
        }
        Ok(())
    }
}

/// State of `drain_train_ids`: the miner plus the cluster id of every row, in input order
/// (`0` for a NULL message).
#[derive(Default)]
struct IdsState {
    drain: Option<Drain>,
    ids: Vec<u64>,
}

/// Result of `drain_train_ids`.
#[derive(Debug, AsDuckDB)]
pub struct TrainIds {
    pub model: Option<BlobValue>,
    pub cluster_ids: List<u64>,
    pub templates: List<String>,
    pub sizes: List<u64>,
}

struct TrainIdsAgg;

impl AggregateCallbacks for TrainIdsAgg {
    type BindData = drain::Config;
    type StateItem = IdsState;

    fn bind(
        &self,
        _context: &Context,
        arguments: BindArguments<'_>,
        _result_type: ReturnTypeHandle<'_>,
    ) -> Result<Self::BindData> {
        config::from_bind(&arguments)
    }

    fn init(&self, _bind_data: Option<&Self::BindData>) -> Self::StateItem {
        IdsState::default()
    }

    fn update(
        &self,
        bind_data: Option<&Self::BindData>,
        data: &mut VectorCollection,
        mut states: States<'_, Self::StateItem>,
    ) -> Result<()> {
        let cfg = bind_data.ok_or_else(|| Error::internal("drain_train_ids: missing bind data"))?;
        let input = data.get_vector_at::<String>(0)?;
        for (row, value) in input.reader()?.iter().enumerate() {
            let state = &mut states[row];
            match value {
                Some(line) => {
                    let id = state
                        .drain
                        .get_or_insert_with(|| new_drain(cfg))
                        .train(line)
                        .cluster_id;
                    state.ids.push(id.get());
                }
                None => state.ids.push(0),
            }
        }
        Ok(())
    }

    fn combine(
        &self,
        _bind_data: Option<&Self::BindData>,
        source: &States<'_, Self::StateItem>,
        mut target: States<'_, Self::StateItem>,
    ) -> Result<()> {
        for i in 0..target.len() {
            let src = &source[i];
            let dst = &mut target[i];
            match (&mut dst.drain, &src.drain) {
                (_, None) => dst.ids.extend_from_slice(&src.ids),
                (slot @ None, Some(s)) => {
                    *slot = Some(s.clone());
                    dst.ids.extend_from_slice(&src.ids);
                }
                (Some(d), Some(s)) => {
                    let map = model::merge(d, s)?;
                    dst.ids
                        .extend(src.ids.iter().map(|id| map.get(id).copied().unwrap_or(0)));
                }
            }
        }
        Ok(())
    }

    fn finalize(
        &self,
        _bind_data: Option<&Self::BindData>,
        states: &[&Self::StateItem],
        result: Vector<'_, Unknown>,
        result_offset: usize,
    ) -> Result<()> {
        let mut result = result.cast::<TrainIds>()?;
        // Size the three list children for every row of this batch.
        let (mut n_ids, mut n_clusters) = (0usize, 0usize);
        for s in states {
            n_ids += s.ids.len();
            n_clusters += s.drain.as_ref().map_or(0, |d| max_id(d) as usize);
        }
        {
            let mut children = result.children_mut().ok_or_else(|| {
                Error::internal("drain_train_ids: STRUCT result without children")
            })?;
            let mut ids = children.get_vector_at_mut::<List<u64>>(1)?;
            let base = ids.child_offset();
            // SAFETY: every reserved element is written below (one per id).
            unsafe { ids.set_child_size(base + n_ids)? };
            drop(ids);
            let mut templates = children.get_vector_at_mut::<List<String>>(2)?;
            let base = templates.child_offset();
            // SAFETY: every reserved element is written below (one per cluster id slot).
            unsafe { templates.set_child_size(base + n_clusters)? };
            drop(templates);
            let mut sizes = children.get_vector_at_mut::<List<u64>>(3)?;
            let base = sizes.child_offset();
            // SAFETY: as above.
            unsafe { sizes.set_child_size(base + n_clusters)? };
        }
        let mut writer = result.writer()?;
        for (i, s) in states.iter().enumerate() {
            let row = result_offset + i;
            let Some(d) = &s.drain else {
                if s.ids.is_empty() {
                    writer.set(row, None)?;
                } else {
                    writer.set(
                        row,
                        Some(TrainIds {
                            model: None,
                            cluster_ids: List(vec![None; s.ids.len()]),
                            templates: List(Vec::new()),
                            sizes: List(Vec::new()),
                        }),
                    )?;
                }
                continue;
            };
            let n = max_id(d) as usize;
            let mut templates: Vec<Option<String>> = vec![None; n];
            let mut sizes: Vec<Option<u64>> = vec![None; n];
            for c in d.clusters() {
                let k = c.id().get() as usize - 1;
                templates[k] = Some(c.template().to_string());
                sizes[k] = Some(c.size());
            }
            writer.set(
                row,
                Some(TrainIds {
                    model: Some(BlobValue(model::encode(d)?)),
                    cluster_ids: s
                        .ids
                        .iter()
                        .map(|&id| {
                            (id != 0 && d.cluster(drain::ClusterId::new(id).unwrap()).is_some())
                                .then_some(id)
                        })
                        .collect(),
                    templates: List(templates),
                    sizes: List(sizes),
                }),
            )?;
        }
        Ok(())
    }
}

fn max_id(d: &Drain) -> u64 {
    d.clusters().map(|c| c.id().get()).max().unwrap_or(0)
}

fn signature(
    extension: &Extension,
    ret: LogicalType,
    docs: FunctionDocumentationBuilder,
) -> Result<SignatureBuilder> {
    let sig = SignatureBuilder::new().param(FunctionParameter::new::<String>("message"));
    Ok(config::add_params(sig, extension)?
        .return_type(ret)
        .documentation(docs.add_category(CATEGORY)))
}

pub fn register(extension: &Extension) -> Result<()> {
    AggregateFunctionBuilder::new(
        "drain_train",
        signature(
            extension,
            BlobValue::logical_type(extension)?,
            FunctionDocumentationBuilder::new()
                .description(
                    "Train a Drain log template model on the messages of a group and return it as a BLOB. \
                     Training is order-dependent: write drain_train(msg ORDER BY ts) for a deterministic, \
                     logpai/Drain3-identical model. Named parameters: sim_threshold, depth, max_children, \
                     max_clusters, extra_delimiters, masks, param_str, parametrize_numeric_tokens.",
                )
                .add_example(
                    "Train on a table",
                    "SELECT drain_train(message ORDER BY ts) FROM logs",
                )
                .add_example(
                    "With masking",
                    "SELECT drain_train(message ORDER BY ts, masks := [{'pattern': '\\d+\\.\\d+\\.\\d+\\.\\d+', 'mask': '<IP>'}]) FROM logs",
                ),
        )?,
        Train,
    )
    .set_property(AggregateFunctionProperty::AggregateOrderDependent(true))
    .register(extension)?;

    AggregateFunctionBuilder::new(
        "drain_train_ids",
        signature(
            extension,
            TrainIds::logical_type(extension)?,
            FunctionDocumentationBuilder::new()
                .description(
                    "Like drain_train, but also returns the cluster id each input row was assigned while \
                     training (cluster_ids, in input order), and the final template and size of every \
                     cluster (templates[id], sizes[id]). The building block of drain_parse.",
                )
                .add_example(
                    "Per-row ids",
                    "SELECT drain_train_ids(message ORDER BY ts) FROM logs",
                ),
        )?,
        TrainIdsAgg,
    )
    .set_property(AggregateFunctionProperty::AggregateOrderDependent(true))
    .register(extension)
}
