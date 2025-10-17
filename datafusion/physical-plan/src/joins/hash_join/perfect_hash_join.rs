use std::{sync::Arc, task::Poll};

use arrow::{
    array::{
        new_null_array, Array, ArrayRef, Int64Array, Int64Builder, RecordBatch, UInt64Array
    },
    compute::{take, take_record_batch, BatchCoalescer},
};
use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion_common::{
    cast::{as_int64_array, as_uint64_array},
    NullEquality, Result,
};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_expr::JoinType;
use datafusion_physical_expr::PhysicalExprRef;
use futures::{ready, Stream, StreamExt};

use crate::{
    handle_state,
    joins::utils::{BuildProbeJoinMetrics, JoinFilter, StatefulStreamResult},
};

pub(crate) enum PerfectHashJoinStreamState {
    FetchProbeBatch,
    ProcessProbeBatch,
    Completed,
}

struct ProbeBatchState {
    key_values: Int64Array,
    batch: RecordBatch,
    batch_output: BatchCoalescer,
}

impl ProbeBatchState {
    pub fn new(schema: SchemaRef, batch_size: usize) -> Self {
        let mut builder = Int64Builder::new();
        Self {
            key_values: builder.finish(),
            batch: RecordBatch::new_empty(Arc::new(Schema::empty())),
            batch_output: BatchCoalescer::new(schema, batch_size),
        }
    }

    fn key_values(&self) -> &Int64Array {
        &self.key_values
    }
}

pub(crate) struct PerfectHashJoinStream {
    schema: SchemaRef,
    join_type: JoinType,
    build_array: UInt64Array,
    build_batch: RecordBatch,
    on_right: PhysicalExprRef,
    filter: Option<JoinFilter>,
    right: SendableRecordBatchStream,
    state: PerfectHashJoinStreamState,
    join_metrics: BuildProbeJoinMetrics,
    probe_batch_state: ProbeBatchState,
    min: i64,
    max: i64,
}

impl PerfectHashJoinStream {
    ///
    pub fn new(
        schema: SchemaRef,
        join_type: JoinType,
        build_array: UInt64Array,
        build_batch: RecordBatch,
        on_right: PhysicalExprRef,
        filter: Option<JoinFilter>,
        right: SendableRecordBatchStream,
        batch_size: usize,
        state: PerfectHashJoinStreamState,
        join_metrics: BuildProbeJoinMetrics,
        min: i64,
        max: i64,
    ) -> Self {
        Self {
            schema: schema.clone(),
            join_type,
            build_array,
            build_batch,
            on_right,
            filter,
            right,
            state,
            join_metrics,
            probe_batch_state: ProbeBatchState::new(schema, batch_size),
            min,
            max,
        }
    }

    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            return match self.state {
                PerfectHashJoinStreamState::FetchProbeBatch => {
                    handle_state!(ready!(self.fetch_probe_batch(cx)))
                }
                PerfectHashJoinStreamState::ProcessProbeBatch => {
                    handle_state!(ready!(self.process_probe_batch()))
                }
                PerfectHashJoinStreamState::Completed => Poll::Ready(None),
            };
        }
    }

    fn fetch_probe_batch(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        if let Some(batch) = self
            .probe_batch_state
            .batch_output
            .next_completed_batch()
        {
            return Poll::Ready(Ok(StatefulStreamResult::Ready(Some(batch))));
        }

        match ready!(self.right.poll_next_unpin(cx)) {
            None => {
                self.state = PerfectHashJoinStreamState::Completed;
            }
            Some(Ok(batch)) => {
                let array = self
                    .on_right
                    .evaluate(&batch)?
                    .into_array(batch.num_rows())?;


                let key_values = as_int64_array(&array)?;
                // println!("key values length: {}", key_values.len());
                self.probe_batch_state.key_values = key_values.clone();
                self.probe_batch_state.batch = batch;

                self.state = PerfectHashJoinStreamState::ProcessProbeBatch;
            }
            Some(Err(err)) => return Poll::Ready(Err(err)),
        };

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    fn process_probe_batch(
        &mut self,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {

        let key_values = self.probe_batch_state.key_values();

        let mut build_match = Vec::new();
        let mut probe_match = Vec::new();   

        // build
        for (row_index, probe_value) in key_values.iter().enumerate() {
            if let Some(value) = probe_value {
                // println!("val: {} in between: min {} + max? {}", value, self.min, self.max);
                // Just a sanity check of now, filter pushdown should take care of this
                if self.min <= value && value <= self.max {
                    let index = (value - self.min) as usize;

                    if !self.build_array.is_null(index) {
                        // println!("build array length: {} accessing index: {}", self.build_array.len(), index);
                        let build_value = self.build_array.value(index);
                        build_match.push(build_value);
                        probe_match.push(row_index as u64);
                    }
                }
            }
        }

        let build_indices = UInt64Array::from(build_match);
        let probe_indices = UInt64Array::from(probe_match);

        let mut build_cols = take_record_batch(&self.build_batch, &build_indices)?.columns().to_vec();
        let probe_cols = take_record_batch(&self.probe_batch_state.batch, &probe_indices)?.columns().to_vec();
        build_cols.extend(probe_cols);

        let batch = RecordBatch::try_new(Arc::clone(&self.schema), build_cols)?;
        self.probe_batch_state.batch_output.push_batch(batch)?;

        self.probe_batch_state.batch_output.finish_buffered_batch()?;
        self.state = PerfectHashJoinStreamState::FetchProbeBatch;

        if let Some(batch) = self
            .probe_batch_state
            .batch_output
            .next_completed_batch()
        {
            return Poll::Ready(Ok(StatefulStreamResult::Ready(Some(batch))));
        }


        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }
}

impl Stream for PerfectHashJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl RecordBatchStream for PerfectHashJoinStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
