use rand::Rng;
use std::sync::Arc;

use arrow::array::{Int32Array, PrimitiveArray};
use arrow::datatypes::{DataType, Field, Int32Type, SchemaBuilder};
use arrow::record_batch::RecordBatch;

pub fn generate_int32_arrow_array(n: usize) -> PrimitiveArray<Int32Type> {
    let mut builder = Int32Array::builder(n);
    let mut rng = rand::thread_rng();
    for _ in 0..=n {
        builder.append_value(rng.gen::<i32>())
    }

    builder.finish()
}

pub fn generate_record_batch(cols: &[&str], count_rows: usize) -> anyhow::Result<RecordBatch> {
    let mut columns = Vec::new();
    let mut schema_builder = SchemaBuilder::new();
    for c in cols {
        columns.push(Arc::new(generate_int32_arrow_array(count_rows)) as _);
        schema_builder.push(Field::new(c.to_string(), DataType::Int32, false));
    }
    let result_batch = RecordBatch::try_new(Arc::new(schema_builder.finish()), columns)?;

    Ok(result_batch)
}
