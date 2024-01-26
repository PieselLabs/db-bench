use arrow::array::{AsArray, PrimitiveArray};
use arrow::buffer::{Buffer, MutableBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Int32Type, SchemaBuilder};
use arrow::record_batch::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::Arc;

mod utils;

// select "1", "2", "5" from table where "1" > 12 and "6" < 4;

#[inline]
fn fusion_by_row(record_batch: &RecordBatch) -> anyhow::Result<RecordBatch> {
    let mut buffer1 = MutableBuffer::from_len_zeroed(record_batch.num_rows() * 4);
    let mut buffer2 = MutableBuffer::from_len_zeroed(record_batch.num_rows() * 4);
    let mut buffer5 = MutableBuffer::from_len_zeroed(record_batch.num_rows() * 4);

    let buffer1_data = buffer1.typed_data_mut::<i32>();
    let buffer2_data = buffer2.typed_data_mut::<i32>();
    let buffer5_data = buffer5.typed_data_mut::<i32>();

    let column1 = record_batch
        .column_by_name("1")
        .unwrap()
        .as_primitive::<Int32Type>()
        .values();
    let column2 = record_batch
        .column_by_name("2")
        .unwrap()
        .as_primitive::<Int32Type>()
        .values();
    let column5 = record_batch
        .column_by_name("5")
        .unwrap()
        .as_primitive::<Int32Type>()
        .values();
    let column6 = record_batch
        .column_by_name("6")
        .unwrap()
        .as_primitive::<Int32Type>()
        .values();

    for i in 0..record_batch.num_rows() {
        if column1[i] > 12 && column6[i] < 4 {}
    }

    let mut sel_index = 0;

    for i in 0..record_batch.num_rows() {
        if column1[i] > 12 && column6[i] < 4 {
            buffer1_data[sel_index] = column1[i];
            buffer2_data[sel_index] = column2[i];
            buffer5_data[sel_index] = column5[i];
            sel_index += 1;
        }
    }

    let scalar_buffer1: ScalarBuffer<i32> = ScalarBuffer::new(Buffer::from(buffer1), 0, sel_index);
    let scalar_buffer2: ScalarBuffer<i32> = ScalarBuffer::new(Buffer::from(buffer2), 0, sel_index);
    let scalar_buffer5: ScalarBuffer<i32> = ScalarBuffer::new(Buffer::from(buffer5), 0, sel_index);

    let result_column1: PrimitiveArray<Int32Type> = PrimitiveArray::new(scalar_buffer1, None);
    let result_column2: PrimitiveArray<Int32Type> = PrimitiveArray::new(scalar_buffer2, None);
    let result_column5: PrimitiveArray<Int32Type> = PrimitiveArray::new(scalar_buffer5, None);

    let mut schema_builder = SchemaBuilder::new();
    schema_builder.push(Field::new("1", DataType::Int32, false));
    schema_builder.push(Field::new("2", DataType::Int32, false));
    schema_builder.push(Field::new("5", DataType::Int32, false));
    let schema = schema_builder.finish();

    let result_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(result_column1),
            Arc::new(result_column2),
            Arc::new(result_column5),
        ],
    )?;

    Ok(result_batch)
}

pub fn fusion_by_row_bench(c: &mut Criterion) {
    let count_rows = 1_000_000;

    let record_batch = utils::generate_record_batch(
        &["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
        count_rows,
    )
    .unwrap();

    c.bench_function("fusion_by_row_bench", move |b| {
        b.iter(|| fusion_by_row(black_box(&record_batch)))
    });
}

criterion_group!(filter_project1, fusion_by_row_bench);
criterion_main!(filter_project1);
