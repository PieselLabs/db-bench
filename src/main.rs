use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

fn main() -> anyhow::Result<()> {
    let file = File::open("example.parquet")?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

    println!("Converted arrow schema is: {}", builder.schema());

    let mut reader = builder.build().unwrap();
    let record_batch = reader.next().unwrap().unwrap();

    println!("Read {} records.", record_batch.num_rows());
    Ok(())
}
