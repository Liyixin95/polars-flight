use arrow2::datatypes::Schema;
use arrow2::io::flight::{default_ipc_fields, serialize_batch, serialize_schema};
use arrow2::io::ipc::{self, IpcField, IpcSchema};
use arrow_format::flight::data::FlightData;
use polars::prelude::{DataFrame, IpcWriterOption};

pub struct DfIterator {
    df: DataFrame,
    batch_size: usize,
    offset: i64,
    ipc_schema: Vec<IpcSchema>,
    write_opt: IpcWriterOption,
}

pub struct DfBatchIterBuilder {
    df: DataFrame,
    batch_size: usize,
    schema: Schema,
    ipc_fields: Vec<IpcField>,
    write_opt: IpcWriterOption,
}

impl DfBatchIterBuilder {
    pub fn new(df: DataFrame, batch_size: usize) -> Self {
        let schema = df.schema().to_arrow();
        let ipc_field = default_ipc_fields(&schema.fields);
        Self {
            df,
            batch_size,
            schema,
            ipc_fields: ipc_field,
            write_opt: Default::default(),
        }
    }

    pub fn build_iterator(&self) -> impl Iterator<Item = arrow2::error::Result<FlightData>> {
        let schema_msg = serialize_schema(&self.schema, Some(&self.ipc_fields));
        let height = self.df.height();
        std::iter::repeat(())
            .enumerate()
            .scan(self.df.height(), |state, (idx, _)| {
                if *state == 0 {
                    return None;
                }

                let remain = state.checked_sub(idx * self.batch_size);
                match remain {
                    Some(remain) => {
                        *state = remain;
                        Some(idx * self.batch_size)
                    }
                    None => {
                        *state = 0;
                        Some(idx * self.batch_size)
                    }
                }
            });
        std::iter::once(schema_msg).map(|msg| Ok(msg))
    }
}

pub fn data_frame_iterator(
    df: DataFrame,
    batch_size: usize,
    ipc_schema: Option<Vec<IpcField>>,
    write_opt: IpcWriterOption,
) -> impl Iterator<Item = arrow2::error::Result<FlightData>> {
    let ipc_fields = ipc_schema.unwrap_or_else(|| {
        let schema = df.schema().to_arrow();
        default_ipc_fields(&schema.fields)
    });

    let schema_msg = serialize_schema(schema, &ipc_fields);

    todo!()
}

impl Iterator for DfIterator {
    type Item = FlightData;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: 可以考虑发一个batch就free一个（需要unsafe）
        self.df.get_columns().iter().map(|s| {
            let chunks = s.slice(self.offset, self.batch_size).chunks();

            let ret = serialize_batch(chunk, fields, options);
        });
        todo!()
    }
}
