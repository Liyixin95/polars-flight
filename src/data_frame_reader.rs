use crate::BoxError;
use arrow2::datatypes::Field;
use arrow2::io::flight::deserialize_message;
use arrow2::io::ipc::read::{deserialize_schema, Dictionaries};
use arrow2::io::ipc::IpcSchema;
use arrow_format::flight::data::{FlightData, FlightDescriptor};
use futures_core::Stream;
use futures_util::StreamExt;
use polars::datatypes::DataType;
use polars::prelude::DataFrame;
use polars::series::Series;
use std::error::Error;

pub struct DataFrameReader<S> {
    flight_strema: S,
    dit: Dictionaries,
    fields: Vec<Field>,
    ipc_schema: IpcSchema,
}

impl<S, E> DataFrameReader<S>
where
    E: Send + Sync + Error + 'static,
    S: Stream<Item = Result<FlightData, E>> + Unpin,
{
    pub async fn try_new(mut s: S) -> Result<(Self, Option<FlightDescriptor>), BoxError> {
        let first = s
            .next()
            .await
            .ok_or_else(|| BoxError::from("failed to read schema message"))?
            .map_err(BoxError::from)?;

        let (schema, ipc_schema) =
            deserialize_schema(first.data_header.as_slice()).map_err(BoxError::from)?;

        Ok((
            Self {
                flight_strema: s,
                dit: Default::default(),
                fields: schema.fields,
                ipc_schema,
            },
            first.flight_descriptor,
        ))
    }

    pub async fn read_all(mut self) -> Result<DataFrame, BoxError> {
        let first = self
            .flight_strema
            .next()
            .await
            .ok_or_else(|| BoxError::from("failed to read schema message"))?
            .map_err(BoxError::from)?;

        let (schema, ipc_schema) =
            deserialize_schema(first.data_header.as_slice()).map_err(BoxError::from)?;

        let mut columns: Vec<_> = schema
            .fields
            .iter()
            .map(|field| Series::new_empty(&field.name, &DataType::from(field.data_type())))
            .map(|mut series| {
                if let (_, Some(hint)) = self.flight_strema.size_hint() {
                    // Safety: 这里只尝试预分配空间，不涉及类型相关操作
                    unsafe {
                        let _ = series.chunks_mut().try_reserve_exact(hint);
                    }
                }
                series
            })
            .collect();

        while let Some(data) = self.flight_strema.next().await {
            let data = data.map_err(BoxError::from)?;

            let chunk = deserialize_message(&data, &schema.fields, &ipc_schema, &mut self.dit)
                .map_err(BoxError::from)?;

            if let Some(chunk) = chunk {
                chunk
                    .into_arrays()
                    .into_iter()
                    .zip(&mut columns)
                    .for_each(|(array, series)| {
                        debug_assert!(series.dtype().eq(array.data_type()));
                        // Safety: deserialize_message 确保了chunk中的类型是按schema排列的
                        unsafe {
                            series.chunks_mut().push(array);
                        }
                    });
            };
        }

        Ok(DataFrame::from_iter(columns))
    }
}
