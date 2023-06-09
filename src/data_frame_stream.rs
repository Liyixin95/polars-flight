use arrow2::{io::{flight::{deserialize_message, deserialize_schemas}, ipc::{IpcSchema, read::{Dictionaries, deserialize_schema}}}, datatypes::Schema};
use arrow_format::{flight::data::FlightData};
use futures_core::Stream;
use pin_project_lite::pin_project;
use polars::prelude::DataFrame;
use core::slice::SlicePattern;
use std::{
    pin::Pin,
    task::{Context, Poll, ready}, error::Error, marker::PhantomData,
};

use crate::BoxError;

#[derive(Default)]
enum States {
    #[default]
    DecodeSchema,
    DecodeMessage(Schema, IpcSchema),
}

pin_project! {
    pub struct DataFrameStream<S, E> {
        #[pin]
        flight_strema: S,
        _marker: PhantomData<E>,
        state: States,
        dit: Dictionaries,
    }    
}


impl<S, E> Stream for DataFrameStream<S, E>
where
    E: Into<BoxError>,
    S: Stream<Item = Result<FlightData, E>>,
{
    type Item = Result<DataFrame, BoxError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            let Some(data) = ready!(this.flight_strema.poll_next(cx)) else {
                return Poll::Ready(None);
            };

            let data = data.map_err(Into::into)?;

            let state = match this.state {
                States::DecodeSchema => {
                    let (schema, ipc_schema) = deserialize_schema(data.data_header.as_slice()).map_err(Into::into)?;
                },
                States::DecodeMessage(_, _) => todo!(),
            };

            *this.state = state;

        }

        deserialize_message(data, fields, ipc_schema, dictionaries);
        todo!()
    }
}
