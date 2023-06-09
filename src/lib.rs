use std::error::Error;

type BoxError = Box<dyn Error + Send + 'static>;

mod data_frame_stream;