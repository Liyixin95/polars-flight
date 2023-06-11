use std::error::Error;

type BoxError = Box<dyn Error + Send + Sync>;

mod data_frame_reader;
pub use data_frame_reader::DataFrameReader;

mod data_frame_iterator;
