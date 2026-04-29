use std::{
    error::Error,
    io::{Error as IoError, ErrorKind},
};

pub(crate) type DynError = Box<dyn Error + Send + Sync + 'static>;

pub(crate) fn invalid_input(message: impl Into<String>) -> IoError {
    IoError::new(ErrorKind::InvalidInput, message.into())
}

pub(crate) fn invalid_data(message: impl Into<String>) -> IoError {
    IoError::new(ErrorKind::InvalidData, message.into())
}
