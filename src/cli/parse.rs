use std::{fmt, vec};

use crate::cli::frame::Frame;

#[derive(Debug)]
pub(crate) struct Parse {
    parts: vec::IntoIter<Frame>,
}

/// Error encountered while parsing a frame.
///
/// Only `EndOfStream` errors are handled at runtime. All other errors result in
/// the connection being terminated.
#[derive(Debug)]
pub(crate) enum ParseError {
    /// Attempting to extract a value failed due to the frame being fully
    /// consumed.
    EndOfStream,

    /// All other errors
    Other(crate::cli::Error),
}

impl Parse {
    pub(crate) fn new(frame: Frame) -> Result<Parse, ParseError> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("protocol error; expected array got {:?}", frame).into()),
        };
        Ok(Parse {
            parts: array.into_iter(),
        })
    }
    /// Return the next entry. Array frames are arrays of frames, so the next
    /// entry is a frame.
    fn next(&mut self) -> Result<Frame, ParseError> {
        self.parts.next().ok_or(ParseError::EndOfStream)
    }
    /// Return the next entry as a string.
    ///
    /// If the next entry cannot be represented as a String, then an error is returned.
    pub(crate) fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            Frame::Str(s) => Ok(s),
            frame => Err(format!("protocol error; expected simple frame , got {:?}", frame).into()),
        }
    }
    /// Return the next entry as an integer.
    ///
    /// This includes `Simple`, `Bulk`, and `Integer` frame types. `Simple` and
    /// `Bulk` frame types are parsed.
    ///
    /// If the next entry cannot be represented as an integer, then an error is
    /// returned.
    pub(crate) fn next_int(&mut self) -> Result<u64, ParseError> {
        use atoi::atoi;

        const MSG: &str = "protocol error; invalid number";

        match self.next()? {
            // An integer frame type is already stored as an integer.
            Frame::Integer(v) => Ok(v),
            // Simple and bulk frames must be parsed as integers. If the parsing
            // fails, an error is returned.
            Frame::Str(data) => atoi::<u64>(data.as_bytes()).ok_or_else(|| MSG.into()),
            frame => Err(format!("protocol error; expected int frame but got {:?}", frame).into()),
        }
    }
}

impl From<String> for ParseError {
    fn from(src: String) -> ParseError {
        ParseError::Other(src.into())
    }
}

impl From<&str> for ParseError {
    fn from(src: &str) -> ParseError {
        src.to_string().into()
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EndOfStream => "protocol error; unexpected end of stream".fmt(f),
            ParseError::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for ParseError {}
