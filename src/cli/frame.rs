use std::{fmt, io::Cursor, num::TryFromIntError, string::FromUtf8Error};

use bytes::Buf;

// a frame for our message
#[derive(Clone, Debug)]
pub enum Frame {
    Str(String),
    Error(String),
    Integer(u64),
    // Bulk(Bytes),
    // Null,
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum Error {
    /// Not enough data is available to parse a message
    Incomplete,

    /// Invalid message encoding
    Other(crate::cli::Error),
}

// An abstraction over the data that is sent over the network.
impl Frame {
    /// Returns an empty array
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }
    /// Pushes an integer into the array
    pub(crate) fn push_int(&mut self, value: u64) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(value));
            }
            _ => panic!("not an array frame"),
        }
    }
    /// Pushes a string into the array
    pub(crate) fn push_string(&mut self, value: &str) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Str(value.to_string()));
            }
            _ => panic!("not an array frame"),
        }
    }
    /// Check if message can be decoded
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            b'-' => {
                get_int(src)?;
                Ok(())
            }
            b'*' => {
                let len = get_int(src)?;
                for _ in 0..len {
                    Frame::check(src)?;
                }
                Ok(())
            }
            actual => Err(format!("protocol error; invalid frame type byte {}", actual).into()),
        }
    }

    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            // this is a string
            b'+' => {
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::Str(string))
            }
            // this is an int
            b'-' => {
                let line = get_int(src)?;
                Ok(Frame::Integer(line))
            }
            // this is a frame
            b'*' => {
                let len = get_int(src)?.try_into()?;
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }

                Ok(Frame::Array(out))
            }
            _ => unimplemented!(),
        }
    }
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u8())
}

fn get_line<'a>(src: &'a mut Cursor<&[u8]>) -> Result<&'a [u8], Error> {
    let start = src.position() as usize;
    let mut end = src.get_ref().len();
    if end == 0 {
        panic!("no line")
    } else {
        end -= 1;
    }

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(Error::Incomplete)
}

fn get_int(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    use atoi::atoi;

    let str_src = get_line(src)?;
    atoi(str_src).ok_or_else(|| "protocol invalid frame format".into())
}

impl std::error::Error for Error {}

impl From<&str> for Error {
    fn from(value: &str) -> Error {
        value.to_string().into()
    }
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_src: TryFromIntError) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}
