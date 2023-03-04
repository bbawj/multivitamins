use std::io::{Cursor, self};

use tokio::{net::TcpStream, io::{BufWriter, AsyncReadExt, AsyncWriteExt}};
use bytes::{Buf, BytesMut};
use crate::cli::frame::Frame;

#[derive(Debug)]
pub struct Connection<'a> {
    stream: BufWriter<&'a mut TcpStream>,
    buffer: BytesMut,
}

// An abstraction over the TcpStream that is used to send and receive messages.
impl Connection<'_> {

    pub fn new(stream: &mut TcpStream) -> Connection {
        let stream = BufWriter::new(stream);
        let buffer = BytesMut::with_capacity(4096);
        Connection {
            stream,
            buffer
        }
    }

    pub async fn read_frame(&mut self)
        -> crate::cli::Result<Option<Frame>>
        {
            loop {
                // Attempt to parse a frame from the buffered data. If enough data
                // has been buffered, the frame is returned.
                if let Some(frame) = self.parse_frame()? {
                    return Ok(Some(frame));
                }

                // There is not enough buffered data to read a frame. Attempt to
                // read more data from the socket.
                //
                // On success, the number of bytes is returned. `0` indicates "end
                // of stream".
                if 0 == self.stream.read_buf(&mut self.buffer).await? {
                    // The remote closed the connection. For this to be a clean
                    // shutdown, there should be no data in the read buffer. If
                    // there is, this means that the peer closed the socket while
                    // sending a frame.
                    if self.buffer.is_empty() {
                        return Ok(None);
                    } else {
                        return Err("connection reset by peer".into());
                    }
                }
            }
        }
    fn parse_frame(&mut self) -> crate::cli::Result<Option<Frame>> {
        use crate::cli::frame::Error::Incomplete;

        // Cursor is used to track the "current" location in the
        // buffer. Cursor also implements `Buf` from the `bytes` crate
        // which provides a number of helpful utilities for working
        // with bytes.
        let mut buf = Cursor::new(&self.buffer[..]);

        // The first step is to check if enough data has been buffered to parse
        // a single frame. This step is usually much faster than doing a full
        // parse of the frame, and allows us to skip allocating data structures
        // to hold the frame data unless we know the full frame has been
        // received.
        match Frame::check(&mut buf) {
            Ok(_) => {
                // The `check` function will have advanced the cursor until the
                // end of the frame. Since the cursor had position set to zero
                // before `Frame::check` was called, we obtain the length of the
                // frame by checking the cursor position.
                let len = buf.position() as usize;

                // Reset the position to zero before passing the cursor to
                // `Frame::parse`.
                buf.set_position(0);

                // Parse the frame from the buffer. This allocates the necessary
                // structures to represent the frame and returns the frame
                // value.
                //
                // If the encoded frame representation is invalid, an error is
                // returned. This should terminate the **current** connection
                // but should not impact any other connected client.
                let frame = Frame::parse(&mut buf)?;

                // Discard the parsed data from the read buffer.
                //
                // When `advance` is called on the read buffer, all of the data
                // up to `len` is discarded. The details of how this works is
                // left to `BytesMut`. This is often done by moving an internal
                // cursor, but it may be done by reallocating and copying data.
                self.buffer.advance(len);

                // Return the parsed frame to the caller.
                Ok(Some(frame))
            }
            // There is not enough data present in the read buffer to parse a
            // single frame. We must wait for more data to be received from the
            // socket. Reading from the socket will be done in the statement
            // after this `match`.
            //
            // We do not want to return `Err` from here as this "error" is an
            // expected runtime condition.
            Err(Incomplete) => Ok(None),
            // An error was encountered while parsing the frame. The connection
            // is now in an invalid state. Returning `Err` from here will result
            // in the connection being closed.
            Err(e) => Err(e.into()),
        }    
    }
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Array(val) => {
                // Encode the frame type prefix. For an array, it is `*`.
                self.stream.write_u8(b'*').await?;
                // Encode the length of the array.
                self.write_decimal(val.len() as u64).await?;
                // Iterate and encode each entry in the array.
                for entry in &**val {
                    self.write_val(entry).await?;
                }

            }
            _ => self.write_val(frame).await?,
        }
        self.stream.flush().await
    }

    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;
        // Convert the value to a string
        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;
        Ok(())
    }

    pub async fn write_val(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Str(val) => {
                // encode strings as +
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                // encode ints as -
                self.stream.write_u8(b'-').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Error(val) => {
                // encode errors as -
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            // we are writing bytes based on an array, there cant be arrays to write
            Frame::Array(_) => unreachable!()
        }
        Ok(())
    }

}
