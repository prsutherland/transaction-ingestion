use std::io::BufRead;
use std::error::Error;

pub struct CsvU16RowReader<R: BufRead> {
    reader: R,
    current_row: Vec<u8>,
    row_idx: usize,
}

impl<R: BufRead> CsvU16RowReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            current_row: Vec::with_capacity(8 * 1024),
            row_idx: 0,
        }
    }

    /// Reads next CSV record, returns (u16 from first field, full row bytes).
    /// Row bytes exclude trailing '\n' and optional preceding '\r'.
    /// Ok(None) at clean EOF.
    pub fn next(&mut self) -> Result<Option<(u16, &[u8])>, Box<dyn Error>> {
        self.current_row.clear();
        self.row_idx += 1;

        let mut col = 0;

        // State for parsing quotes to determine if newline is end of record
        let mut pending_quote = false;
        let mut in_quotes = false;

        // State for finding the value of the u16 in the first column
        let mut first_digit_found = false;
        let mut last_digit_found = false;
        let mut first_digit = 0;
        let mut last_digit = 0;

        loop {
            // Read until we find a newline or EOF
            let chunk = self.reader.fill_buf()?;
            if chunk.is_empty() {
                break; // EOF mid-record; finalize below
            }

            for (idx, b) in chunk.iter().enumerate() {
                if pending_quote {
                    pending_quote = false;
                    if *b == b'"' {
                        self.current_row.push(*b);
                        continue;
                    } else {
                        in_quotes = false;
                    }
                }

                match *b {
                    b'"' => {
                        if in_quotes {
                            pending_quote = true;
                        } else {
                            in_quotes = true;
                        }
                    }
                    b'\n' if !in_quotes => {
                        self.reader.consume(idx + 1);

                        if self.current_row.last() == Some(&b'\r') {
                            self.current_row.pop();
                        }

                        return if !first_digit_found || !last_digit_found {
                            if self.row_idx == 1 {
                                // Header row, skip it
                                return self.next();
                            } else {
                                Err("bad u16 in col0".into())
                            }
                        } else {
                            let id = parse_u16_ascii(&self.current_row[first_digit..last_digit])
                                .ok_or_else(|| -> Box<dyn Error> { "bad u16 in col0".into() })?;
                            Ok(Some((id, self.current_row.as_slice())))
                        };
                    }
                    b',' if !in_quotes => {
                        col += 1;
                        if col == 2 && first_digit_found && !last_digit_found {
                            last_digit = self.current_row.len();
                            last_digit_found = true;
                        }
                    }
                    _ => {
                        // This will accept rows like:
                        // "deposit","a123","1","1"
                        // It will treat a123 as 123 but we don't care
                        // because it is only used for routing to the correct
                        // worker, which will reparse the whole row and
                        // reject invalid rows.
                        if col == 1 {
                            if !first_digit_found && *b >= b'0' && *b <= b'9' {
                                first_digit = self.current_row.len();
                                first_digit_found = true;
                            } else if first_digit_found
                                && !last_digit_found
                                && *b >= b'0'
                                && *b <= b'9'
                            {
                                last_digit = self.current_row.len();
                                last_digit_found = true;
                            }
                        }
                    }
                }

                self.current_row.push(*b);
            }

            let consumed = chunk.len();
            self.reader.consume(consumed);
        }

        // EOF finalization (no trailing newline)
        if self.current_row.last() == Some(&b'\r') {
            self.current_row.pop();
        }
        if self.current_row.is_empty() {
            Ok(None)
        } else if !first_digit_found || !last_digit_found {
            if self.row_idx == 1 {
                // Header row, skip it, end of file.
                Ok(None)
            } else {
                Err("bad u16 in col0".into())
            }
        } else {
            let id = parse_u16_ascii(&self.current_row[first_digit..last_digit])
                .ok_or_else(|| -> Box<dyn Error> { "bad u16 in col0".into() })?;
            Ok(Some((id, self.current_row.as_slice())))
        }
    }
}

#[inline]
fn parse_u16_ascii(bytes: &[u8]) -> Option<u16> {
    if bytes.is_empty() {
        return None;
    }
    let mut n: u32 = 0;
    for &b in bytes {
        if b < b'0' || b > b'9' {
            return None;
        }
        n = n * 10 + (b - b'0') as u32;
        if n > u16::MAX as u32 {
            return None;
        }
    }
    Some(n as u16)
}
