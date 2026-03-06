use std::error::Error;
use std::io::BufRead;

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

        // State for extracting client id from column 1 in one pass.
        // We intentionally accept leading junk like "a123" and route by 123.
        let mut client_started = false;
        let mut client_finished = false;
        let mut client_overflow = false;
        let mut client_value: u32 = 0;

        loop {
            // Read until we find a newline or EOF
            let chunk = self.reader.fill_buf()?;
            if chunk.is_empty() {
                break; // EOF mid-record; finalize below
            }
            let chunk_len = chunk.len();

            // Fast path: when we're not currently parsing quoted content, handle
            // a contiguous unquoted prefix with fewer branches and bulk copy.
            let mut start_idx = 0usize;
            if !in_quotes && !pending_quote {
                let mut special_idx = None;
                for (idx, b) in chunk.iter().enumerate() {
                    if *b == b'\n' || *b == b'"' {
                        special_idx = Some((idx, *b));
                        break;
                    }
                }

                match special_idx {
                    None => {
                        process_unquoted_prefix(
                            chunk,
                            &mut col,
                            &mut client_started,
                            &mut client_finished,
                            &mut client_overflow,
                            &mut client_value,
                        );
                        self.current_row.extend_from_slice(chunk);
                        self.reader.consume(chunk_len);
                        continue;
                    }
                    Some((idx, b'\n')) => {
                        let prefix = &chunk[..idx];
                        process_unquoted_prefix(
                            prefix,
                            &mut col,
                            &mut client_started,
                            &mut client_finished,
                            &mut client_overflow,
                            &mut client_value,
                        );
                        self.current_row.extend_from_slice(prefix);
                        self.reader.consume(idx + 1);

                        if self.current_row.last() == Some(&b'\r') {
                            self.current_row.pop();
                        }

                        return if !client_started || client_overflow {
                            if self.row_idx == 1 {
                                // Header row, skip it
                                self.next()
                            } else {
                                Err("bad u16 in col0".into())
                            }
                        } else {
                            Ok(Some((client_value as u16, self.current_row.as_slice())))
                        };
                    }
                    Some((idx, b'"')) => {
                        let prefix = &chunk[..idx];
                        process_unquoted_prefix(
                            prefix,
                            &mut col,
                            &mut client_started,
                            &mut client_finished,
                            &mut client_overflow,
                            &mut client_value,
                        );
                        self.current_row.extend_from_slice(prefix);
                        start_idx = idx;
                    }
                    Some(_) => {}
                }
            }

            for (offset, b) in chunk[start_idx..].iter().enumerate() {
                let idx = start_idx + offset;
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

                        return if !client_started || client_overflow {
                            if self.row_idx == 1 {
                                // Header row, skip it
                                return self.next();
                            } else {
                                Err("bad u16 in col0".into())
                            }
                        } else {
                            Ok(Some((client_value as u16, self.current_row.as_slice())))
                        };
                    }
                    b',' if !in_quotes => {
                        col += 1;
                        if col == 2 && client_started && !client_finished {
                            client_finished = true;
                        }
                    }
                    _ => {
                        // This will accept rows like:
                        // "deposit","a123","1","1"
                        // It will treat a123 as 123 but we don't care
                        // because it is only used for routing to the correct
                        // worker, which will reparse the whole row and
                        // reject invalid rows.
                        if col == 1 && !client_finished {
                            if b.is_ascii_digit() {
                                client_started = true;
                                client_value = client_value * 10 + (b - b'0') as u32;
                                if client_value > u16::MAX as u32 {
                                    client_overflow = true;
                                }
                            } else if client_started {
                                client_finished = true;
                            }
                        }
                    }
                }

                self.current_row.push(*b);
            }

            self.reader.consume(chunk_len);
        }

        // EOF finalization (no trailing newline)
        if self.current_row.last() == Some(&b'\r') {
            self.current_row.pop();
        }
        if self.current_row.is_empty() {
            Ok(None)
        } else if !client_started || client_overflow {
            if self.row_idx == 1 {
                // Header row, skip it, end of file.
                Ok(None)
            } else {
                Err("bad u16 in col0".into())
            }
        } else {
            Ok(Some((client_value as u16, self.current_row.as_slice())))
        }
    }
}

#[inline]
fn process_unquoted_prefix(
    bytes: &[u8],
    col: &mut usize,
    client_started: &mut bool,
    client_finished: &mut bool,
    client_overflow: &mut bool,
    client_value: &mut u32,
) {
    for b in bytes {
        if *b == b',' {
            *col += 1;
            if *col == 2 && *client_started && !*client_finished {
                *client_finished = true;
            }
            continue;
        }

        if *col == 1 && !*client_finished {
            if b.is_ascii_digit() {
                *client_started = true;
                *client_value = (*client_value * 10) + (b - b'0') as u32;
                if *client_value > u16::MAX as u32 {
                    *client_overflow = true;
                }
            } else if *client_started {
                *client_finished = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CsvU16RowReader;
    use std::io::Cursor;

    #[test]
    fn parses_full_client_id_for_routing() {
        let input = b"type,client,tx,amount\ndeposit,123,1,1.0\n";
        let mut reader = CsvU16RowReader::new(Cursor::new(input.as_slice()));

        let record = reader
            .next()
            .expect("reader should parse first data row")
            .expect("reader should return one data row");

        assert_eq!(record.0, 123);
        assert_eq!(record.1, b"deposit,123,1,1.0");
    }

    #[test]
    fn keeps_permissive_client_digit_scan_for_routing() {
        let input = b"type,client,tx,amount\ndeposit,a123,1,1.0\n";
        let mut reader = CsvU16RowReader::new(Cursor::new(input.as_slice()));

        let record = reader
            .next()
            .expect("reader should parse first data row")
            .expect("reader should return one data row");

        assert_eq!(record.0, 123);
    }

    #[test]
    fn falls_back_to_quoted_path_when_quote_appears() {
        let input = b"type,client,tx,amount\n\"de,posit\",12,1,1.0\n";
        let mut reader = CsvU16RowReader::new(Cursor::new(input.as_slice()));

        let record = reader
            .next()
            .expect("reader should parse first data row")
            .expect("reader should return one data row");

        assert_eq!(record.0, 12);
        assert_eq!(record.1, b"\"de,posit\",12,1,1.0");
    }
}
