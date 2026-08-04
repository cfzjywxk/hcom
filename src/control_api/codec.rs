use super::protocol::{MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES};
use std::io::{Read, Write};
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum FrameError {
    #[error("control frame ended before its declared length")]
    Truncated,
    #[error("control frame is empty")]
    Empty,
    #[error("control frame exceeds the bounded size")]
    Oversize,
    #[error("control frame is not valid UTF-8")]
    InvalidUtf8,
    #[error("control frame contains forbidden control bytes")]
    ForbiddenControl,
    #[error("control socket I/O failed")]
    Io(#[source] std::io::Error),
}

pub(crate) fn read_request_frame(reader: &mut impl Read) -> Result<Vec<u8>, FrameError> {
    read_frame(reader, MAX_REQUEST_BYTES)
}

pub(crate) fn read_response_frame(reader: &mut impl Read) -> Result<Vec<u8>, FrameError> {
    read_frame(reader, MAX_RESPONSE_BYTES)
}

pub(crate) fn write_request_frame(
    writer: &mut impl Write,
    payload: &[u8],
) -> Result<(), FrameError> {
    write_frame(writer, payload, MAX_REQUEST_BYTES)
}

pub(crate) fn write_response_frame(
    writer: &mut impl Write,
    payload: &[u8],
) -> Result<(), FrameError> {
    write_frame(writer, payload, MAX_RESPONSE_BYTES)
}

fn read_frame(reader: &mut impl Read, max_bytes: usize) -> Result<Vec<u8>, FrameError> {
    let mut header = [0u8; 4];
    read_exact_bounded(reader, &mut header)?;
    let length = usize::try_from(u32::from_be_bytes(header)).expect("u32 fits usize");
    if length == 0 {
        return Err(FrameError::Empty);
    }
    if length > max_bytes {
        return Err(FrameError::Oversize);
    }
    let mut payload = vec![0u8; length];
    read_exact_bounded(reader, &mut payload)?;
    validate_frame_text(&payload)?;
    Ok(payload)
}

fn write_frame(
    writer: &mut impl Write,
    payload: &[u8],
    max_bytes: usize,
) -> Result<(), FrameError> {
    if payload.is_empty() {
        return Err(FrameError::Empty);
    }
    if payload.len() > max_bytes || payload.len() > u32::MAX as usize {
        return Err(FrameError::Oversize);
    }
    validate_frame_text(payload)?;
    writer
        .write_all(&(payload.len() as u32).to_be_bytes())
        .map_err(FrameError::Io)?;
    writer.write_all(payload).map_err(FrameError::Io)?;
    writer.flush().map_err(FrameError::Io)
}

fn read_exact_bounded(reader: &mut impl Read, mut buffer: &mut [u8]) -> Result<(), FrameError> {
    while !buffer.is_empty() {
        match reader.read(buffer) {
            Ok(0) => return Err(FrameError::Truncated),
            Ok(count) => {
                let (_, rest) = buffer.split_at_mut(count);
                buffer = rest;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(FrameError::Io(error)),
        }
    }
    Ok(())
}

fn validate_frame_text(payload: &[u8]) -> Result<(), FrameError> {
    let text = std::str::from_utf8(payload).map_err(|_| FrameError::InvalidUtf8)?;
    if text.chars().any(|character| {
        character == '\u{1b}'
            || ('\u{80}'..='\u{9f}').contains(&character)
            || (character.is_control() && !matches!(character, '\t' | '\n' | '\r'))
    }) {
        return Err(FrameError::ForbiddenControl);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_api::protocol::PROTOCOL_VERSION;
    use crate::control_api::{
        ArchitectActionReason, ClarificationPage, ClarificationRecord, ControlResponse,
        ControlResult, MAX_CLARIFICATION_PAGE_RECORDS,
    };
    use std::io::Cursor;

    #[test]
    fn partial_reads_are_supported_but_truncation_is_rejected() {
        struct OneByteReader(Cursor<Vec<u8>>);
        impl Read for OneByteReader {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                let count = buffer.len().min(1);
                self.0.read(&mut buffer[..count])
            }
        }
        let payload = br#"{"request_id":"r"}"#;
        let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
        frame.extend_from_slice(payload);
        assert_eq!(
            read_request_frame(&mut OneByteReader(Cursor::new(frame.clone()))).unwrap(),
            payload
        );
        frame.pop();
        assert!(matches!(
            read_request_frame(&mut Cursor::new(frame)),
            Err(FrameError::Truncated)
        ));
    }

    #[test]
    fn oversize_is_rejected_from_header_without_payload_allocation() {
        let header = ((MAX_REQUEST_BYTES + 1) as u32).to_be_bytes();
        assert!(matches!(
            read_request_frame(&mut Cursor::new(header)),
            Err(FrameError::Oversize)
        ));
    }

    #[test]
    fn ansi_c0_c1_and_invalid_utf8_are_rejected() {
        for payload in [
            b"{\"\x1b\":\"x\"}".as_slice(),
            b"{\"\0\":\"x\"}".as_slice(),
            "{\"x\":\"\u{80}\"}".as_bytes(),
            b"{\"\xff\":\"x\"}".as_slice(),
        ] {
            let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
            frame.extend_from_slice(payload);
            assert!(read_request_frame(&mut Cursor::new(frame)).is_err());
        }
    }

    #[test]
    fn maximum_clarification_page_stays_within_the_control_response_frame() {
        let worst_case_path = format!("/{}", "\\".repeat(4095));
        let records = (1..=u32::from(MAX_CLARIFICATION_PAGE_RECORDS))
            .map(|sequence| ClarificationRecord {
                sequence,
                reason: ArchitectActionReason::Clarification,
                developer_request_path: worst_case_path.clone(),
                architect_clarification_path: worst_case_path.clone(),
                human_decision_confirmed: true,
            })
            .collect();
        let response = ControlResponse {
            protocol_version: PROTOCOL_VERSION,
            request_id: "maximum-clarification-page".into(),
            ok: true,
            result: Some(ControlResult::Clarifications {
                page: ClarificationPage {
                    run_id: "run".into(),
                    session_version: 1,
                    task_ordinal: 0,
                    task_key: "task".into(),
                    total_records: 64,
                    after_sequence: 0,
                    records,
                    next_after_sequence: Some(u32::from(MAX_CLARIFICATION_PAGE_RECORDS)),
                },
            }),
            error: None,
        };
        let payload = serde_json::to_vec(&response).unwrap();
        assert!(payload.len() < MAX_RESPONSE_BYTES);
        let mut frame = Vec::new();
        write_response_frame(&mut frame, &payload).unwrap();
        let decoded = read_response_frame(&mut Cursor::new(frame)).unwrap();
        assert_eq!(decoded, payload);
    }
}
