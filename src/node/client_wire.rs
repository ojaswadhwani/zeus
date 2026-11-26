pub(crate) const MAX_KEY: usize = 1024;
pub(crate) const MAX_VALUE: usize = 16 * 1024;
pub(crate) const MAX_BATCH_OPERATIONS: usize = 128;
pub(crate) const MAX_BATCH_BYTES: usize = 64 * 1024;
pub(crate) const MAX_SCAN_ROWS: usize = 128;
pub(crate) const MAX_SCAN_BYTES: usize = 96 * 1024;

pub(crate) fn push_field(out: &mut Vec<u8>, field: &[u8]) {
    out.extend_from_slice(&u16::try_from(field.len()).unwrap().to_be_bytes());
    out.extend_from_slice(field);
}

pub(crate) fn take_field<'a>(input: &mut &'a [u8]) -> Option<&'a [u8]> {
    let (length, rest) = input.split_first_chunk::<2>()?;
    let (field, rest) = rest.split_at_checked(u16::from_be_bytes(*length).into())?;
    *input = rest;
    Some(field)
}
