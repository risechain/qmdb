pub fn try_get_slice(bz: &[u8], start: usize, end: usize) -> Result<&[u8], String> {
    if end > bz.len() {
        return Err(format!("end({}) >= bz.len()", end));
    }
    Ok(&bz[start..end])
}
