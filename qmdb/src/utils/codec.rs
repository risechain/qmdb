use byteorder::{ByteOrder, LittleEndian};

pub fn decode_le_i64(v: &Vec<u8>) -> i64 {
    LittleEndian::read_i64(&v[0..8])
}
pub fn decode_le_u64(v: &Vec<u8>) -> u64 {
    LittleEndian::read_u64(&v[0..8])
}
pub fn encode_le_u64(n: u64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}
pub fn encode_le_i64(n: i64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}
