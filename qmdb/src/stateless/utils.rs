pub fn sn_to_level0nth(sn: u64) -> u64 {
    let (high, low) = (sn / 2048, sn % 2048);
    high * 2048 * 2 + low
}

// activebits leaf nth
pub fn level0nth_to_level8nth(level0nth: u64) -> u64 {
    let twig_id = level0nth / (2 * 2048);
    8 * 2 * twig_id + 8 + ((level0nth / 256) % 8)
}

// activebits leaf nth
pub fn sn_to_level8nth(sn: u64) -> u64 {
    let level0nth = sn_to_level0nth(sn);
    level0nth_to_level8nth(level0nth)
}
