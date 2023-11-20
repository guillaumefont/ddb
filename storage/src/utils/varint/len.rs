use std::ops::ShrAssign;

use num_traits::Unsigned;

pub fn len_varint<N: Unsigned + ShrAssign<i32>>(value: N) -> usize {
    let mut length = 0;
    let mut value = value;
    loop {
        length += 1;
        value >>= 7;
        if value.is_zero() {
            break;
        }
    }
    length
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_length_test() {
        assert_eq!(len_varint(150u32), 2);
        assert_eq!(len_varint(150u64), 2);
        assert_eq!(len_varint(150usize), 2);
    }
}
