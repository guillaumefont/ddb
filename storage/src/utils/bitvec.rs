pub struct BitVec {
    pub data: Vec<u8>,
    pub len: usize,
}

impl BitVec {
    pub fn new(len: usize) -> Self {
        let data_len = match len % 8 {
            0 => len / 8,
            _ => len / 8 + 1
        };
        let data = vec![0u8; data_len];
        Self {
            data,
            len
        }
    }

    pub fn set(&mut self, index: usize) {
        let byte_index = index / 8;
        let bit_index = index % 8;
        let byte = self.data[byte_index];
        let new_byte =  byte | (1 << bit_index);
        self.data[byte_index] = new_byte;
    }

    pub fn get(&self, index: usize) -> bool {
        let byte_index = index / 8;
        let bit_index = index % 8;
        let byte = self.data[byte_index];
        let mask = 1 << bit_index;
        byte & mask != 0
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn buffer_size() {
        let bitvec = BitVec::new(10);
        assert_eq!(bitvec.data.len(), 2);

        let bitvec = BitVec::new(8);
        assert_eq!(bitvec.data.len(), 1);

        let bitvec = BitVec::new(31);
        assert_eq!(bitvec.data.len(), 4);

        let bitvec = BitVec::new(32);
        assert_eq!(bitvec.data.len(), 4);
    }

    #[test]
    fn set_and_get() {
        let mut bitvec = BitVec::new(12);
        bitvec.set(10);
        assert_eq!(bitvec.get(10), true);
        assert_eq!(bitvec.get(11), false);
    }
}