use crate::utils::{bitvec::BitVec, murmur3::Murmur3Hasher};

#[derive(Debug)]
pub struct SstFilter {
    pub bitvec: BitVec,
    num_functions: u32,
}

impl SstFilter {
    pub fn new(item_count: usize, miss_rate: f64) -> Self {
        let filter_size = (((item_count as f64) * miss_rate.ln())
            / (1.0f64 / 2.0f64.powf(2.0f64.ln())).ln())
        .ceil() as usize;
        let num_functions = ((filter_size as f64 / item_count as f64) * 2.0f64.ln()).round() as u32;
        Self {
            bitvec: BitVec::new(filter_size),
            num_functions,
        }
    }

    pub fn from_data(data: &[u8], num_functions: u32) -> SstFilter {
        let bitvec = BitVec::from_data(data);
        Self {
            bitvec,
            num_functions,
        }
    }

    pub fn add(&mut self, key: &[u8]) {
        for func_i in 0..self.num_functions {
            let mut hasher = Murmur3Hasher::new(func_i);
            hasher.update(key);
            let index = hasher.finalize();
            let index = index as usize % self.bitvec.len;
            self.bitvec.set(index);
        }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        for func_i in 0..self.num_functions {
            let mut hasher = Murmur3Hasher::new(func_i);
            hasher.update(key);
            let index = hasher.finalize();
            let index = index as usize % self.bitvec.len;
            if !self.bitvec.get(index) {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use crate::sst::filter::SstFilter;

    #[test]
    fn filter_size() {
        let filter = SstFilter::new(4000, 0.01);
        assert_eq!(filter.bitvec.len, 38344); // 38341 rounded to next byte
        assert_eq!(filter.num_functions, 7);
    }

    #[test]
    fn test_filter() {
        let mut filter = SstFilter::new(100, 0.01);
        for i in 0..100 {
            let key = format!("foo{}", i);
            filter.add(key.as_bytes());
        }

        for i in 0..100 {
            let key = format!("foo{}", i);
            assert_eq!(filter.may_contain(key.as_bytes()), true);
        }
    }
}
