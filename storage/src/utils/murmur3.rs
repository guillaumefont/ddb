pub struct Murmur3Hasher {
    hash: u32,
    buffer: [u8; 4],
    buffer_len: usize,
    data_len: usize,
}

impl Murmur3Hasher {
    pub fn new(seed: u32) -> Self {
        let buffer = [0, 0, 0, 0];
        let buffer_len = 0;
        let data_len = 0;
        Self {
            hash: seed,
            buffer,
            buffer_len,
            data_len,
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        let data_len = data.len();
        self.data_len += data_len;
        let mut data_i = 0;

        if self.buffer_len + data_len < 4 {
            self.buffer[self.buffer_len..self.buffer_len + data_len].copy_from_slice(data);
            return;
        }

        if self.buffer_len > 0 {
            self.buffer[self.buffer_len..4].copy_from_slice(&data[0..4 - self.buffer_len]);
            self._update();
            data_i = 4 - self.buffer_len;
        }

        while data_i + 4 <= data_len {
            self.buffer.copy_from_slice(&data[data_i..data_i + 4]);
            self._update();
            data_i += 4;
        }

        self.buffer_len = data_len - data_i;
        self.buffer = [0, 0, 0, 0];
        self.buffer[0..self.buffer_len].copy_from_slice(&data[data_i..]);
    }

    fn _update(&mut self) {
        let k = u32::from_le_bytes(self.buffer);

        self._rotate(k);

        self.hash = self.hash.rotate_left(13);
        self.hash = self.hash.wrapping_mul(5);
        self.hash = self.hash.wrapping_add(0xe6546b64);
    }

    fn _rotate(&mut self, v: u32) {
        let v = v.wrapping_mul(0xcc9e2d51);
        let v = v.rotate_left(15);
        let v = v.wrapping_mul(0x1b873593);

        self.hash = self.hash ^ v;
    }

    pub fn finalize(mut self) -> u32 {
        if self.buffer_len > 0 {
            let rem = u32::from_le_bytes(self.buffer);
            self._rotate(rem);
        }

        self.hash ^= self.data_len as u32;

        self.hash = self.hash ^ (self.hash >> 16);
        self.hash = self.hash.wrapping_mul(0x85ebca6b);
        self.hash = self.hash ^ (self.hash >> 13);
        self.hash = self.hash.wrapping_mul(0xc2b2ae35);
        self.hash = self.hash ^ (self.hash >> 16);

        self.hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_murmur3() {
        let mut hasher = Murmur3Hasher::new(0);
        hasher.update(b"hello world");
        let hash = hasher.finalize();
        assert_eq!(hash, 1586663183);
    }
}
