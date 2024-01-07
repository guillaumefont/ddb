use crate::sst::table::table::SstTable;

pub struct Level {
    tables: Vec<SstTable>,
}

pub struct Levels {
    levels: Vec<Level>,
}
