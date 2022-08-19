#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Date(pub i32);

impl Date {
    pub fn new(val: i32) -> Self {
        Self(val)
    }
}
