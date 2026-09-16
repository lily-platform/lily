/// Raw header data extracted from HTTP buffer before parsing
#[derive(Debug, Clone)]
pub struct RawHeader {
    pub name: String,
    pub value: String,
    pub line_number: usize,
    pub raw_line: String,
}
