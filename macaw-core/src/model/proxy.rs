use crate::lib::*;
use arrayvec::ArrayString;

#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy, Serialize, Deserialize)]
pub struct ProxyId(ArrayString<64>);

impl ProxyId {
    pub fn new(name: &str) -> Result<Self, anyhow::Error> {
        Ok(Self(ArrayString::from_str(name)?))
    }
}

impl fmt::Display for ProxyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}

impl FromStr for ProxyId {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}
