use crate::lib::*;

#[derive(Debug, Clone)]
pub struct TargetUrl {
    pub scheme: http::uri::Scheme,
    pub authority: http::uri::Authority,
}

impl TargetUrl {
    pub fn apply(&self, other: &http::Uri) -> http::Uri {
        http::uri::Builder::from(other.clone())
            .scheme(self.scheme.clone())
            .authority(self.authority.clone())
            .build()
            .expect("Bug: Types are already converted, cannot fail")
    }
}

impl FromStr for TargetUrl {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let uri = http::Uri::from_str(s)?;
        Self::try_from(uri)
    }
}

impl TryFrom<http::Uri> for TargetUrl {
    type Error = anyhow::Error;
    fn try_from(url: http::Uri) -> Result<Self, Self::Error> {
        Ok(Self {
            scheme: url
                .scheme()
                .cloned()
                .ok_or(anyhow::anyhow!("No scheme in target url"))?,
            authority: url
                .authority()
                .cloned()
                .ok_or(anyhow::anyhow!("No authority in target url"))?,
        })
    }
}
