use crate::lib::*;

#[derive(Debug, Clone)]
pub struct TargetUrl {
    pub scheme: http::uri::Scheme,
    pub authority: http::uri::Authority,
    pub path_and_query: http::uri::PathAndQuery,
}

impl TargetUrl {
    pub fn apply(&self, binding: &http::Uri) -> Result<http::Uri, anyhow::Error> {
        let mut builder = http::uri::Builder::from(binding.clone())
            .scheme(self.scheme.clone())
            .authority(self.authority.clone());

        let binding_path = binding.path_and_query().and_then(|pq| {
            if pq.as_str() != "/" {
                Some(pq.as_str())
            } else {
                None
            }
        });
        let target_path =
            (self.path_and_query.as_str() != "/").then_some(self.path_and_query.as_str());

        if let Some(target_path) = target_path {
            if let Some(binding_path) = binding_path {
                return Err(anyhow::anyhow!(
                    "Can't override binding connection path ({}) with proxy target path ({})",
                    binding_path,
                    target_path
                ));
            }
            builder = builder.path_and_query(target_path);
        }
        Ok(builder.build()?)
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
            path_and_query: url
                .path_and_query()
                .cloned()
                .ok_or(anyhow::anyhow!("No path and query in target url"))?,
        })
    }
}
