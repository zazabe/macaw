pub mod http_method_serde {
    use http::Method;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(method: &Method, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(method.as_str())
    }
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Method, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse::<Method>().map_err(serde::de::Error::custom)
    }
}

pub mod http_uri_serde {
    use http::Uri;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(uri: &Uri, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(uri.to_string().as_str())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Uri, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse::<Uri>().map_err(serde::de::Error::custom)
    }
}

pub mod http_version_serde {
    use http::Version;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(version: &Version, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let version_str = match *version {
            http::Version::HTTP_09 => "HTTP/0.9",
            http::Version::HTTP_10 => "HTTP/1.0",
            http::Version::HTTP_11 => "HTTP/1.1",
            http::Version::HTTP_2 => "HTTP/2.0",
            http::Version::HTTP_3 => "HTTP/3.0",
            _ => "UNKNOWN",
        };
        serializer.serialize_str(version_str)
    }
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Version, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "HTTP/0.9" => Ok(Version::HTTP_09),
            "HTTP/1.0" => Ok(Version::HTTP_10),
            "HTTP/1.1" => Ok(Version::HTTP_11),
            "HTTP/2.0" | "HTTP/2" => Ok(Version::HTTP_2),
            "HTTP/3.0" | "HTTP/3" => Ok(Version::HTTP_3),
            _ => Err(serde::de::Error::custom(format!(
                "invalid HTTP version: {}",
                s
            ))),
        }
    }
}

pub mod http_status_serde {
    use http::StatusCode;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(status: &StatusCode, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u16(status.as_u16())
    }
    pub fn deserialize<'de, D>(deserializer: D) -> Result<StatusCode, D::Error>
    where
        D: Deserializer<'de>,
    {
        let code = u16::deserialize(deserializer)?;
        StatusCode::from_u16(code).map_err(serde::de::Error::custom)
    }
}
