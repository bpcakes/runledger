macro_rules! define_identifier {
    ($identifier:ident, $blank_error:ident) => {
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            serde::Serialize,
            serde::Deserialize,
        )]
        #[serde(transparent)]
        pub struct $identifier<'a>(&'a str);

        impl<'a> $identifier<'a> {
            #[must_use]
            pub const fn new(value: &'a str) -> Self {
                Self(value)
            }

            pub fn try_new(value: &'a str) -> Result<Self, IdentifierValidationError> {
                validate_identifier(value, IdentifierValidationError::$blank_error)?;
                Ok(Self::new(value))
            }

            #[must_use]
            pub const fn as_str(&self) -> &'a str {
                self.0
            }
        }

        impl PartialEq<&str> for $identifier<'_> {
            fn eq(&self, other: &&str) -> bool {
                self.0 == *other
            }
        }

        impl PartialEq<$identifier<'_>> for &str {
            fn eq(&self, other: &$identifier<'_>) -> bool {
                *self == other.0
            }
        }

        impl PartialEq<String> for $identifier<'_> {
            fn eq(&self, other: &String) -> bool {
                self.0 == other.as_str()
            }
        }

        impl PartialEq<$identifier<'_>> for String {
            fn eq(&self, other: &$identifier<'_>) -> bool {
                self.as_str() == other.0
            }
        }

        impl std::borrow::Borrow<str> for $identifier<'_> {
            fn borrow(&self) -> &str {
                self.0
            }
        }

        impl fmt::Display for $identifier<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.0)
            }
        }

        impl<'a> TryFrom<&'a str> for $identifier<'a> {
            type Error = IdentifierValidationError;

            fn try_from(value: &'a str) -> Result<Self, Self::Error> {
                Self::try_new(value)
            }
        }
    };
}

macro_rules! define_owned_identifier {
    ($owned_identifier:ident, $borrowed_identifier:ident, $blank_error:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
        #[serde(transparent)]
        pub struct $owned_identifier(String);

        impl $owned_identifier {
            pub fn new(value: impl AsRef<str>) -> Result<Self, IdentifierValidationError> {
                let value = value.as_ref();
                validate_identifier(value, IdentifierValidationError::$blank_error)?;
                Ok(Self(value.to_owned()))
            }

            #[must_use]
            pub fn from_static(value: &'static str) -> Self {
                match Self::new(value) {
                    Ok(identifier) => identifier,
                    Err(_) => panic!("static identifier cannot be blank"),
                }
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }

            #[must_use]
            pub fn into_inner(self) -> String {
                self.0
            }

            #[must_use]
            pub fn as_borrowed(&self) -> $borrowed_identifier<'_> {
                $borrowed_identifier::new(self.as_str())
            }
        }

        impl std::borrow::Borrow<str> for $owned_identifier {
            fn borrow(&self) -> &str {
                self.as_str()
            }
        }

        impl AsRef<str> for $owned_identifier {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $owned_identifier {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl TryFrom<&str> for $owned_identifier {
            type Error = IdentifierValidationError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $owned_identifier {
            type Error = IdentifierValidationError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl std::str::FromStr for $owned_identifier {
            type Err = IdentifierValidationError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl<'de> serde::Deserialize<'de> for $owned_identifier {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = <String as serde::Deserialize>::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }

        impl TryFrom<$borrowed_identifier<'_>> for $owned_identifier {
            type Error = IdentifierValidationError;

            fn try_from(value: $borrowed_identifier<'_>) -> Result<Self, Self::Error> {
                Self::new(value.as_str())
            }
        }

        impl From<$owned_identifier> for String {
            fn from(value: $owned_identifier) -> Self {
                value.into_inner()
            }
        }

        impl PartialEq<&str> for $owned_identifier {
            fn eq(&self, other: &&str) -> bool {
                self.as_str() == *other
            }
        }

        impl PartialEq<$owned_identifier> for &str {
            fn eq(&self, other: &$owned_identifier) -> bool {
                *self == other.as_str()
            }
        }

        impl PartialEq<String> for $owned_identifier {
            fn eq(&self, other: &String) -> bool {
                self.as_str() == other.as_str()
            }
        }

        impl PartialEq<$owned_identifier> for String {
            fn eq(&self, other: &$owned_identifier) -> bool {
                self.as_str() == other.as_str()
            }
        }

        impl PartialEq<$borrowed_identifier<'_>> for $owned_identifier {
            fn eq(&self, other: &$borrowed_identifier<'_>) -> bool {
                self.as_str() == other.as_str()
            }
        }

        impl PartialEq<$owned_identifier> for $borrowed_identifier<'_> {
            fn eq(&self, other: &$owned_identifier) -> bool {
                self.as_str() == other.as_str()
            }
        }
    };
}

pub(crate) use define_identifier;
pub(crate) use define_owned_identifier;
