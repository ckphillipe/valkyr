use std::collections::{BTreeMap, BTreeSet};

/// A small deterministic matcher for Valkyr route patterns.
///
/// `*` matches any sequence and `{name}` (or the compatible `${name}` spelling)
/// captures a non-empty sequence. Literal text is matched exactly. Captures are
/// useful to database query adapters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pattern {
    source: String,
    tokens: Vec<Token>,
}

pub type Capture = BTreeMap<String, String>;

impl Pattern {
    pub fn new(pattern: impl Into<String>) -> Self {
        let source = pattern.into();
        let tokens = tokens(&source);
        Self { source, tokens }
    }

    pub fn matches(&self, value: &str) -> Option<Capture> {
        match_tokens(&self.tokens, value).map(|captures| captures.into_iter().collect())
    }

    /// Names declared by `{name}` and `${name}` captures, independent of a
    /// concrete match. Database adapters use this for configuration checks.
    pub fn capture_names(&self) -> BTreeSet<String> {
        self.tokens
            .iter()
            .filter_map(|token| match token {
                Token::Capture(name) => Some(name.clone()),
                Token::Literal(_) | Token::Wildcard => None,
            })
            .collect()
    }

    pub(crate) fn source(&self) -> &str {
        &self.source
    }

    pub(crate) fn has_wildcard_or_capture(&self) -> bool {
        self.tokens
            .iter()
            .any(|token| matches!(token, Token::Wildcard | Token::Capture(_)))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Token {
    Literal(String),
    Wildcard,
    Capture(String),
}

fn tokens(pattern: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < pattern.len() {
        let tail = &pattern[index..];
        if tail.starts_with('*') {
            tokens.push(Token::Wildcard);
            index += 1;
            continue;
        }
        let capture_offset = if tail.starts_with("${") {
            2
        } else if tail.starts_with('{') {
            1
        } else {
            0
        };
        if capture_offset != 0 {
            if let Some(end) = tail.find('}') {
                let name = &tail[capture_offset..end];
                if !name.is_empty() {
                    tokens.push(Token::Capture(name.to_owned()));
                    index += end + 1;
                    continue;
                }
            }
        }
        let end = [tail.find('*'), tail.find('{'), tail.find("${")]
            .into_iter()
            .flatten()
            .filter(|end| *end > 0)
            .min()
            .unwrap_or(tail.len());
        tokens.push(Token::Literal(tail[..end].to_owned()));
        index += end;
    }
    tokens
}

fn match_tokens(tokens: &[Token], value: &str) -> Option<Vec<(String, String)>> {
    if tokens.is_empty() {
        return value.is_empty().then(Vec::new);
    }
    match &tokens[0] {
        Token::Literal(literal) => value
            .strip_prefix(literal)
            .and_then(|rest| match_tokens(&tokens[1..], rest)),
        Token::Wildcard => (0..=value.len())
            .rev()
            .filter(|index| value.is_char_boundary(*index))
            .find_map(|index| match_tokens(&tokens[1..], &value[index..])),
        Token::Capture(name) => (1..=value.len())
            .filter(|index| value.is_char_boundary(*index))
            .find_map(|index| {
                match_tokens(&tokens[1..], &value[index..]).map(|mut captures| {
                    captures.insert(0, (name.clone(), value[..index].to_string()));
                    captures
                })
            }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn captures_and_wildcards_match() {
        let captures = Pattern::new("/users/{id}/*")
            .matches("/users/42/profile")
            .unwrap();
        assert_eq!(captures["id"], "42");
        assert!(Pattern::new("/users/*").matches("/groups/42").is_none());
    }

    #[test]
    fn supports_dollar_brace_captures() {
        let captures = Pattern::new("/services/${service}/config")
            .matches("/services/api/config")
            .unwrap();
        assert_eq!(captures["service"], "api");
    }

    #[test]
    fn reports_declared_capture_names() {
        assert_eq!(
            Pattern::new("/services/${service}/{id}").capture_names(),
            BTreeSet::from(["id".into(), "service".into()])
        );
    }
}
