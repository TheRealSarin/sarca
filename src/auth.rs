use sha2::{Digest, Sha256};

use crate::config::{AuthConfig, Grant};

#[derive(Debug, Clone)]
pub struct Principal {
    pub name: String,
    caps: Vec<String>,
    register: Vec<String>,
    send: Vec<String>,
    groups: Vec<String>,
}

impl Principal {
    fn new(name: String, grant: &Grant) -> Self {
        Self {
            name,
            caps: grant.caps.clone(),
            register: grant.register.clone(),
            send: grant.send.clone(),
            groups: grant.groups.clone(),
        }
    }

    pub fn has_cap(&self, cap: &str) -> bool {
        self.caps.iter().any(|pattern| glob_match(pattern, cap))
    }

    pub fn may_register(&self, target: &str) -> bool {
        self.register
            .iter()
            .any(|pattern| glob_match(pattern, target))
    }

    pub fn may_address(&self, target: &str) -> bool {
        self.send.iter().any(|pattern| glob_match(pattern, target))
    }

    pub fn may_join(&self, group: &str) -> bool {
        self.groups.iter().any(|pattern| glob_match(pattern, group))
    }
}

pub struct Authenticator {
    anonymous: Option<Grant>,
    tokens: Vec<([u8; 32], String, Grant)>,
}

impl Authenticator {
    pub fn new(config: &AuthConfig) -> Self {
        let tokens = config
            .tokens
            .iter()
            .filter_map(|token| {
                let mut digest = [0u8; 32];
                hex::decode_to_slice(&token.sha256, &mut digest).ok()?;
                Some((digest, token.name.clone(), token.grant.clone()))
            })
            .collect();
        Self {
            anonymous: config.anonymous.clone(),
            tokens,
        }
    }

    pub fn authenticate(&self, token: Option<&str>) -> Option<Principal> {
        match token {
            None => self
                .anonymous
                .as_ref()
                .map(|grant| Principal::new("anonymous".into(), grant)),
            Some(token) => {
                let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
                let mut found = None;
                for (expected, name, grant) in &self.tokens {
                    if constant_time_eq(expected, &digest) && found.is_none() {
                        found = Some(Principal::new(name.clone(), grant));
                    }
                }
                found
            }
        }
    }
}

pub fn hash_token(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

fn constant_time_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern = pattern.as_bytes();
    let text = text.as_bytes();
    let (mut p, mut t) = (0, 0);
    let mut backtrack: Option<(usize, usize)> = None;
    while t < text.len() {
        if p < pattern.len() && pattern[p] == b'*' {
            backtrack = Some((p, t));
            p += 1;
        } else if p < pattern.len() && pattern[p] == text[t] {
            p += 1;
            t += 1;
        } else if let Some((star_p, star_t)) = backtrack {
            p = star_p + 1;
            t = star_t + 1;
            backtrack = Some((star_p, star_t + 1));
        } else {
            return false;
        }
    }
    pattern[p..].iter().all(|&b| b == b'*')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TokenConfig;

    #[test]
    fn globs_match() {
        assert!(glob_match("game-*", "game-1"));
        assert!(glob_match("*-eu", "game-eu"));
        assert!(glob_match("a*b*c", "aXXbYYc"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "exactly"));
        assert!(!glob_match("game-*", "server-1"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("group:*", "group:games"));
        assert!(glob_match("**", "group:games"));
    }

    fn auth() -> Authenticator {
        Authenticator::new(&AuthConfig {
            anonymous: Some(Grant {
                caps: vec![],
                register: vec![],
                send: vec!["public-*".into()],
                groups: vec![],
            }),
            tokens: vec![TokenConfig {
                name: "ops".into(),
                sha256: hash_token("s3cret"),
                grant: Grant {
                    caps: vec!["game.*".into()],
                    register: vec!["game-*".into()],
                    send: vec!["*".into()],
                    groups: vec!["games".into()],
                },
            }],
        })
    }

    #[test]
    fn tokens_authenticate_and_grant() {
        let auth = auth();
        let ops = auth.authenticate(Some("s3cret")).unwrap();
        assert_eq!(ops.name, "ops");
        assert!(ops.has_cap("game.admin"));
        assert!(!ops.has_cap("billing"));
        assert!(ops.may_register("game-1"));
        assert!(!ops.may_register("db"));
        assert!(ops.may_address("group:games"));
        assert!(ops.may_join("games"));
        assert!(!ops.may_join("production"));
        assert!(auth.authenticate(Some("wrong")).is_none());
    }

    #[test]
    fn anonymous_uses_its_own_grant() {
        let anon = auth().authenticate(None).unwrap();
        assert!(anon.may_address("public-status"));
        assert!(!anon.may_address("game-1"));
        assert!(!anon.may_register("public-x"));
        assert!(!anon.may_join("games"));
        let closed = Authenticator::new(&AuthConfig::default());
        assert!(closed.authenticate(None).is_none());
    }
}
