//! Tags for conditional reads: a client sends back the tag it holds and gets
//! "not modified" while the value under that tag has not moved.

/// A value and the tag it was taken under: an unchanged tag means an unchanged value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tagged<T> {
    pub etag: u64,
    pub value: T,
}

impl<T> Tagged<T> {
    /// Whether a client that sent `if_none_match` already holds this value.
    pub fn unchanged_since(&self, if_none_match: u64) -> bool {
        unchanged(if_none_match, self.etag)
    }
}

/// Whether a client that sent `if_none_match` already holds the value tagged `etag`.
/// Zero is what a client sends when it holds nothing.
pub fn unchanged(if_none_match: u64, etag: u64) -> bool {
    if_none_match != 0 && if_none_match == etag
}

/// One run's half of every tag it hands out, so a restarted agent never
/// repeats a tag a client still holds. Never zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Epoch(u32);

impl Epoch {
    pub fn new() -> Self {
        loop {
            match getrandom::u32() {
                Ok(0) => continue,
                Ok(epoch) => return Self(epoch),
                Err(_) => {
                    let nanos = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |d| d.subsec_nanos());
                    return Self(nanos | 1);
                }
            }
        }
    }

    /// The tag of the `generation`th version of a value; never zero.
    pub fn tag(self, generation: u32) -> u64 {
        (self.0 as u64) << 32 | generation as u64
    }
}

impl Default for Epoch {
    fn default() -> Self {
        Self::new()
    }
}

/// A value that gets a new tag whenever it moves and keeps its tag while it does not.
#[derive(Clone, Debug)]
pub struct Versioned<T> {
    epoch: Epoch,
    generation: u32,
    current: Tagged<T>,
}

impl<T> Versioned<T> {
    pub fn new(epoch: Epoch, value: T) -> Self {
        Self {
            epoch,
            generation: 0,
            current: Tagged {
                etag: epoch.tag(0),
                value,
            },
        }
    }

    pub fn get(&self) -> &Tagged<T> {
        &self.current
    }

    /// Takes `value` under a new tag, whether or not it differs.
    pub fn replace(&mut self, value: T) {
        self.generation = self.generation.wrapping_add(1);
        self.current = Tagged {
            etag: self.epoch.tag(self.generation),
            value,
        };
    }

    /// Takes `value` under a new tag if it differs; false when it was already held.
    pub fn set(&mut self, value: T) -> bool
    where
        T: PartialEq,
    {
        if self.current.value == value {
            return false;
        }
        self.replace(value);
        true
    }
}

impl<T: Default> Default for Versioned<T> {
    fn default() -> Self {
        Self::new(Epoch::new(), T::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tag_is_never_zero() {
        assert_ne!(Epoch::new().tag(0), 0);
    }

    #[test]
    fn two_runs_start_from_different_tags() {
        assert_ne!(Epoch::new().tag(0), Epoch::new().tag(0));
    }

    #[test]
    fn a_client_holding_nothing_is_always_answered() {
        let tagged = Tagged { etag: 7, value: () };
        assert!(!tagged.unchanged_since(0));
        assert!(!tagged.unchanged_since(6));
        assert!(tagged.unchanged_since(7));
    }

    #[test]
    fn the_same_value_again_keeps_its_tag() {
        let mut versioned = Versioned::new(Epoch::new(), 1);
        let first = versioned.get().etag;
        assert!(!versioned.set(1));
        assert_eq!(versioned.get().etag, first);
    }

    #[test]
    fn a_different_value_moves_the_tag() {
        let mut versioned = Versioned::new(Epoch::new(), 1);
        let first = versioned.get().etag;
        assert!(versioned.set(2));
        assert_ne!(versioned.get().etag, first);
        assert_eq!(versioned.get().value, 2);
    }

    #[test]
    fn a_replace_moves_the_tag_even_for_the_same_value() {
        let mut versioned = Versioned::new(Epoch::new(), 1);
        let first = versioned.get().etag;
        versioned.replace(1);
        assert_ne!(versioned.get().etag, first);
    }
}
