use crate::models::Event;
use std::collections::HashSet;
use std::env;

pub(crate) fn supported_dex(module: &str) -> Option<&'static str> {
    match module {
        "kaddex.exchange" => Some("mercatus"),
        "kdlaunch.kdswap-exchange" => Some("kdswap"),
        _ => None,
    }
}

/// Filters events to only keep those relevant for the wallet application
pub struct EventFilter {
    allowed_event_types: HashSet<String>,
    excluded_modules: HashSet<String>,
}

impl EventFilter {
    /// Creates a filter configured for wallet use cases
    /// Keeps transfers, the events needed to reconstruct cross-chain transfers, and supported
    /// DEX swap events.
    /// Reads excluded modules from EXCLUDED_MODULES env var (comma-separated list)
    pub fn for_wallet() -> Self {
        let excluded_modules = env::var("EXCLUDED_MODULES")
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().to_string())
            .collect::<HashSet<String>>();

        if !excluded_modules.is_empty() {
            log::info!("Excluding events from modules: {:?}", excluded_modules);
        }

        Self {
            allowed_event_types: HashSet::from([
                "TRANSFER".to_string(),
                "TRANSFER_XCHAIN".to_string(),
                "TRANSFER_XCHAIN_RECD".to_string(),
                "X_RESUME".to_string(),
                "SWAP".to_string(),
                // Future event types can be added here
            ]),
            excluded_modules,
        }
    }

    /// Creates a filter that allows all events (no filtering)
    #[allow(dead_code)]
    pub fn allow_all() -> Self {
        Self {
            allowed_event_types: HashSet::new(),
            excluded_modules: HashSet::new(),
        }
    }

    /// Check if an event should be kept based on filter configuration
    pub fn should_keep(&self, event: &Event) -> bool {
        // First check if module is excluded
        if self.excluded_modules.contains(&event.module) {
            return false;
        }

        // If no allowed types configured, allow everything (that's not excluded)
        if self.allowed_event_types.is_empty() {
            return true;
        }

        if !self.allowed_event_types.contains(&event.name) {
            return false;
        }

        // SWAP is not a standard event name, so additionally trust
        // only contracts whose event schemas we understand.
        if event.name == "SWAP" {
            return supported_dex(&event.module).is_some();
        }

        true
    }

    /// Filter a list of events, keeping only allowed ones
    pub fn filter_events(&self, events: Vec<Event>) -> Vec<Event> {
        events.into_iter().filter(|e| self.should_keep(e)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(name: &str) -> Event {
        Event {
            block: "test-block".to_string(),
            chain_id: 0,
            height: 100,
            idx: 0,
            module: "coin".to_string(),
            module_hash: "test-hash".to_string(),
            name: name.to_string(),
            params: serde_json::json!([]),
            param_text: "".to_string(),
            qual_name: format!("coin.{}", name),
            request_key: "test-key".to_string(),
            pact_id: None,
        }
    }

    #[test]
    fn test_wallet_filter_keeps_transfers() {
        let filter = EventFilter::for_wallet();
        let transfer_event = make_event("TRANSFER");
        assert!(filter.should_keep(&transfer_event));
    }

    #[test]
    fn test_wallet_filter_keeps_cross_chain_transfers() {
        let filter = EventFilter::for_wallet();
        assert!(filter.should_keep(&make_event("TRANSFER_XCHAIN")));
        assert!(filter.should_keep(&make_event("TRANSFER_XCHAIN_RECD")));
    }

    #[test]
    fn test_wallet_filter_keeps_x_resume() {
        let filter = EventFilter::for_wallet();
        let resume = Event {
            module: "pact".to_string(),
            qual_name: "pact.X_RESUME".to_string(),
            ..make_event("X_RESUME")
        };
        assert!(filter.should_keep(&resume));
    }

    #[test]
    fn test_wallet_filter_keeps_supported_dex_swaps_only() {
        let filter = EventFilter::for_wallet();
        let kaddex_swap = Event {
            module: "kaddex.exchange".to_string(),
            qual_name: "kaddex.exchange.SWAP".to_string(),
            ..make_event("SWAP")
        };
        let unrelated_swap = Event {
            module: "free.some-dex".to_string(),
            qual_name: "free.some-dex.SWAP".to_string(),
            ..make_event("SWAP")
        };
        let kdswap = Event {
            module: "kdlaunch.kdswap-exchange".to_string(),
            qual_name: "kdlaunch.kdswap-exchange.SWAP".to_string(),
            ..make_event("SWAP")
        };

        assert!(filter.should_keep(&kaddex_swap));
        assert!(filter.should_keep(&kdswap));
        assert!(!filter.should_keep(&unrelated_swap));
    }

    #[test]
    fn test_swap_must_be_enabled_as_an_allowed_event_type() {
        let filter = EventFilter {
            allowed_event_types: HashSet::from(["TRANSFER".to_string()]),
            excluded_modules: HashSet::new(),
        };
        let kaddex_swap = Event {
            module: "kaddex.exchange".to_string(),
            qual_name: "kaddex.exchange.SWAP".to_string(),
            ..make_event("SWAP")
        };

        assert!(!filter.should_keep(&kaddex_swap));
    }

    #[test]
    fn test_wallet_filter_excludes_other_events() {
        let filter = EventFilter::for_wallet();
        let other_event = make_event("SOME_OTHER_EVENT");
        assert!(!filter.should_keep(&other_event));
    }

    #[test]
    fn test_allow_all_filter() {
        let filter = EventFilter::allow_all();
        let transfer_event = make_event("TRANSFER");
        let other_event = make_event("SOME_OTHER_EVENT");
        assert!(filter.should_keep(&transfer_event));
        assert!(filter.should_keep(&other_event));
    }

    #[test]
    fn test_filter_events_list() {
        let filter = EventFilter::for_wallet();
        let events = vec![
            make_event("TRANSFER"),
            make_event("OTHER"),
            make_event("TRANSFER"),
            make_event("RANDOM"),
        ];
        let filtered = filter.filter_events(events);
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|e| e.name == "TRANSFER"));
    }

    #[test]
    fn test_module_exclusion() {
        let filter = EventFilter {
            allowed_event_types: HashSet::from(["TRANSFER".to_string()]),
            excluded_modules: HashSet::from(["free.crankk01".to_string()]),
        };

        let allowed_event = Event {
            module: "coin".to_string(),
            ..make_event("TRANSFER")
        };
        let excluded_event = Event {
            module: "free.crankk01".to_string(),
            ..make_event("TRANSFER")
        };

        assert!(filter.should_keep(&allowed_event));
        assert!(!filter.should_keep(&excluded_event));
    }

    #[test]
    fn test_module_exclusion_takes_precedence() {
        let filter = EventFilter {
            allowed_event_types: HashSet::from(["TRANSFER".to_string()]),
            excluded_modules: HashSet::from(["coin".to_string()]),
        };

        let excluded_transfer = Event {
            module: "coin".to_string(),
            ..make_event("TRANSFER")
        };

        // Even though TRANSFER is allowed, coin module is excluded
        assert!(!filter.should_keep(&excluded_transfer));
    }
}
