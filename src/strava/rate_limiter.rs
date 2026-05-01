use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct RateLimiter {
    state: Mutex<WindowState>,
}

struct WindowState {
    used: u32,
    limit: u32,
    window_end_secs: u64,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(WindowState {
                used: 0,
                limit: 100,
                window_end_secs: next_window_end(unix_now()),
            }),
        }
    }

    /// Returns None if a slot is available (and claims it), or Some(wait) if the window is exhausted.
    /// The caller is responsible for sleeping on Some.
    pub fn try_acquire(&self) -> Option<Duration> {
        let mut s = self.state.lock().unwrap();
        let now = unix_now();
        if now >= s.window_end_secs {
            s.used = 0;
            s.window_end_secs = next_window_end(now);
        }
        if s.used < s.limit {
            s.used += 1;
            None
        } else {
            Some(Duration::from_secs(s.window_end_secs.saturating_sub(now) + 5))
        }
    }

    /// Syncs from Strava response headers; takes max for used to handle out-of-order concurrent responses.
    pub fn update_from_headers(&self, used: u32, limit: u32) {
        let mut s = self.state.lock().unwrap();
        s.used = s.used.max(used);
        if limit > 0 {
            s.limit = limit;
        }
    }

    /// Called on 429: marks window as exhausted and returns how long the caller should sleep.
    pub fn mark_exhausted(&self) -> Duration {
        let mut s = self.state.lock().unwrap();
        s.used = s.limit;
        let now = unix_now();
        Duration::from_secs(s.window_end_secs.saturating_sub(now) + 5)
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn next_window_end(now: u64) -> u64 {
    let secs_past_boundary = now % 900;
    now + (900 - secs_past_boundary)
}
