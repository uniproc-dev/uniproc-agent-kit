# uniproc-agent-kit

Building blocks the uniproc agents share, whatever the OS underneath:
[uniproc-windows-agent](https://github.com/uniproc-dev/uniproc-windows-agent) (ETW, SCM) and
[uniproc-linux-agent](https://github.com/uniproc-dev/uniproc-linux-agent) (eBPF, systemd).
Nothing here knows about an OS, a schema or a transport.

| Module | What it gives an agent |
|---|---|
| `tag` | `Tagged<T>` and `Epoch` for conditional reads (`ifNoneMatch` → not modified), `Versioned<T>` that moves its tag only when the value moves. |
| `latest` | `Latest<T>`: the value a collector published last, as `Tagged<Arc<T>>`, or why its last attempt failed; cheap to clone for readers. |
| `monitor` | `Monitor`: ticks a `Collector` on a thread of its own. Every tick returns when the next one is due, so the collector owns its schedule; a `Waker` ticks it sooner, but no sooner than the spacing after the last tick. `monitor::channel` makes the waker before the monitor, so the collector is built whole with it. The first tick is over before `start` returns; a tick that panics later stops the monitor and shows as `failure()`. The thread is a state machine with its transition table in the module docs. |
| `notify` | `Notify`: a generation counter any executor's futures can wait on, for long polls that answer when the next report is in. |
| `runner` | `Runner<K>`: worker threads that run one job at a time per target; a second job for a busy target gets `Busy` at once, a panicking job cancels its answer and frees its target. |
| `follow` | `Following` / `Board`: watches that stream a key's status and holds that keep a key followed for a while, and the board the thread that learns the statuses keeps. The agent brings the source (SCM notifications, systemd over D-Bus, ...). |

## Using it

```toml
uniproc-agent-kit = { git = "https://github.com/uniproc-dev/uniproc-agent-kit", tag = "v0.2.0" }
```

A monitor over any collector:

```rust
use std::time::{Duration, Instant};
use uniproc_agent_kit::{Collector, Monitor, Why, monitor};

struct Core {
    probe: Probe,
    latest: Latest<Report>,
}

impl Collector for Core {
    fn tick(&mut self, _why: Why) -> Instant {
        self.latest.store(self.probe.report());
        self.probe.next_due()
    }
}

let (waker, wakes) = monitor::channel();
let probe = Probe::start(move || waker.wake())?;
let monitor = Monitor::start("core", Duration::from_millis(50), wakes, Core { probe, latest })?;
```

The collector moves to the monitor's thread, so it is `Send`; a wake sent before the monitor
starts is not lost.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
