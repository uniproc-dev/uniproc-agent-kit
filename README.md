# uniproc-agent-kit

Building blocks the uniproc agents share, whatever the OS underneath:
[uniproc-windows-agent](https://github.com/uniproc-dev/uniproc-windows-agent) (ETW, SCM) and
[uniproc-linux-agent](https://github.com/uniproc-dev/uniproc-linux-agent) (eBPF, systemd).
Nothing here knows about an OS, a schema or a transport.

| Module | What it gives an agent |
|---|---|
| `tag` | `Tagged<T>` and `Epoch` for conditional reads (`ifNoneMatch` → not modified), `Versioned<T>` that moves its tag only when the value moves. |
| `monitor` | `Monitor`: a collector on a thread of its own that reports on every wake-up, at least once a period and no more often than the spacing (`Cadence`, 1 s / 50 ms by default). The first report is in before `start` returns. |
| `runner` | `Runner<K>`: worker threads that run one job at a time per target; a second job for a busy target gets `Busy` at once, a panicking job cancels its answer and frees its target. |
| `follow` | `Following` / `Board`: watches that stream a key's status and holds that keep a key followed for a while, and the board the thread that learns the statuses keeps. The agent brings the source (SCM notifications, systemd over D-Bus, ...). |

## Using it

```toml
uniproc-agent-kit = { git = "https://github.com/uniproc-dev/uniproc-agent-kit", tag = "v0.1.0" }
```

A monitor over any collector:

```rust
use uniproc_agent_kit::{Cadence, Monitor};

let monitor = Monitor::start(
    "core",
    Cadence::default(),
    || {
        let mut collector = Collector::new()?;
        Ok(move || collector.report())
    },
    move |report| latest.store(report),
)?;
```

`std::thread::current()` inside the start closure is the monitor's thread: a collector that
unparks it gets a report within the spacing.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
