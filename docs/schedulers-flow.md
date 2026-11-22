# Function App Schedulers - Flow

Timer triggers are defined in `function_app.py`. Cron times are UTC (NCRONTAB, 6 fields).
In `TEST_MODE` each job runs once at app start (`run_on_startup`). Whether the recurring cron stays depends on `RUN_ON_MARKET_HOURS` (see below).

## Schedule (UTC vs IST)

| Job | UTC | IST | Duration (hh:mm) | Frequency | When it stops |
|---|---|---|---|---|---|
| `daily_job` | 03:00-03:40 | 08:30-09:10 | 00:40 | Every 10 min (5 fires/day) | Each run stops at the 480s time budget and the next fire resumes; once all symbols are done, later fires exit immediately |
| `option_chain_fetcher` (timer window) | 03:00-10:59 | 08:30-16:29 | 07:59 | Every minute (fires at seconds 0..N-1, one symbol batch per second) | Each fire ends after its batch |
| `option_chain_fetcher` (actually collects) | 03:45-10:30 | 09:15-16:00 | 06:45 | Same as above | Skips outside market hours and holidays |
| `daily_market_backup` | 11:30 | 17:00 | 00:00 (single fire) | Once per day | Ends after backup and container delete; reruns the same day only skip already-finished steps |

All jobs run Monday to Friday. `daily_job` also stops once the market closes at 16:00 IST (10:30 UTC).

In pure test mode (`TEST_MODE=true`, `RUN_ON_MARKET_HOURS=false`) every job runs once when the app starts and does not repeat (the backup runs once, a minute after the test window ends).

## TEST_MODE and RUN_ON_MARKET_HOURS

`RUN_ON_MARKET_HOURS` defaults to `true` when the env var is missing (release branches). Set it to `false` on master/private branches for pure test behaviour.

| TEST_MODE | RUN_ON_MARKET_HOURS | Behaviour |
|---|---|---|
| false | any | Normal prod schedule. |
| true | `true` (default) | Normal Mon-Fri crons stay active and each job also runs once at app start. On a trading day (not weekend/holiday) every run behaves like prod (live container, no test window). On a weekend/holiday it behaves as test: startup run only inside the `TEST_MODE_MINUTES` window, scheduled fires skip, data goes to the test container, and a one-shot `test_mode_backup` runs after the window. |
| true | `false` | Pure test mode: every job runs once at app start, test container, no recurring schedule. |

## FORCE_BACKUP_ID (one-off live backup)

With `TEST_MODE=false`, setting `FORCE_BACKUP_ID=<value>` makes the next app start run one live backup on any day, including weekends and holidays. The ID is recorded in `scheduler-state` (`force_backup_<id>`), so later restarts with the same value do nothing. Use a new value to force another run. A failed run is retried on later restarts, up to 3 attempts.

- The container is kept unless `FORCE_BACKUP_DELETE=true` (and `DELETE_AFTER_BACKUP=true`).
- Ignored when `TEST_MODE=true`.
- Set it after 16:00 IST so the day's data is complete.

## Telegram upload parts

The backup ZIP is sent in parts of at most `TELEGRAM_PART_BYTES` (default `5MB`; accepts `512KB`, `8MB` or plain bytes, 1024-based; always capped at Telegram's 49 MB limit), with a 4 s pause between parts to stay under Telegram's group rate limit. Small parts survive dropped connections that big uploads do not. Each part's caption explains how to join the parts. If any part fails, the whole ZIP is saved to `backup-failed`.

After every successful backup a final status message is sent in the same format as the part captions (`Backup:`, `Source:`, `Trigger:`, `Date:`, `Blobs | ZIP`) ending with `Container-Deleted: true|false`. It is `false` when the container is kept (for example a forced run without `FORCE_BACKUP_DELETE=true`, `DELETE_AFTER_BACKUP=false`, or a failed deletion). If Telegram failed, the message also says the ZIP is only in `backup-failed`.

## Overview

```mermaid
flowchart LR
    subgraph Triggers["function_app.py timer triggers"]
        T1["daily_job<br/>cron: 0 0,10,20,30,40 3 * * 1-5<br/>UTC 03:00-03:40 | IST 08:30-09:10<br/>Duration 00:40<br/>Mon-Fri, EVERY 10 MIN<br/>stops when all symbols done or budget used<br/>(TEST_MODE: once at start)"]
        T2["option_chain_fetcher<br/>cron: 0-N * 3-10 * * 1-5<br/>UTC 03:00-10:59 | IST 08:30-16:29<br/>Duration 07:59<br/>Mon-Fri, EVERY MINUTE<br/>(collects only 09:15-16:00 IST = 03:45-10:30 UTC)<br/>(TEST_MODE: once at start)"]
        T3["daily_market_backup<br/>cron: 0 30 11 * * 1-5<br/>UTC 11:30 | IST 17:00<br/>Duration 00:00 (single fire)<br/>Mon-Fri, ONCE PER DAY<br/>(TEST_MODE: once after window ends)"]
    end

    T1 --> DS_RUN["DailyScheduler.run()"]
    T2 --> LS_RUN["LiveScheduler.run()"]
    T3 --> DS_BK["DailyScheduler.run_backup()"]

    DS_RUN --> HIST[("History candles<br/>ParquetCacheManager")]
    DS_RUN --> CHAIN0[("Baseline option chain<br/>OptionChainCacheManager")]
    LS_RUN --> CHAIN[("Live option chain<br/>OptionChainCacheManager")]
    DS_BK --> TG["MarketDataBackupService<br/>zip -> Telegram (TelegramService)"]
    TG -. "Telegram failed" .-> FAILED[("backup-failed container<br/>fallback ZIP")]
    DS_RUN -. progress .-> PRG[("DailyProgress<br/>per-day record")]
    DS_BK -. progress .-> PRG
```

## daily_job -> DailyScheduler.run()

```mermaid
flowchart TD
    A(["Timer fires, Mon-Fri, EVERY 10 MIN<br/>UTC 03:00-03:40 | IST 08:30-09:10, duration 00:40<br/>(TEST_MODE: once at app start)"]) --> B{"Market closed?<br/>now > 16:00 IST (10:30 UTC)"}
    B -- yes --> Z([Return True])
    B -- no --> C{TEST_MODE and<br/>window ended?}
    C -- yes --> Z
    C -- no --> D{Not TEST_MODE and<br/>holiday?}
    D -- yes --> D1[Cleanup old broker-master folders] --> Z
    D -- no --> F[Load watchlist and option-chain symbols]
    F --> G[Build context / pick cache root]
    G --> H[Load DailyProgress<br/>list pending tasks]
    H --> I{Anything pending?}
    I -- no --> Z
    I -- yes --> J[Set deadline = time budget]
    J --> K[Next history:symbol task]
    K --> L{Out of time?<br/>closed / test window / budget}
    L -- yes --> P([Paused: return False<br/>next 10-min run resumes])
    L -- no --> M[Fetch missing history<br/>per broker and date range]
    M --> N[Record success/failure in progress]
    N --> O{More history symbols?}
    O -- yes --> K
    O -- no --> Q[Next chain:symbol task]
    Q --> R{Out of time?}
    R -- yes --> P
    R -- no --> S[Fetch expiries + baseline option chain]
    S --> T[Record progress]
    T --> U{More chain symbols?}
    U -- yes --> Q
    U -- no --> V([Daily Job Completed: return True])
```

## option_chain_fetcher -> LiveScheduler.run()

```mermaid
flowchart TD
    A(["Timer fires EVERY MINUTE, Mon-Fri, seconds 0..N-1<br/>UTC 03:00-10:59 | IST 08:30-16:29, duration 07:59<br/>(TEST_MODE: once at app start)"]) --> B{TEST_MODE?}
    B -- yes --> C[target_seconds = all batches]
    B -- no --> D[target_seconds = current second]
    D --> E{past_due and same minute<br/>and last second < now?}
    E -- yes --> F[Catch up missed seconds]
    E -- no --> G
    F --> G[For each second]
    C --> G
    G --> H[batch = symbols<br/>sec*BATCH : sec*BATCH+BATCH]
    H --> I{Batch empty?}
    I -- yes --> G
    I -- no --> J[LiveScheduler.run]
    J --> K{"Market open 09:15-16:00 IST<br/>(03:45-10:30 UTC), not holiday,<br/>and TEST window active?"}
    K -- no --> X([Skip])
    K -- yes --> L[asyncio.gather over symbols<br/>semaphore max_concurrency]
    L --> M[Resolve instrument]
    M --> N[For each preferred broker<br/>fyers / dhan]
    N --> O[Fetch option chain range<br/>in thread]
    O --> P[(Write to option-chain cache)]
    P --> Q{Error?}
    Q -- yes --> R[Log, continue with others]
    Q -- no --> S([Done])
    R --> S
```

## daily_market_backup -> DailyScheduler.run_backup()

```mermaid
flowchart TD
    A(["Timer fires ONCE PER DAY, Mon-Fri<br/>UTC 11:30 | IST 17:00, single fire 00:00<br/>(TEST_MODE: once after window ends)"]) --> B{Not TEST_MODE and holiday?}
    B -- yes --> Z([Return True])
    B -- no --> C{TEST_MODE?}
    C -- yes --> D{Window initialized<br/>and ended?}
    D -- no --> F([Return False])
    D -- yes --> E[Container = test cache]
    C -- no --> E2[Container = market data cache]
    E --> G
    E2 --> G[Load progress in scheduler-state container]
    G --> H{Backup task already finished?}
    H -- yes --> I{Backup succeeded?}
    I -- yes --> J[Delete container if not yet deleted]
    I -- no --> F
    J --> Z2([Return success])
    H -- no --> K[Mark backup attempt]
    K --> L[MarketDataBackupService.run]
    L --> M{Result}
    M -- "Telegram OK" --> N[Record done]
    M -- "Telegram failed,<br/>saved to backup-failed" --> N2[Log ERROR<br/>Record done + fallback:container note]
    M -- "Nothing to back up,<br/>zip failed or fallback save failed" --> P[Log error, skip deletion<br/>retry limited by MAX_ATTEMPTS] --> F
    N --> O[Delete backed-up container<br/>if DELETE_AFTER_BACKUP]
    N2 --> O
    O --> Z3([Return True])
```

### MarketDataBackupService.run()

```mermaid
flowchart TD
    A([run]) --> B[For each container:<br/>market-data-cache, broker-master]
    B --> C{Container empty?}
    C -- yes --> C1[Warn and skip]
    C -- no --> D[Build ZIP<br/>up to 3 attempts, 5s backoff]
    D --> E{ZIP built?}
    E -- no --> E1[/"Telegram alert: zip failed<br/>(exception type only)"/] --> X1([Raise: run fails])
    E -- yes --> F[Send to Telegram<br/>split if over size limit, 3 retries per part]
    F --> G{Sent?}
    G -- yes --> OK([Success])
    G -- no --> H[/"Telegram alert: send failed<br/>(partial parts: N of M, must not be used)"/]
    H --> I[Upload whole ZIP to backup-failed<br/>date/container.zip, overwrite, 3 attempts]
    I --> J{Saved?}
    J -- yes --> K[/"Telegram alert: ONLY copy in backup-failed,<br/>download then delete manually"/] --> FB([Success with fallback_blob])
    J -- no --> L[/"Telegram alert: backup-failed upload failed"/] --> X2([Raise: data kept, run fails])
    C1 --> M{All containers empty?}
    M -- yes --> X3([Raise NothingToBackUpError])
    M -- no --> OK
```

Notes:
- Alerts are best effort; if Telegram is fully down the alert cannot be delivered either, so check the ERROR log.
- `backup-failed` is never pruned automatically.
- Source containers are deleted only after success or a saved fallback.
