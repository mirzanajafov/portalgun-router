# PortalGun Router

Rick's portal gun opens a door to whichever dimension he points it at, and when that dimension is on fire, he points it somewhere else. An API gateway does the same job thousands of times a second, with a fraction of a millisecond to decide. I wanted to build one and find out what that fraction of a millisecond actually buys, measured against the gateways people run in production.

PortalGun is an HTTP reverse proxy in Go. It decides where each request goes from health, latency, rate limits and rollout rules, and changes that decision when something breaks. Next to it there's a small control plane (config versions, canary rollouts, chaos) and a Next.js dashboard.

Everything a gateway promises is a number: it adds little latency, it keeps a client inside its quota across several instances, it moves traffic off a dead upstream before many requests fail, and it catches a bad release without rolling back good ones. I built the project around those four numbers rather than a feature list, and each of them is measured against something real.

It's running at **[portalgun.najafov.dev](https://portalgun.najafov.dev)**. You can kill a dimension, slow one down or break the canary, and watch traffic move. The numbers below are on the Results page too.

![The live dashboard: two gateways spreading traffic over four dimensions, the chaos panel with the last fault's detection time, the shared rate limit, and the canary](docs/dashboard.png)

One thing up front: the "dimensions" are containers on one server with fault injection (Toxiproxy) in front of them. The failures are real TCP failures (refused, reset, blackholed connections, slow answers), and the gateway's reaction to them is real. The geography isn't. Every hop is a Docker bridge, and "regions" differ only by the latency I inject.

## How a request goes through it

1. Match the route from an immutable config snapshot behind an atomic pointer, so a config change never takes a lock on the hot path.
2. Rate limit, with one of four designs (below).
3. Canary split, sticky per client.
4. Pick a dimension with power of two choices and peak EWMA, the way Finagle and Linkerd do it, skipping ejected dimensions and dimensions at their in-flight cap. If everything is at its cap, answer 503 at once instead of queueing.
5. Send the attempt with a per-try timeout through that dimension's own transport.
6. On a retryable failure, retry on a different dimension if the retry budget allows (retries can be at most 20% of requests over 10 s, so a dead pool can't triple the load).
7. Record the outcome for health, the canary and metrics.

The proxy itself is `httputil.ReverseProxy` with my own `Transport`, which picks the dimension per attempt, so retries move to another dimension without ReverseProxy knowing. I didn't rewrite hop-by-hop headers, streaming and upgrades myself: that's the code most likely to have subtle bugs, and the standard library has had years of them fixed.

Health is three mechanisms, because each catches what the others miss: active probes (find a dead dimension that gets no traffic), passive ejection from real traffic (5 failures in a row, or half of the last 50 attempts), and a panic threshold (if more than half the pool looks broken, ignore health and spread over everything, because a shared dependency failing would otherwise eject the whole pool). All of it is local to each gateway instance. Nothing on the request path depends on Redis or the controller.

## Does it work?

All of this was measured on my laptop (i7-1270P, a 16-vCPU WSL2 VM), with a CPU limit on every container and the proxy comparison on separate CPU sets. My own load generator is open-loop and measures latency from the scheduled send time, so a stall can't hide in the tail (the wrk2 correction); I checked it against vegeta and wrk2 first. Medians of three runs or more per cell, always on the charger. A laptop is noisier than a dedicated server, so I only compare numbers from the same session, and I say where the spread is too wide to conclude anything.

### What a proxy in the path costs

A GET with a 1 KiB response from an upstream that answers at once, so the only thing left to measure is the proxy. Every proxy gets one worker or thread on one vCPU and 256 MB, keep-alive to the upstream, and no access log.

| system | added p50 at 1k rps | p99 at 1k rps | CPU per request at 1k rps | highest rate it held | memory at 1k rps |
|---|---|---|---|---|---|
| nginx | +107 µs | 1.34 ms | 114 µs | 15k | 22 MB |
| HAProxy | +108 µs | 1.45 ms | 120 µs | 12.5k | 88 MB |
| Go `httputil.ReverseProxy`, 30 lines | +139 µs | 1.64 ms | 173 µs | 8k | 56 MB |
| PortalGun | +155 µs | 1.74 ms | 189 µs | 6k | 31 MB |
| Envoy | +207 µs | 1.97 ms | 224 µs | 6k | 107 MB |
| Caddy | +201 µs | 2.33 ms | 237 µs | 4k | 86 MB |

"Held" means p99 under 50 ms, under 0.1% errors and at least 95% of the target rate. With no proxy at all the lab itself tops out near 20k, so nginx's real maximum is somewhere above 15k. The rates go up in coarse steps, and that last column turned out to depend on the evening as much as on the proxy (more on that below).

What I take from this:

- My own code costs little next to net/http. PortalGun spends 1.03-1.09x the bare ReverseProxy's CPU per request at every rate up to 8k and adds 16 µs at p50 over it at 1k.
- nginx and HAProxy held roughly twice the rate on one core. At the same 6k they spend about 20% less CPU per request than PortalGun, and their cost keeps falling as load grows. That's roughly what I expected from C event loops with no garbage collector.
- Envoy surprised me. With one worker and its defaults it used more CPU per request than PortalGun at every rate and ran out of memory one step above 6k. That compares defaults, though: none of the proxies here has overload protection turned on, and Envoy in production usually runs with its overload manager.
- Past their maximum, the Go proxies and Envoy all fail the same way. Requests queue in memory until the container reaches 256 MB and starts swapping, and throughput falls to a few hundred rps, or the kernel kills it. nginx and HAProxy never got near the limit. My first sweep caught PortalGun doing this at 8k, and I spent a while suspecting my own code before the bare ReverseProxy did exactly the same at 10k. PortalGun caps requests in flight per dimension, but nothing caps what the gateway holds in total, so it can't turn away work it has no room for. That's the first thing I'd add.

The table says PortalGun held one step less than the bare ReverseProxy, 6k against 8k, on 3-9% more CPU. That didn't add up, so I ran just the two of them again at 6k, 7k and 8k, three times each, on a night with everything else on the laptop stopped:

| rate | p99, ReverseProxy | p99, PortalGun | CPU per request, ReverseProxy / PortalGun |
|---|---|---|---|
| 6k | 2.4 ms | 3.9 ms | 119 / 122 µs |
| 7k | 3.2 ms | 5.5 ms | 108 / 112 µs |
| 8k | 5.4 ms | 12.4 ms | 97 / 102 µs |

Both held 8k in every run, far under the 50 ms line. The CPU per request was the same as in the sweep; the evening wasn't. At 6k the bare proxy's p99 was 12-22 ms in the sweep and 2.4 ms here, and that close to the knee, a noisier machine decides which side of the line a run lands on. The comparison inside the sweep is still fair, since every proxy ran under the same conditions in rotated order, but I wouldn't quote its maximum rates as properties of the proxies. What does hold is the tail: PortalGun's p99 is 1.6-2.3x the bare proxy's near the knee, on 3-5% more CPU. My guess is the garbage collector, since PortalGun allocates twice as much per request (below), but I haven't checked it.

Before the full benchmark I measured the hot path in isolation with Go benchmarks. On its own code PortalGun costs about twice what ReverseProxy's handler does (5.2 µs against 2.9 µs), but net/http's server and client cost about 19 µs per proxied request, so on a whole request it's about 1.12x, and the real benchmark came in lower still. Profiling took it from 46 allocations per request to 40 (ReverseProxy on its own makes 21): the client key was computed twice, an address parser allocated an error for every Host header without a port, and a buffer pool allocated on every `Put`.

### One limit across several gateways

The hard part of a distributed rate limiter is that the gateways have to agree on one bucket without asking anyone on every request. I built four designs behind one interface, so they run side by side:

| design | how | cost on the request path |
|---|---|---|
| static split | each of N gateways enforces r/N locally | nothing |
| redis-exact | GCRA in a Lua script, one Redis call per request, Redis's own clock | one round trip |
| local-sync | a local budget, synced with a shared bucket in Redis every Δ, split by each gateway's share of recent demand | none; one call per gateway per Δ |
| lease | a gateway takes a batch of tokens (about 20 ms of its own traffic) and spends them locally | one round trip per batch |

To judge accuracy, every gateway records each decision in memory during the run, and afterwards they're merged and replayed through one perfect token bucket. That gives over- and under-admission against what a single, perfect limiter would have done with exactly the same requests.

One key allowed 1,000 requests a second with a burst of 100, two gateways sharing Redis. Latency is measured under the limit at 800 rps; what got through is measured at 1,500 rps, with traffic split evenly, shifting (90% to one gateway, swapping every 5 s), shifting over four gateways, and in bursts of 5,000 rps on top of 800:

| design | added p50 | Redis trips per request | admitted, even | shifting | 4 gateways | bursts |
|---|---|---|---|---|---|---|
| static split | none | 0 | 0.0% | -34.4% | -59.8% | -27.5% |
| redis-exact | +0.38 ms | 1.00 | +9.4% | -9.0% | -19.1% | -11.4% |
| local-sync, 10 ms | none | 0.25 | 0.0% | 0.0% | 0.0% | -0.1% |
| local-sync, 50 ms | none | 0.05 | 0.0% | 0.0% | 0.0% | -1.0% |
| local-sync, 100 ms | none | 0.03 | +0.1% | -0.2% | -0.6% | -1.3% |
| local-sync, 500 ms | none | 0.01 | -0.3% | -19.1% | -18.8% | -14.3% |
| lease, 5 ms | +0.15 ms | 0.43 | 0.0% | 0.0% | -1.6% | -0.1% |
| lease, 20 ms | none | 0.13 | 0.0% | 0.0% | 0.0% | -0.5% |
| lease, 100 ms | none | 0.03 | -0.1% | -0.1% | -0.1% | -2.5% |

Plus means requests let through over the limit, minus means good requests turned away. "None" is under 20 µs, below what this laptop can tell apart.

What I take from this:

- The static split is right only when traffic is even. At 90/10 it turns away a third of what the client is allowed, which is what I'd predicted on paper (about 650 of 1,000 a second), and with four gateways 60%.
- local-sync at 100 ms is what PortalGun runs by default, and the demo too. It adds nothing measurable to the request, makes one Redis call per gateway every 100 ms, and stayed within 1.3% of the perfect bucket in every case. A slow or dead Redis never reaches the request path, because the sync runs beside it. At 10 ms it's within 0.1%, for 8x the Redis calls; I'd only pay that for a limit that has to be tight rather than one that protects an upstream. At 500 ms it falls apart as soon as demand moves.
- redis-exact is the negative result, and the one I learned most from. It should be exact by construction, and in the first run it was, outside the bursts. But it was also waiting on Redis for as long as Redis took (more on that below). Once its 2 ms timeout really applied, about a third of its decisions at 1,500 rps were made without Redis: three calls in a row past 2 ms open a breaker for a second, and in this lab that happens every few seconds. Switching is what hurts. A gateway deciding on its own stops spending from the shared bucket, so the other gateway gets all of it, and nothing is charged back when Redis returns. That's the +9.4%. local-sync already pays back what it admitted while degraded, so the fix is clear: trip on a share of failures instead of three in a row, stay open for 100 ms instead of a second, and charge the debt. I haven't built it yet. Until then, redis-exact isn't the design to use for a limit that's a contract, which is the one job it exists for.
- With 10,000 keys drawn from a Zipf distribution at 3,000 rps, the same breaker sent a third to two thirds of redis-exact's and lease's decisions to the fallback, and both let 5-10% too many through. local-sync stayed exact, at the price of 3.5-7 Redis commands per request, because each sync runs the script once per active key.
- With Redis 5 ms slower, redis-exact's p50 stays at 0.35 ms and every design keeps the limit; with Redis stopped, every decision falls back to the static split, which at an even split admits exactly the limit.

The first full run found two bugs in my own limiters, which is the main reason I trust the second one:

- My 2 ms Redis timeout never applied. go-redis only turns a context deadline into a socket deadline when `ContextTimeoutEnabled` is set, so the deadline only bounded the wait for a pooled connection. With Redis 5 ms slower, every request through redis-exact waited for it (p50 6.5 ms). Turning the option on wasn't enough either: a timed-out connection can't be reused, so against a slow Redis every request would dial a new one. That's why redis-exact and lease got the breaker, and the rerun above showed what the breaker costs.
- local-sync capped the shared bucket at its burst before subtracting what had been spent since the last sync, as if every request had arrived at the end of the interval. Whenever one interval refills more than the burst (500 ms × 1,000/s against a burst of 100), the extra tokens were thrown away, and the limit dropped to about two thirds. At 10-100 ms almost nothing was lost, which is why it looked fine in every shorter setting. After the fix, 500 ms at an even split went from -31% to -0.3%, and 100 ms in bursts from -4.5% to -1.3%.

An earlier smoke run had found two more: local-sync's script was never loaded into Redis at all (go-redis' fallback from `EVALSHA` to `EVAL` can't work inside a pipeline), so it had been running on its static-split fallback the whole time and at a 90/10 split admitted 35% too little; and lease went to about one Redis call per request exactly when the bucket ran dry. The demo's burst button gave the same answer with and without the first bug, which is why I couldn't see it there.

### When a dimension breaks

2,000 requests per second over three dimensions, one of them broken for 30 s, and I count the requests that failed and how long until less than 1% of the traffic still touches the broken one. One gateway, three runs per cell, medians:

| fault | probes only | passive ejection only | both | both + retries |
|---|---|---|---|---|
| crash (connection refused) | 1,560 lost, 1.5 s | 10, at once | 6, at once | 0, at once |
| connection reset | 1,226, 2.0 s | 11, at once | 7, at once | 0, at once |
| blackhole (no answer, 1 s per try) | 18, at once | 16, at once | 11, at once | 0, at once |
| 500 ms slower | 0, at once | 0, at once | 0, at once | 0, at once |
| 30% errors | 6,009, never | 185, 1.5 s | 204, 5.9 s | 0, 0.5 s |
| down 2 s, up 2 s | 3,961, never | 13, 15.9 s | 11, 15.9 s | 0, 15.9 s |

"At once" means within the first 100 ms.

What I take from this:

- With all three together, no request was lost in any of the six faults, and the p99 during the fault stayed at 5.8-6.0 ms, the same as before it. Retries never went above 1.001 attempts per request.
- Probes alone are slow, and on a crash worse than slow: 1,560 lost, more than the dead dimension's third of the traffic. A refused connection fails in microseconds and leaves nothing in flight, so to P2C the dead dimension looks like the cheapest one, and it got 53% of the requests until the probes caught it. I had already kept failures out of the latency average for exactly this reason, and it wasn't enough. Passive ejection catches it in 10 ms and is on in every config I ship, so this only bites with it off; a penalty for failures in P2C's cost would fix it there.
- The blackhole is where my prediction was wrong. I expected the per-try timeout to dominate: everything sent to the hung dimension during the first timeout waits for it, about 170 requests at 250 ms. It lost 11-18 at every timeout I tried, and none with retries. Hung requests pile up in flight, P2C multiplies the dimension's cost by that count, and it stops getting picked within milliseconds, long before anything detects it. The timeout only decides how long those few requests wait, so I left it at 1 s.
- 30% errors stays under the "half of the last 50" rule, and gets caught anyway by five failures in a row, after about 0.9 s, as I'd worked out on paper. Retries hide the errors meanwhile.
- A dimension 500 ms slower: round robin's p99 went to 554 ms until the probes took it out. P2C moved the traffic off at once and p99 didn't move. Passive ejection never fires on a dimension that's only slow, which is what I want.
- Flapping is where probes alone are useless: two failed probes a second apart take most of a 2 s outage, and two good ones fit into the next 2 s up. Passive ejection catches every outage in milliseconds, and since each ejection doubles, after about 16 s the dimension stays out.
- Ten minutes with nothing broken, but 1% errors and log-normal latency on every dimension: no ejections at all.
- All three dimensions failing 20% at once is what the panic threshold is for. With it, 2,507 of about 60,000 requests were lost, and retries stopped at 1.2 attempts per request, so the retry budget held. Without it, every dimension got ejected and the gateway answered 503 itself: 35,527 lost, and 12,798 more after the fault was over, because the ejections outlasted it. 4% lost instead of 59%.
- Two settings bought nothing in this lab: ejecting on the first refused connection (detected in 6 ms instead of 7, nothing lost either way) and slow start (back to 90% of its share in 8.7 s with it and 4.7 s without, no errors either way). These dimensions have no caches to warm, so slow start only costs time here. I kept it for services that do.

The lab broke too. In two of the three repetitions, Toxiproxy itself died about 20 s after round robin had pushed a third of the traffic through 500 ms of latency, and took every dimension with it. The harness now restarts it and runs the cell once more; I haven't found out why it dies.

### Catching a bad release

The canary decides with a sequential test (Wald's SPRT) on who the errors came from, not on an error threshold. If v2 is no worse than v1, an error lands on v2 with probability equal to v2's share of traffic, so a downstream blip that hits both versions cancels out, and the test can look every second without inflating its false rollback rate. A fixed-sample test checked every second doesn't have that property.

I couldn't run thousands of releases on a live stack, so the numbers come from a simulator that calls the same Go package the controller runs: 72 cells, 2,000 trials for every cell where the canary is as good as the stable version and 500 for every regression.

| rule | good versions rolled back, over 36 cells |
|---|---|
| sequential test + exact test at the end of the step (what the controller runs) | 0.1-0.7% |
| exact test once at the end of the step | 0.4-1.5% |
| z-test every second (nominally 5%) | 22-52% |
| canary over 2x stable over 60 s | 1.6-91% |
| canary over a fixed rate (2x the known normal) | 0.2-78%, and 99.9-100% when a 30 s downstream blip hits both versions |

A release that fails every request is out after its first second. The price is small regressions on thin traffic: at 200 rps the test catches a 2x error rate within five minutes in only 61% of steps, where the exact test with the full five minutes of data gets 80%, and it often promotes a 1.2-1.5x regression early. If small regressions matter more than speed, the fixed-horizon test is the better tool.

The simulator also caught two mistakes in my own design: the first rule for an undecided step (roll back if the evidence leans towards "worse") rolled back up to 41% of good versions at low traffic, because with a handful of events the statistic leans that way by construction; and the first end-of-step test used a normal approximation that broke when the canary saw less than one error per step. And a live run caught one the simulator had hidden: my SPRT kept updating after crossing the "no worse" bound, which Wald's test doesn't do, and that quietly made it look more powerful than it is. The table above is after the fix.

The first live rollout on the real stack (two gateways, 200 rps, v2 failing half its requests, starting at 1%) was rolled back after 31 s, but not by the statistics: passive ejection took the only v2 dimension out first, and the engine rolls back when no healthy dimension runs the canary. The test was about two errors short of deciding, because at 1% only half a canary request per second arrived instead of two: with 500 simulated clients, the 1% bucket held fewer of them than it should. The demo's traffic now comes from 2,000.

## How the pieces fit

```
dashboard (Next.js) ──> controller ──> Postgres (config versions, rollouts, chaos)
                            │  ▲
            config + notify │  │ per-second snapshots from every gateway
                            ▼  │
                           Redis <──────── rate limiters
                            ▲
clients ──> gateway ×2 ─────┴──> Toxiproxy ──> dimensions (v1, v2)
```

Gateways never need the controller, Redis or Postgres to keep routing: they keep the last config they had. Losing the controller freezes config and canaries, losing Redis switches the limiters to the static split, losing Postgres only stops config history. Each of those is a test in the Docker e2e suite.

The dashboard's admin token stays in an httpOnly cookie on the Next server. Visitors get bounded chaos: one action at a time, at most 30 s, reverted automatically, with a cooldown and a limit per address, so nobody can hurt the server or the other apps on it. The controller talks to Toxiproxy and the dimensions' control ports, never to the Docker socket.

## Running it

You need Docker. Go 1.26 and Node 24 only for working on the code.

```bash
docker compose up -d --build
```

That starts two gateways, four dimensions behind Toxiproxy, Redis, Postgres, the controller, the dashboard on http://127.0.0.1:3123 and a small traffic generator that only sends while someone has the dashboard open. The controller is on :3121 and the first gateway on :3122. The local admin token is `dev-token`; `docker compose stop` when you're done.

The experiments run their own stacks, publish no ports, and write to `bench/results/<experiment>/<run>/` (raw request logs stay out of git):

```bash
go run ./cmd/bench e1 -workload w1     # proxy overhead against nginx, HAProxy, Envoy, Caddy
go run ./cmd/bench e2 -reps 3          # the limiter designs
go run ./cmd/bench e3 -reps 3          # failover
go run ./cmd/bench hotpath             # Go benchmarks of the hot path; run it in a Linux container
go run ./cmd/canarysim                 # the canary simulator
go run ./cmd/bench report-e2 bench/results/e2/<run>   # tables from a run; report and report-e3 do the same for E1 and E3
```

Tests: `go test -race ./...` (with `PORTAL_TEST_REDIS` and `PORTAL_TEST_POSTGRES` set it also runs the integration tests), `PORTAL_E2E=1 go test ./e2e/` for the Docker suite (its own stack on ports 3131-3133), fuzz targets in `internal/proxy`, and `npm test` in `web/`.

On the server it runs with `docker-compose.prod.yml`, which publishes nothing and takes every secret from a `chmod 600` `.env` that `deploy/init-env.sh` generates. `deploy/deploy.sh` backs up Postgres, fast-forwards, rebuilds, waits for every service to report healthy and goes back to the previous commit if one doesn't. A backup container dumps Postgres every night and keeps two weeks; `deploy/restore-check.sh` restores the latest dump into a throwaway Postgres to prove it can be restored. Only the dashboard is public.

## API at a glance

The controller, behind the dashboard. Writes need the admin token.

| | |
|---|---|
| `GET /api/state` | config version, the last merged snapshot from every gateway, the rollout, recent events |
| `GET /api/events` | the same, pushed as server-sent events |
| `GET /api/config`, `PUT /api/config` | the current config; a new version goes to every gateway over Redis |
| `GET /api/config/history` | earlier versions |
| `GET /api/rollouts`, `POST /api/rollouts`, `POST /api/rollouts/abort` | canary rollouts |
| `GET /api/chaos`, `POST /api/chaos`, `DELETE /api/chaos` | bounded chaos on a dimension |

Each gateway has an admin listener that is never published: `/healthz`, `/metrics` (Prometheus), `/state`, `/events`, `GET` and `PUT /config`, and `POST /decisions/drain`, which hands the limiter's decision log to E2's oracle.

## What's where

| | |
|---|---|
| `cmd/gateway`, `internal/proxy`, `internal/balance`, `internal/health` | the gateway: routing, transport, P2C, probes, ejection, retries |
| `internal/limit` | the four limiter designs and the oracle |
| `internal/canary` | the SPRT, the end-of-step test and the rollout rules |
| `cmd/controller`, `internal/controller`, `internal/store`, `internal/chaos` | the control plane |
| `cmd/dimension`, `cmd/traffic` | the upstreams and the demo's traffic |
| `cmd/loadgen`, `cmd/bench`, `internal/bench`, `bench/` | the load generator, the experiments and their stacks, and every result |
| `cmd/canarysim` | the canary simulator |
| `web/` | the dashboard |
| `e2e/` | the Docker test suite |
| `deploy/` | production setup, deploy, backup and restore check |

## What I'd do next

- A cap on what the gateway holds in total, answering 503 past saturation, so an overload can't push it into its memory limit. E1 showed that's the way it fails today.
- Fix the Redis breaker in redis-exact and lease as described above, with a histogram of Redis call times first, then run their E2 cells again.
- Find out where PortalGun's extra tail near the knee comes from, starting with a GC trace at 7k.
- Run E1 on a machine that isn't a laptop: either a quiet window on the server or a dedicated-CPU VM for an hour, and the other four workloads (slow upstream, large responses, request bodies, a new connection per request).
- Run E3 with two gateways, and with more than three repetitions per cell.
- Later: a failure penalty in P2C's cost, a gossip limiter without Redis, hedged requests, adaptive concurrency limits, HTTP/2 to clients, REST to gRPC.
