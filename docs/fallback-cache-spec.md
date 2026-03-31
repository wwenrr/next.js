# Fallback Cache for Turbo Tasks

## Problem

Turbo's primary cache is keyed on all task inputs — source files, config,
lockfiles, toolchain. When any source file changes, the cache misses and the
entire task runs from scratch. For tasks with expensive, stable dependencies
(Rust crate compilation, Docker base layers), this means re-doing minutes of
work that hasn't actually changed.

In practice, ~90% of cache misses are "dirty source, clean deps" — the
dependency graph (`Cargo.lock`, `Dockerfile` dep-install stages) hasn't changed,
only local code has.

## Solution: Fallback Cache

A task can declare a **fallback cache** — a secondary, coarser-grained cache
that provides a warm start when the primary cache misses.

**Core invariants:**

1. **Read on primary miss only.** The fallback is never consulted when the
   primary cache hits.
2. **Write on task success only.** The fallback is never written if the parent
   task fails, preventing cache poisoning from partial artifacts.
3. **Best-effort.** If fallback restore fails, the task proceeds as a cold
   build. The fallback is an optimization, not a correctness requirement.
4. **Does not replace primary.** After a fallback-assisted build succeeds, the
   primary cache is still written with the full key. Next identical build is a
   full cache hit with zero fallback involvement.

## Lifecycle

```
Task invoked
  │
  ├─ Primary cache HIT → restore primary artifacts, done
  │
  └─ Primary cache MISS
       │
       ├─ Fallback cache HIT
       │    1. Restore fallback artifacts to disk
       │    2. Run postLoad hook (fixup artifacts for current env)
       │    3. Export TURBO_FALLBACK_DIR=<path> into task env
       │    4. Run task (incremental build — only changed inputs recompile)
       │    5. Run preStore hook (prepare artifacts for storage)
       │    6. Write fallback cache (updated artifacts)
       │    7. Write primary cache (full task output, as normal)
       │
       └─ Fallback cache MISS
            1. Run task (full cold build)
            2. Run preStore hook (prepare artifacts for storage)
            3. Write fallback cache (first seed)
            4. Write primary cache (full task output, as normal)
```

## Configuration

```jsonc
// turbo.json
{
  "tasks": {
    "my-task": {
      "inputs": ["...all normal inputs..."],
      "outputs": ["...normal outputs..."],

      "fallbackCache": {
        // Subset of inputs that cover the stable/expensive part.
        // When these haven't changed, the fallback cache is valid.
        "inputs": ["Cargo.lock", "rust-toolchain.toml"],

        // Artifacts to store and restore for the warm start.
        "outputs": [
          "target/release/deps/**",
          "target/release/.fingerprint/**",
          "target/release/build/**"
        ],

        // Optional. Shell command run after fallback restore, before the task.
        // Use to fixup restored artifacts for the current environment.
        // Receives TURBO_FALLBACK_DIR as the path where artifacts were restored.
        "postLoad": "scripts/fallback-post-load.sh",

        // Optional. Shell command run after task success, before fallback store.
        // Use to strip machine-specific data, prune unnecessary files, etc.
        "preStore": "scripts/fallback-pre-store.sh"
      }
    }
  }
}
```

### Key computation

The fallback cache key is computed the same way as the primary cache key, but
over the `fallbackCache.inputs` glob set instead of the task's `inputs`. This
means the fallback key changes only when the dependency manifest or toolchain
changes — not when source files change.

```
fallback_key = hash(
  task_name,
  "fallback-v1",
  hash_of(fallbackCache.inputs),
  platform/arch
)
```

### Environment interface

When the fallback cache hits, the task's environment gets:

| Variable | Value | Purpose |
|---|---|---|
| `TURBO_FALLBACK_HIT` | `"1"` | Lets the task command adapt behavior |
| `TURBO_FALLBACK_DIR` | absolute path | Where fallback artifacts were restored |

This allows the task command itself to conditionally use the fallback (critical
for Docker's `--cache-from` pattern) without requiring command templating.

## Hooks

### `preStore`

Runs after the parent task succeeds, before writing the fallback cache.

**Purpose:** Prepare artifacts for storage — strip absolute paths, remove
machine-specific state, prune large files with low cache hit value.

**Contract:**
- Working directory: repo root
- `TURBO_FALLBACK_DIR`: path containing the artifacts to be stored
- Exit 0: proceed with fallback cache write
- Exit non-zero: skip fallback cache write (logged as warning, not a build failure)

### `postLoad`

Runs after fallback cache restore, before the parent task executes.

**Purpose:** Fix up restored artifacts for the current environment — rewrite
paths, set timestamps, configure environment variables for the parent task.

**Contract:**
- Working directory: repo root
- `TURBO_FALLBACK_DIR`: path where fallback artifacts were restored
- May write to stdout in `KEY=VALUE` format to inject env vars into the parent
  task (similar to GitHub Actions' `$GITHUB_OUTPUT`)
- Exit 0: proceed with task execution using restored artifacts
- Exit non-zero: discard restored artifacts, proceed as cold build (logged as warning)

## Concrete Use Cases

### Rust (next-swc native build)

**What's expensive:** Compiling ~200+ dependency crates (2-5 minutes).

**What changes often:** Local crate source in `crates/` and `packages/next-swc/`.

**Fallback key:** `Cargo.lock` + `rust-toolchain.toml` — these pin the exact
dependency versions and compiler. When they haven't changed, all dependency
`.rlib`/`.rmeta` files are still valid.

```jsonc
{
  "tasks": {
    "build-native": {
      "inputs": [
        "Cargo.lock",
        "Cargo.toml",
        "rust-toolchain.toml",
        "crates/**",
        "packages/next-swc/**",
        "turbopack/**"
      ],
      "outputs": ["packages/next-swc/native/*.node"],

      "fallbackCache": {
        "inputs": ["Cargo.lock", "rust-toolchain.toml"],
        "outputs": [
          "target/release/deps/**",
          "target/release/.fingerprint/**",
          "target/release/build/**"
        ],
        "preStore": "scripts/rust-fallback-pre-store.sh",
        "postLoad": "scripts/rust-fallback-post-load.sh"
      }
    }
  }
}
```

**`preStore` (Rust):**
```bash
#!/bin/bash
# Strip artifacts that don't cache well or contain machine-specific state
cd "$TURBO_FALLBACK_DIR"
# Incremental compilation artifacts are large and machine-specific
rm -rf target/release/incremental
# .d files contain absolute paths that won't match on other machines
find target/release/deps -name '*.d' -delete
find target/release/.fingerprint -name '*.json' -exec \
  sed -i '' 's|'"$PWD"'|REPO_ROOT|g' {} +
```

**`postLoad` (Rust):**
```bash
#!/bin/bash
# Restore absolute paths in fingerprint metadata
cd "$TURBO_FALLBACK_DIR"
find target/release/.fingerprint -name '*.json' -exec \
  sed -i '' 's|REPO_ROOT|'"$PWD"'|g' {} +
# Touch all .rlib files so cargo doesn't consider them stale vs Cargo.lock mtime
find target/release/deps -name '*.rlib' -exec touch {} +
```

**Expected speedup:** A fallback-warm Rust build compiles only the local crates
(~10-30s) instead of all dependencies + local crates (~3-5min). The fallback
archive is ~200-500 MB compressed (deps only, no incremental).

### Docker (builder image)

**What's expensive:** `apt-get install`, downloading/installing toolchains,
`npm ci` of system dependencies (2-10 minutes per layer).

**What changes often:** Application source code copied into later layers.

**Fallback key:** `Dockerfile` + dependency lockfiles — these determine the
base layers. When they haven't changed, the layer cache is valid.

```jsonc
{
  "tasks": {
    "docker-build": {
      "inputs": [
        "scripts/native-builder.Dockerfile",
        "rust-toolchain.toml",
        "Cargo.lock",
        "Cargo.toml",
        "crates/**",
        "packages/next-swc/**"
      ],
      "outputs": [".docker-image-id"],

      "fallbackCache": {
        "inputs": [
          "scripts/native-builder.Dockerfile",
          "rust-toolchain.toml"
        ],
        "outputs": [".cache/docker-buildx/**"],
        "postLoad": "scripts/docker-fallback-post-load.sh",
        "preStore": "scripts/docker-fallback-pre-store.sh"
      }
    }
  }
}
```

**`postLoad` (Docker):**
```bash
#!/bin/bash
# Inject cache-from into the task's env so the docker build command uses it
echo "DOCKER_CACHE_FROM=type=local,src=$TURBO_FALLBACK_DIR/.cache/docker-buildx"
```

The task command references this:
```bash
docker buildx build \
  ${DOCKER_CACHE_FROM:+--cache-from $DOCKER_CACHE_FROM} \
  --cache-to type=local,dest=.cache/docker-buildx,mode=max \
  -t next-swc-builder:latest \
  -f scripts/native-builder.Dockerfile .
```

**`preStore` (Docker):**
```bash
#!/bin/bash
# buildx cache-to already wrote to .cache/docker-buildx — nothing extra needed.
# Could prune old layers here if size is a concern.
echo "Docker buildx cache ready for storage"
```

**Expected speedup:** A fallback-warm Docker build reuses all base layers and
only rebuilds from the `COPY` of changed source onward (~30s vs ~5-10min cold).

## Integration with Existing Infrastructure

This feature builds on the existing `turbo-cache.mjs` remote cache client.
Fallback artifacts are stored as zstd-compressed tarballs using the same
`exists`/`get`/`put` API, with keys derived from `fallbackCache.inputs`.

The existing `native-cache.js` and `docker-image-cache.js` scripts are
effectively hand-rolled versions of this pattern. With fallback cache as a
first-class turbo feature, these scripts can be replaced by turbo.json
configuration.

### Migration path

| Current | Fallback cache equivalent |
|---|---|
| `native-cache.js --restore` | Automatic: fallback restore + `postLoad` |
| `native-cache.js --save` | Automatic: `preStore` + fallback store |
| `docker-image-cache.js` | Automatic: fallback restore/store + hooks |
| sccache (vercel fork) | Unchanged — sccache operates at per-compilation-unit level via Vercel Artifacts backend, complementary to task-level fallback |

## Design Decisions

### Why hooks instead of built-in artifact knowledge?

Hooks keep the feature generic. Turbo doesn't need to understand Rust
fingerprints, Docker layer format, or Go module caches. Each ecosystem's hooks
handle the specifics. This also means the feature works for ecosystems we
haven't anticipated.

### Why env vars instead of command rewriting?

The `postLoad` hook exports env vars (`DOCKER_CACHE_FROM`, etc.) that the task
command references with standard shell expansion (`${VAR:+--flag $VAR}`). This
is simpler than command templating and composes naturally — multiple hooks can
each contribute env vars without conflicting.

### Why write-on-success only?

A failed Rust build leaves partial `.rlib` files and inconsistent fingerprints.
Caching these would cause downstream builds to fail with confusing linker
errors. By writing only after success, the fallback cache always contains a
known-good dependency tree.

### Relationship to sccache

sccache operates at a different granularity: per-compilation-unit (individual
`.rs` → `.o` translations). Fallback cache operates at the task level (entire
dependency tree). They are complementary:

- **Fallback cache** restores the bulk dependency artifacts in one fetch
- **sccache** fills in any individual compilation units that the fallback
  doesn't cover (e.g., if a single dep was added to `Cargo.lock`)

Both can be active simultaneously. sccache sees cache hits for units already
restored by fallback, which is fast (local check, no network).

## Open Questions

1. **Fallback TTL / eviction.** Should fallback caches expire faster than
   primary caches? They're larger (all deps) but less precise. Suggestion:
   inherit the task's cache TTL by default, allow override via
   `fallbackCache.ttl`.

2. **Size limits.** Rust `target/*/deps/` can be 1-5 GB compressed. Should
   turbo enforce a per-fallback size cap, or leave that to the remote cache
   backend?

3. **Multiple fallback tiers.** A chain like `Cargo.lock` exact →
   `Cargo.lock` minus patch versions → toolchain only. Probably YAGNI for v1 —
   a single fallback tier covers the 90% case.

4. **Concurrent writes.** Two machines miss primary simultaneously, both write
   fallback. Last-write-wins is fine since the content should be equivalent
   (same deps, same toolchain).

5. **Local vs remote fallback.** Should there be a local-disk fallback tier
   (faster, no network) in addition to remote? Useful for developers iterating
   locally across branches with the same `Cargo.lock`.
