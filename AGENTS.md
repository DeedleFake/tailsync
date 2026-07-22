# AGENTS.md

Instructions for AI coding agents working in this repository.

## Project overview

**tailsync** synchronizes a directory between machines on a Tailscale tailnet.

The project is early: module and README exist; application code is not yet laid out. Prefer discovering structure from the tree and `go.mod` over assuming a layout.

## Technology stack

| Layer | Choice |
|-------|--------|
| Language | Go — see `go.mod` for the required toolchain |
| Module | `deedles.dev/tailsync` |
| Network | Tailscale tailnet (implementation details TBD as the code grows) |

Do not pin toolchain or dependency versions in this file (they go stale). Prefer “as specified in `go.mod`” or unversioned names.

## Development commands

```bash
go mod download
go mod tidy
go test ./...
go vet ./...
go fmt ./...
```

`go test` already compiles packages; do not run a separate `go build` only to check that the project compiles.

After changing dependencies or tools (for example `go get`, `go get -tool`), run `go mod tidy` so `go.mod` and `go.sum` only list needed modules and checksums.

## Code style and conventions

- **Logging** — `log/slog` with structured key-value fields.
- **Context** — pass `context.Context` as the first argument for cancelable / long-running work.
- **Errors** — handle explicitly; wrap with `fmt.Errorf("...: %w", err)` when adding context.
- **Modern Go** — match current stdlib helpers (`slices`, `maps`, `cmp`, `iter`, etc.) as used with the toolchain in `go.mod`.
- **Imports** — goimports-style groups: standard library, third-party, then `deedles.dev/...`.
- **Scope** — prefer small, focused changes. Do not reformat unrelated files or drive-by refactors.
- **File size** — avoid growing hand-maintained source files past ~1000 lines without decomposing them.

## Agent guidelines

1. **Git is read-only under all circumstances.** Never run write/mutating git commands. That includes (non-exhaustive): `commit`, `add`, `rm`, `mv`, `restore --staged`, `checkout`, `switch`, `branch` (create/delete), `merge`, `rebase`, `cherry-pick`, `stash`, `reset`, `clean`, `tag`, `push`, `pull` (when it updates refs), `am`, `revert`, `commit --amend`, or anything that modifies the index, working tree via git, or remote state. Read-only commands (`status`, `diff`, `log`, `show`, `blame`, `ls-files`, etc.) are fine. Leave all commits and branch management to the user.
2. **Do not pin versions in this file** — refer to `go.mod` or unversioned names so these instructions stay valid as versions change.
3. **Verify** with `go test ./...` and `go vet ./...` before considering work done.
4. **Secrets** — do not commit tokens, API keys, Tailscale auth keys, or machine-specific paths.
5. **Tailscale boundary** — prefer networking over the tailnet only; do not introduce public Internet or bind-to-all behavior unless explicitly requested.

## PR checklist

- [ ] `go test ./...` and `go vet ./...` pass (no separate `go build` needed)
- [ ] `go fmt ./...` applied
- [ ] No secrets in the diff
- [ ] No agent-created git commits or other git writes
