# AGENTS.md

Instructions for AI coding agents working in this repository.

## Project overview

**tailsync** synchronizes a directory between machines on a Tailscale tailnet.

Prefer discovering structure from the tree and `go.mod` over assuming a layout.

## Technology stack

| Layer | Choice |
|-------|--------|
| Language | Go — see `go.mod` for the required toolchain |
| Module | `deedles.dev/tailsync` |
| Network | Tailscale tailnet (host `tailscaled` by default; optional tsnet) |
| Analysis tools | `modernize`, `staticcheck` — declared as module tools in `go.mod` |

Do not pin toolchain or dependency versions in this file (they go stale). Prefer “as specified in `go.mod`” or unversioned names.

## Development commands

```bash
go mod download
go mod tidy
go test -vet=all ./...
go fmt ./...
go tool modernize ./...
go tool staticcheck ./...
```

`go test` already compiles packages; do not run a separate `go build` only to check that the project compiles.

### Tools

`modernize` and `staticcheck` are module tools (see the `tool` block in `go.mod`). Run them with `go tool <name>`; do not install them with a separate `go install` unless you are debugging the tools themselves.

After changing dependencies or tools (for example `go get`, `go get -tool`), always run **`go mod tidy`** so `go.mod` and `go.sum` only list needed modules and checksums. CI fails if `go mod tidy` would change those files.

## Code style and conventions

- **Logging** — `log/slog` with structured key-value fields.
- **Context** — pass `context.Context` as the first argument for cancelable / long-running work.
- **Errors** — handle explicitly; wrap with `fmt.Errorf("...: %w", err)` when adding context. Error strings should not be capitalized (staticcheck ST1005).
- **Modern Go** — match current stdlib helpers (`slices`, `maps`, `cmp`, `iter`, etc.) as used with the toolchain in `go.mod`. Prefer patterns that `go tool modernize` accepts.
- **Imports** — goimports-style groups: standard library, third-party, then `deedles.dev/...`.
- **Scope** — prefer small, focused changes. Do not reformat unrelated files or drive-by refactors.
- **File size** — avoid growing hand-maintained source files past ~1000 lines without decomposing them.

## Agent guidelines

1. **User-facing language (ASD-STE100).** Write all responses to the user in [ASD-STE100](https://www.asd-ste100.org/) Simplified Technical English unless the user explicitly says otherwise. Apply the STE writing rules in practice:
   - Prefer short, clear sentences (aim for one main idea per sentence; keep sentences short).
   - Use active voice and simple present/past when you can.
   - Prefer concrete verbs and nouns; avoid vague wording, jargon, and figurative language.
   - Use the same word for the same meaning; do not use synonyms for variety.
   - Prefer approved/common technical terms; explain a term only when the user needs it.
   - Use vertical lists for procedures and parallel items.
   - Code identifiers, commands, paths, error strings, and quoted logs stay as-is (do not rewrite them into STE).
   - This rule applies to chat replies and user-facing text you write (for example README or PR descriptions). It does not force STE style into product source code comments or Go identifiers unless the user asks for that.
2. **Git is read-only under all circumstances.** Never run write/mutating git commands. That includes (non-exhaustive): `commit`, `add`, `rm`, `mv`, `restore --staged`, `checkout`, `switch`, `branch` (create/delete), `merge`, `rebase`, `cherry-pick`, `stash`, `reset`, `clean`, `tag`, `push`, `pull` (when it updates refs), `am`, `revert`, `commit --amend`, or anything that modifies the index, working tree via git, or remote state. Read-only commands (`status`, `diff`, `log`, `show`, `blame`, `ls-files`, etc.) are fine. Leave all commits and branch management to the user.
3. **Do not pin versions in this file** — refer to `go.mod` or unversioned names so these instructions stay valid as versions change.
4. **Verify** with `go test -vet=all ./...`, `go tool modernize ./...`, and `go tool staticcheck ./...` before considering work done. Run `go mod tidy` after any dependency or tool change.
5. **Secrets** — do not commit tokens, API keys, Tailscale auth keys, or machine-specific paths.
6. **Tailscale boundary** — prefer networking over the tailnet only; do not introduce public Internet or bind-to-all behavior unless explicitly requested.

## PR checklist

- [ ] `go mod tidy` applied if deps/tools changed (no leftover `go.mod` / `go.sum` drift)
- [ ] `go test -vet=all ./...` passes (no separate `go build` needed)
- [ ] `go tool modernize ./...` and `go tool staticcheck ./...` pass
- [ ] `go fmt ./...` applied
- [ ] No secrets in the diff
- [ ] No agent-created git commits or other git writes
