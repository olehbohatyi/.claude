---
name: commit-message
description: Write and validate git commit messages using the Conventional Commits v1.0.0 specification. Use this whenever the user asks to commit changes, write a commit message, generate a changelog, determine a semantic version bump, or mentions "conventional commits", "commit message format", or "semver commits".
allowed-tools: Bash(git diff:*), Bash(git status:*), Bash(git log:*), Bash(git add:*), Bash(git commit:*), Read
model: haiku
# ^ formulaic classify+write task, haiku is plenty. Bump to sonnet/inherit if you want richer bodies for complex diffs.
---

# Conventional Commits

Write commit messages that follow the [Conventional Commits v1.0.0](https://www.conventionalcommits.org/en/v1.0.0/) spec, so history stays machine-parseable for changelogs and semver.

## Format

```
<type>[optional scope][!]: <description>

[optional body]

[optional footer(s)]
```

- **type**: noun describing the change (see Types below). REQUIRED.
- **scope**: noun in parentheses naming the affected area, e.g. `(parser)`. OPTIONAL.
- **!**: placed right before the colon to flag a breaking change, e.g. `feat(api)!:`. OPTIONAL.
- **description**: short summary immediately after `: `. REQUIRED.
- **body**: free-form, one or more paragraphs, separated from the description by one blank line. OPTIONAL.
- **footer(s)**: one blank line after the body, each a `Token: value` or `Token #value` line (use `-` in place of spaces in the token, e.g. `Reviewed-by`, `Refs`). OPTIONAL.

## Types

| Type | SemVer | Meaning |
|---|---|---|
| `fix` | PATCH | bug fix |
| `feat` | MINOR | new feature |
| `build`, `chore`, `ci`, `docs`, `style`, `refactor`, `perf`, `test` | none (unless breaking) | other changes (Angular convention, not mandated by spec) |

Only `fix` and `feat` have defined SemVer meaning. Any other type is allowed but has no implicit version effect.

## Breaking changes

Signal a breaking change (→ MAJOR) either way:
1. `!` right before the colon: `feat!: drop support for Node 6`
2. A footer, exactly `BREAKING CHANGE: <description>` (uppercase, required if no `!` is used, or add both for clarity).

`BREAKING-CHANGE` (hyphenated) in a footer token is synonymous with `BREAKING CHANGE`.

## Rules to enforce

- Type/scope/description line MUST NOT be case-sensitive except `BREAKING CHANGE`, which MUST be uppercase.
- Description follows the colon+space directly — no capitalization or punctuation requirement, just be short and imperative.
- If a commit mixes unrelated concerns, split it into multiple commits rather than combining types.
- Casing convention (upper/lower for types) just needs to be consistent within the repo.

## Examples
#### Commit message with no body
```
docs: correct spelling of CHANGELOG
```
#### Commit message with scope
```
feat(lang): add Ukrainian language
```
#### Commit message with ! to draw attention to breaking change
```
feat!: send an email to the customer when a product is shipped
```

## Workflow

When asked to commit (staging first if the user says "add and commit" or nothing is staged yet):
1. If nothing is staged, `git add` the relevant files (ask before a broad `git add -A`/`.` — never stage secrets).
2. Run `git diff --staged` to see what changed.
3. Check `git log -10 --format=%B` for this repo's existing convention: single-line-only messages, or header+body+footer. Match it rather than defaulting to the full template — Conventional Commits doesn't require a body or footer, and a repo that's consistently terse should stay that way unless the user asks otherwise.
4. Pick the type that matches the dominant change; split into multiple commits if concerns are mixed.
5. Add a scope if the change is localized to one module/area.
6. Write a short, imperative description (e.g. "add", not "added"/"adds").
7. Add a body only if the "why" isn't obvious from the diff AND the repo's convention (step 3) allows bodies.
8. Flag breaking changes with `!` and/or a `BREAKING CHANGE:` footer.
9. Run `git commit -m "<header>" -m "<body>" -m "<footer>"` (separate `-m` flags per block), or just `-m "<header>"` for a single-line commit, or open an editor for multi-paragraph messages.
