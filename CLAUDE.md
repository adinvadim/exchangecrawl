# CLAUDE.md

## Security: public repo, real money

This repository is **public** and the code works with **real exchange accounts and funds** (Bybit, Binance). Treat every commit as world-readable the moment it is pushed.

**Secrets must never live in this repository — not even temporarily.** No API keys, secrets, tokens, session cookies, or account identifiers in code, config files, test fixtures, docs, examples, commit messages, or `.env` files. The repo is deliberately structured so there is no place for them: credentials come only from environment variables (`BYBIT_API_KEY`, `BYBIT_API_SECRET`, `BINANCE_API_KEY`, `BINANCE_API_SECRET`, ...), and config files store environment-variable *names*, never values. Keep it that way — do not add a config field, flag, or fixture that would hold a secret value, so there is never a temptation (for a developer or an agent) to paste one in.

Before committing or pushing:

- Review the full diff (`git diff --staged`) for anything that looks like a key, secret, token, or real account data — including in logs, test data, and documentation examples.
- Real placeholder values in docs must be obviously fake (`"..."`, `your-key-here`), never redacted real ones.
- SQLite archives (`*.db`) are gitignored because they can contain real account data — never force-add them or loosen `.gitignore`.
- If a secret does slip into a commit, do not just delete it in a follow-up commit: the key is compromised and must be revoked/rotated immediately, and history rewritten before pushing.
