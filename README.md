# sqlite

An opinionated experimental SQLite database driver.

Don't use this yet.

## Goals

The goal is to support a subset of the SQLite API and features, with opinionated defaults
suitable for SQLite usage in modern Go applications.

- A modern driver API (no more `init` magic initialization).
- Defaults to WAL journaling mode.
- Foreign keys checks are enabled by default.
- A default busy timeout of 5 seconds.
- Helpful error messages.

Made in 🇩🇰 by [maragu](https://www.maragu.dk/), maker of [online Go courses](https://www.golang.dk/).
