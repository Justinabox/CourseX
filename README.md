# CourseX

[https://coursex.school.rip](https://coursex.school.rip)

A redo of USC's [Schedule of Classes website](classes.iusc.edu), putting your experience at the center of focus.

Made by students, for students.

All contributions are welcomed.

## Setup & Contributing

Clone this repository
```bash
git clone https://github.com/MeloticZ/CourseX
```

Make sure to install dependencies:

```bash
bun install
```

Start the development server on `http://localhost:3000`:

```bash
bun run dev
```

Build the application for production:

```bash
bun run build
```

Locally preview production build:

```bash
bun run preview
```

## TiDB ETL (hourly)

The ETL pulls directly from USC + RMP APIs and loads into TiDB staging tables, then promotes on success.

- Entrypoint: `scripts/etl_tidb.py`
- Helpers: `scripts/db.py`, `scripts/fetch_courses.py`, `scripts/fetch_professors.py`

Install Python requirements:

```bash
pip3 install -r requirements.txt
```

Environment variables (export in your shell or a `.env` with the same names):

- `TIDB_HOST` (e.g., `127.0.0.1`)
- `TIDB_PORT` (e.g., `4000`)
- `TIDB_USER`
- `TIDB_PASSWORD`
- `TIDB_DATABASE` (default `coursex`)
- `TIDB_SSL_CA` (optional CA path for TiDB Serverless)

Run example:

```bash
python3 scripts/etl_tidb.py --semester-id 20261 --concurrency 12 --update-professors yes
```

Flags:

- `--semester-id` Term code (e.g., 20261)
- `--concurrency` Parallelism for USC fetches (default 12)
- `--update-professors` `yes|no` whether to refresh RMP into staging (default yes)
- `--dry-run` Skip promotion phase while still loading and validating staging

Behavior:

- Loads all data into `staging_*` tables
- Validates referential integrity (no orphan sections)
- On success, promotes to prod in one transaction
- Records run in `etl_runs`