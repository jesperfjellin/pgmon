<div align="center">
  <img src="./logo/logo.png" alt="Project Logo" width="240"/>
</div>

<div align="center">


  
</div>

# PGMon

PostgreSQL monitoring with a web UI and Prometheus metrics export.

PGMon collects performance metrics, detects common issues, and provides recommendations for maintenance operations. It runs as a single binary that connects to PostgreSQL with a read-only user.

## Features

- **Real-time Metrics** - TPS, QPS, latency percentiles, connections, blocking queries
- **Query Analysis** - Top queries by latency, execution counts, and cache hit ratios
- **Storage Monitoring** - Table sizes, bloat detection, dead tuple ratios, autovacuum tracking
- **Alerts** - Wraparound risk, replication lag, stale statistics, partition gaps
- **Historical Trends** - Retention up to n days with automatic downsampling
- **Recommendations** - Automated suggestions for VACUUM, ANALYZE, REINDEX, and autovacuum tuning

PGMon requires only read-only database access and has no external dependencies.

## Quick Start

### 1. Configure Environment

Copy the sample environment file and edit the database connection string:

```bash
cp .env.sample .env
```

Edit `.env` and set your PostgreSQL connection string:

```bash
PGMON_DSN=postgres://your_user:your_password@localhost:5432/your_database?sslmode=require
```

### 2. Run PGMon

Using Docker Compose:

```bash
docker compose up -d pgmon
```

Access the dashboard at http://localhost:8181

### Configuration

Edit `config.pgmon.yaml` to adjust:
- Cluster name
- Polling intervals (hot_path, workload, storage, hourly)
- Alert thresholds
- Bloat detection settings


## Requirements

- PostgreSQL with `pg_monitor` role access
- Optional: `pg_stat_statements` extension for query analysis
- Optional: `pgstattuple` extension for bloat detection

See the comments in `config.pgmon.yaml` for all available options.

## Prometheus Metrics

PGMon exports Prometheus metrics at `/metrics` for scraping. Metrics include transaction rates, latency percentiles, connection counts, wraparound age, bloat estimates, and more.

## Roadmap

See [ROADMAP.md](./ROADMAP.md) for planned features and development status.

## License

MIT License - see [LICENSE](./LICENSE) for details.
