# ⚠️ Archived Documentation

**These documents are historical and may contain outdated or conflicting information.**

Do not use these for current development. See the main docs instead:
- [SPEC.md](../../SPEC.md) - **Authoritative specification**
- [CHANGELOG.md](../../CHANGELOG.md) - **Accurate version history**
- [README.md](../../README.md) - Quick start guide

## Why These Were Archived

| File | Issue | Status |
|------|-------|--------|
| `v0.1_plan.md` | Completed plan | ✅ Executed successfully |
| `PLAN.md` | Mid-development working doc | Superseded by SPEC.md |
| `roadmap.md` | **Version conflicts** - said v0.2=Consumer Groups but v0.2=Iceberg actually | Superseded by CHANGELOG.md |

## Key Conflict Explained

The `roadmap.md` file planned this version sequence:
- v0.2 = Consumer Groups
- v0.3 = Observability
- v1.0 = Iceberg

**What actually happened:**
- v0.2 = Iceberg/Parquet (prioritized for analytics use case)
- v0.3 = Streaming + Observability (planned)
- Consumer Groups = Future (deprioritized)

The authoritative version history is in [CHANGELOG.md](../../CHANGELOG.md).
