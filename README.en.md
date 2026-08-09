# majia-huiyuan · Membership Ops Playbook <!-- plain-ok -->

[![Skill Version](https://img.shields.io/badge/skill-v1.4.1-blue)](./SKILL.md)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](./LICENSE)
[![skills.sh](https://skills.sh/b/maojiebc/majia-huiyuan)](https://skills.sh/maojiebc/majia-huiyuan)
[![Release](https://img.shields.io/github/v/release/maojiebc/majia-huiyuan?label=release&color=success)](https://github.com/maojiebc/majia-huiyuan/releases)
[![Quality](https://github.com/maojiebc/majia-huiyuan/actions/workflows/quality.yml/badge.svg)](https://github.com/maojiebc/majia-huiyuan/actions/workflows/quality.yml)
[![AI Agent Friendly](https://img.shields.io/badge/AI_Agent-friendly-1abc9c)](./AGENTS.md)

> **会员运营 · 马甲实战版** (Membership Ops — Majia's Field Edition) — a complete, auditable, adaptable reference system for chain-store membership data. Modeled on a fictional coffee chain: **55 logical datasets, 25 ETL pipelines, 12 dashboards, plus a ~3,100-line field-tested formula playbook**. <!-- plain-ok -->
>
> All data is simulated — unrelated to any real company. MIT licensed: personal, corporate, commercial use, all fine.

<p align="center">
  <img src="https://raw.githubusercontent.com/maojiebc/majia-huiyuan/main/docs/architecture.png" width="440" alt="majia-huiyuan v1.4.1 architecture: three assets, five warehouse layers, ten consulting jobs, and boundaries with majia-siyu for execution content and majia-guanyuan for platform tooling"/>
</p>

**English README ↓ · [中文说明](./README.md)**

---

## What it is

Membership operations has a no-man's-land: how fields are defined, how calibers are calculated, how dashboards are built. Business folks think it's a tech detail; data folks think it's business trivia; the few who know both treat it as their rice bowl. So this knowledge lives scattered in people's heads and locked in company intranets — and vanishes when they leave.

This repo is the reference nobody publishes — a **showroom**. A show home teaches renovation better than a bare shell: you may not copy it wholesale, but every wall and every pipe is visible.

## Three assets (all inside the skill folder)

| Asset | Where | What |
|---|---|---|
| **Showroom** | `数据集/` `ETL/` `看板/` `清单/` | Simulated reference platform: 55 logical datasets (DIM/DWD/DWS/ADS/DQC/param), 25 ETLs, 12 role dashboards. Corrected logic lives in `ETL/逻辑SQL/` and `ETL/公共口径/`; raw platform JSON remains the historical workshop snapshot. |
| **Formula playbook** | `公式库/` | 10 volumes (~3,100 lines), distilled from real field experience (anonymized): standard SQL for repurchase / RFM / redemption / retention, a field dictionary, NULL tri-state traps, DWD wide-table paradigms, a 39-ETL catalog, and a white-box NBA task-pool model (task generation → dispatch → touch → recovery). |
| **Methodology transcript** | `分享/区域运营的一天/` | Award-winning live-talk write-up (34 illustrated slides): pain points → AI runs a 5-step action chain, human decides → trust quartet → three demo cases → adoption FAQ. |

## As an Agent Skill

This repo is also an **Agent Skill** ([SKILL.md](./SKILL.md) at the root). Install it into any SKILL.md-compatible agent (Claude Code / OpenClaw / Codex / WorkBuddy) and it becomes an on-call **membership-data consultant** for ten kinds of jobs: caliber & formula Q&A, **the data basis behind membership ops actions** (win-back, frequency lift, churn alerts, task dispatch — who to target, when, with what incentive, and how to measure recovery), **CDP & tag-system design** (OneID identity resolution, audience selection, externalized tag rules), membership data architecture design from zero, gap diagnosis, DDL generation, role-based dashboard planning, data-quality troubleshooting, methodology training, and full replication onto Guandata BI.

```bash
clawhub install majia-huiyuan
gh skill install maojiebc/majia-huiyuan majia-huiyuan --agent claude-code --scope user
git clone https://github.com/maojiebc/majia-huiyuan.git ~/.claude/skills/majia-huiyuan
```

## Data notice (read first)

- **All data is programmatically simulated**, unrelated to any real company; personal identifiers are masked (e.g. `156****0925`).
- Up to 200 simulated rows per table (parameter and naturally small tables keep their actual sample size); full scale (80k members, 1.29M orders) is noted in each structure-definition file.
- Structures, fields, and calibers are citable; **numbers must never be used as real business data**.
- SQL dialect is Spark 3.4 — mind function differences on other engines.
- v1.4.1 SQL is a **reference example that still requires validation** against your own schemas, edge cases, and controls; it is not drop-in production code.
- `*/原始JSON/` and `看板/页面JSON/` are historical v1.4.0 workshop snapshots and do not contain the v1.4.1 logic or field corrections; they are not drop-in import bundles.
- Where a structure file retains historical platform fields, its top-level `v1.4.1 结构覆盖` note is authoritative; current CSV sample headers follow that override.

## Sibling project

| Project | Relationship |
|---|---|
| [majia-siyu](https://github.com/maojiebc/majia-siyu-team) | Private-domain **execution content**: Moments copy, broadcasts, welcome scripts, community engagement, and whole-funnel diagnosis. Two halves of the same action: the data basis (who / when / incentive / dispatch / recovery) stays here, the scripts and content live there. |
| [majia-guanyuan](https://github.com/maojiebc/majia-guanyuan) | Guandata BI field-gain-layer skill. **Tools & pitfall handbooks there, data & formulas here.** |

## Version History

- **v1.4.1** (2026-08-09): Business-correctness release — canonical touch/order, coupon-instance/order, and activity/order bridges; one-clock `as_of_date`; point-in-time SCD2 joins; corrected cohort retention, zero-sales cost months, consecutive losses, franchise payback, zero-order days, and complaint-only dates; split the mislabeled parameter table; added nine business-invariant regression checks, GitHub Actions, and bundle-drift verification. Copy-paste production claims were removed.
- **v1.4.0** (2026-08-07): Capability-boundary deep dive — ten consulting jobs (added "data basis behind ops actions" and "CDP & tag-system design"); new formula volume 10 *Task & Touch Recovery* (white-box NBA task-pool model, nine task types × targeting sources × recovery calibers, anti-disturb & priority arbitration, bad-review win-back); the majia-siyu boundary reframed from mutual exclusion to "two halves of the same action".
- **v1.3.2** (2026-07-28): Tightened routing to membership data, metric definitions, SQL, warehouse design, and dashboards; private-domain content and engagement actions now route to `majia-siyu`.
- **v1.3.1** (2026-07-23): Added the architecture preview, English README, and branded display surfaces.
- **v1.3.0** (2026-07-12): Converted the repository into a consultant-style Agent Skill.

Full history: [GitHub Releases](https://github.com/maojiebc/majia-huiyuan/releases).

## 👤 Author

**Majia (@maojiebc)** · 超级马甲 · 14 years in user operations, a "translator between data and operations" in chain restaurants.

📧 [m9224@163.com](mailto:m9224@163.com) · 🐙 [github.com/maojiebc](https://github.com/maojiebc) · 🐦 [@maojiebc](https://x.com/maojiebc)

> Pitfalls you've stepped in and lessons you've drawn aren't private property — they're collective wisdom.
