"""
War Analyser Signal — Geopolitical Conflict Analysis Tool
Fetches news from RSS feeds, maintains conflict database, runs AI analysis, notifies Discord.
"""

import csv
import json
import logging
import os
import re
import sys
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Infrastructure imports (provided by ClaudeCodeIde)
# ---------------------------------------------------------------------------
from claude_code import ClaudeCode
from discord_notifier import DiscordNotifier

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent if "__file__" in dir() else Path.cwd()
CONFIG_PATH = SCRIPT_DIR / "config.json"
DATA_DIR = SCRIPT_DIR / "data"

CONFLICTS_CSV = DATA_DIR / "conflicts.csv"
HISTORY_CSV = DATA_DIR / "analysis_history.csv"
CONCLUSIONS_FILE = DATA_DIR / "conclusions.json"
SNAPSHOTS_CSV = DATA_DIR / "war_snapshots.csv"
LOG_FILE = DATA_DIR / "analyse.log"
LOG_MAX_LINES = 30_000

log = logging.getLogger("war_analyse")

CONFLICT_STATUS_ACTIVE = {"ongoing", "started", "escalation_risk"}
CONFLICT_STATUS_ENDED = {"ended"}

CONFLICTS_COLUMNS = [
    "id", "name", "region", "start_date", "end_date", "escalation_date",
    "status", "civilian_casualties_est", "military_casualties_est", "last_updated",
]

HISTORY_COLUMNS = [
    "timestamp", "run_number", "run_id",
    "active_conflicts_count", "escalation_risk_count",
    "top_conflict", "top_conflict_intensity",
    "global_risk_score",
    "summary", "comparison_to_previous",
    "discord_message", "model", "cost_usd",
]

SNAPSHOTS_COLUMNS = [
    "timestamp", "run_id", "conflict_id", "conflict_name",
    "status", "intensity_score",
    "end_chance_1w", "end_chance_1m", "end_chance_1y",
    "outbreak_chance_3d", "outbreak_chance_1w", "outbreak_chance_2w",
    "civilian_casualties_est", "military_casualties_est",
]

# JSON schema for Claude's per-conflict analysis
CONFLICT_ANALYSIS_SCHEMA = {
    "type": "object",
    "properties": {
        "conflict_id": {"type": "string"},
        "intensity_score": {"type": "integer", "minimum": 0, "maximum": 100},
        "end_chance_1w": {"type": "integer", "minimum": 0, "maximum": 100},
        "end_chance_1m": {"type": "integer", "minimum": 0, "maximum": 100},
        "end_chance_1y": {"type": "integer", "minimum": 0, "maximum": 100},
        "outbreak_chance_3d": {"type": "integer", "minimum": 0, "maximum": 100},
        "outbreak_chance_1w": {"type": "integer", "minimum": 0, "maximum": 100},
        "outbreak_chance_2w": {"type": "integer", "minimum": 0, "maximum": 100},
        "civilian_casualties_est": {"type": "integer", "minimum": 0},
        "military_casualties_est": {"type": "integer", "minimum": 0},
        "summary": {"type": "string"},
        "justification": {"type": "string"},
        "comparison_to_previous": {"type": "string"},
    },
    "required": [
        "conflict_id", "intensity_score",
        "end_chance_1w", "end_chance_1m", "end_chance_1y",
        "outbreak_chance_3d", "outbreak_chance_1w", "outbreak_chance_2w",
        "civilian_casualties_est", "military_casualties_est",
        "summary", "justification", "comparison_to_previous",
    ],
}

FULL_ANALYSIS_SCHEMA = {
    "type": "object",
    "properties": {
        "conflicts": {
            "type": "array",
            "items": CONFLICT_ANALYSIS_SCHEMA,
        },
        "new_conflicts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "region": {"type": "string"},
                    "start_date": {"type": "string"},
                    "status": {"type": "string", "enum": ["started", "ongoing", "escalation_risk"]},
                    "civilian_casualties_est": {"type": "integer", "minimum": 0},
                    "military_casualties_est": {"type": "integer", "minimum": 0},
                    "summary": {"type": "string"},
                },
                "required": ["name", "region", "start_date", "status",
                             "civilian_casualties_est", "military_casualties_est", "summary"],
            },
        },
        "ended_conflicts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "conflict_id": {"type": "string"},
                    "end_date": {"type": "string"},
                    "reason": {"type": "string"},
                },
                "required": ["conflict_id", "end_date", "reason"],
            },
        },
        "global_risk_score": {"type": "integer", "minimum": 0, "maximum": 100},
        "global_summary": {"type": "string"},
        "comparison_to_previous": {"type": "string"},
    },
    "required": ["conflicts", "new_conflicts", "ended_conflicts",
                 "global_risk_score", "global_summary", "comparison_to_previous"],
}


# ═══════════════════════════════════════════════════════════════════════════
# Logging
# ═══════════════════════════════════════════════════════════════════════════
def trim_log_file():
    if not LOG_FILE.exists():
        return
    try:
        lines = LOG_FILE.read_text(encoding="utf-8").splitlines()
        if len(lines) > LOG_MAX_LINES:
            LOG_FILE.write_text("\n".join(lines[-LOG_MAX_LINES:]) + "\n", encoding="utf-8")
    except OSError:
        pass


def setup_logging():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    trim_log_file()
    if log.handlers:
        return
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    log.setLevel(logging.DEBUG)
    log.addHandler(fh)
    log.addHandler(ch)


# ═══════════════════════════════════════════════════════════════════════════
# Config
# ═══════════════════════════════════════════════════════════════════════════
def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# ═══════════════════════════════════════════════════════════════════════════
# Conflicts CSV — master list
# ═══════════════════════════════════════════════════════════════════════════
def load_conflicts() -> list[dict]:
    """Load all conflicts from CSV. Returns list of dicts."""
    if not CONFLICTS_CSV.exists():
        return []
    rows = []
    with open(CONFLICTS_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def save_conflicts(conflicts: list[dict]):
    """Overwrite conflicts CSV with updated list."""
    with open(CONFLICTS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CONFLICTS_COLUMNS)
        writer.writeheader()
        writer.writerows(conflicts)


def get_active_conflicts(conflicts: list[dict]) -> list[dict]:
    return [c for c in conflicts if c.get("status", "") in CONFLICT_STATUS_ACTIVE]


def compute_tier(conflict: dict) -> int:
    """Deterministic tier based on total estimated casualties. Lower number = higher tier."""
    try:
        total = int(conflict.get("civilian_casualties_est") or 0) + \
                int(conflict.get("military_casualties_est") or 0)
    except (ValueError, TypeError):
        total = 0
    if total >= 100_000:
        return 1
    if total >= 10_000:
        return 2
    if total >= 1_000:
        return 3
    if total >= 100:
        return 4
    return 5


def sort_conflicts_by_tier(conflicts: list[dict]) -> list[dict]:
    """Sort by total casualties descending (Tier 1 first)."""
    def total_casualties(c):
        try:
            return int(c.get("civilian_casualties_est") or 0) + \
                   int(c.get("military_casualties_est") or 0)
        except (ValueError, TypeError):
            return 0
    return sorted(conflicts, key=total_casualties, reverse=True)


def apply_analysis_to_conflicts(
    conflicts: list[dict],
    result: dict,
    timestamp: str,
) -> list[dict]:
    """
    Update conflict list with analysis results:
    - Update casualties estimates for active conflicts
    - Mark ended conflicts
    - Add new conflicts
    """
    conflicts = list(conflicts)

    # Build lookup by id
    by_id = {c["id"]: c for c in conflicts}

    # Update casualties for analysed conflicts
    for ca in result.get("conflicts", []):
        cid = ca.get("conflict_id")
        if cid and cid in by_id:
            by_id[cid]["civilian_casualties_est"] = ca.get("civilian_casualties_est",
                                                            by_id[cid].get("civilian_casualties_est", 0))
            by_id[cid]["military_casualties_est"] = ca.get("military_casualties_est",
                                                            by_id[cid].get("military_casualties_est", 0))
            by_id[cid]["last_updated"] = timestamp

    # Mark ended conflicts
    for ended in result.get("ended_conflicts", []):
        cid = ended.get("conflict_id")
        if cid and cid in by_id:
            by_id[cid]["status"] = "ended"
            by_id[cid]["end_date"] = ended.get("end_date", timestamp[:10])
            by_id[cid]["last_updated"] = timestamp
            log.info("Conflict ended: %s (%s)", by_id[cid]["name"], ended.get("reason", ""))

    # Add new conflicts
    for nc in result.get("new_conflicts", []):
        new_id = str(uuid.uuid4())[:8]
        new_conflict = {
            "id": new_id,
            "name": nc.get("name", "Unknown"),
            "region": nc.get("region", "Unknown"),
            "start_date": nc.get("start_date", timestamp[:10]),
            "end_date": "",
            "escalation_date": "",
            "status": nc.get("status", "started"),
            "civilian_casualties_est": nc.get("civilian_casualties_est", 0),
            "military_casualties_est": nc.get("military_casualties_est", 0),
            "last_updated": timestamp,
        }
        conflicts.append(new_conflict)
        log.info("New conflict detected: %s (%s)", new_conflict["name"], new_conflict["region"])

    # Rebuild from by_id (preserving order of existing, then new appended)
    existing_ids = {c["id"] for c in conflicts if c["id"] in by_id}
    updated = [by_id[c["id"]] if c["id"] in by_id else c for c in conflicts]
    return updated


def format_conflicts_for_prompt(conflicts: list[dict]) -> str:
    """Format active conflicts for Claude prompt."""
    active = get_active_conflicts(conflicts)
    if not active:
        return "## Known Conflicts\n\nNo active conflicts in database. This may be the first run.\n"

    active_sorted = sort_conflicts_by_tier(active)
    lines = ["## Known Active Conflicts (sorted by estimated casualties)\n"]

    for c in active_sorted:
        tier = compute_tier(c)
        civ = c.get("civilian_casualties_est", "unknown")
        mil = c.get("military_casualties_est", "unknown")
        lines.append(
            f"**[{c['id']}] {c['name']}** | Region: {c['region']} | Status: {c['status']} | Tier: {tier}\n"
            f"  Started: {c.get('start_date', '?')} | Last updated: {c.get('last_updated', '?')}\n"
            f"  Est. casualties: civilian={civ}, military={mil}"
        )
        lines.append("")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# History & conclusions
# ═══════════════════════════════════════════════════════════════════════════
def load_history(lookback: int) -> list[dict]:
    if not HISTORY_CSV.exists():
        return []
    rows = []
    with open(HISTORY_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows[-lookback:] if len(rows) > lookback else rows


def load_conclusions(lookback: int) -> list[dict]:
    if not CONCLUSIONS_FILE.exists():
        return []
    try:
        with open(CONCLUSIONS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data[-lookback:] if len(data) > lookback else data
    except (json.JSONDecodeError, IOError):
        return []


def save_history(result: dict, run_number: int, run_id: str, timestamp: str,
                 active_conflicts: list[dict], snapshot_analyses: list[dict],
                 model: str, cost: float | None, discord_msg: str):
    write_header = not HISTORY_CSV.exists() or HISTORY_CSV.stat().st_size == 0

    top = sort_conflicts_by_tier(active_conflicts)
    top_name = top[0]["name"] if top else "N/A"

    # Find intensity score for top conflict
    top_intensity = "?"
    for ca in snapshot_analyses:
        if top and ca.get("conflict_id") == top[0].get("id"):
            top_intensity = ca.get("intensity_score", "?")
            break

    escalation_count = sum(1 for c in active_conflicts if c.get("status") == "escalation_risk")

    row = {
        "timestamp": timestamp,
        "run_number": run_number,
        "run_id": run_id,
        "active_conflicts_count": len([c for c in active_conflicts if c.get("status") != "escalation_risk"]),
        "escalation_risk_count": escalation_count,
        "top_conflict": top_name,
        "top_conflict_intensity": top_intensity,
        "global_risk_score": result.get("global_risk_score", "?"),
        "summary": result.get("global_summary", "")[:500],
        "comparison_to_previous": result.get("comparison_to_previous", "")[:300],
        "discord_message": discord_msg,
        "model": model,
        "cost_usd": cost,
    }

    with open(HISTORY_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HISTORY_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def save_snapshot(run_id: str, timestamp: str, snapshot_analyses: list[dict],
                  active_conflicts: list[dict]):
    """Save per-conflict scores to snapshot CSV."""
    write_header = not SNAPSHOTS_CSV.exists() or SNAPSHOTS_CSV.stat().st_size == 0
    by_id = {c["id"]: c for c in active_conflicts}

    with open(SNAPSHOTS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SNAPSHOTS_COLUMNS)
        if write_header:
            writer.writeheader()
        for ca in snapshot_analyses:
            cid = ca.get("conflict_id", "")
            c = by_id.get(cid, {})
            writer.writerow({
                "timestamp": timestamp,
                "run_id": run_id,
                "conflict_id": cid,
                "conflict_name": c.get("name", "Unknown"),
                "status": c.get("status", ""),
                "intensity_score": ca.get("intensity_score"),
                "end_chance_1w": ca.get("end_chance_1w"),
                "end_chance_1m": ca.get("end_chance_1m"),
                "end_chance_1y": ca.get("end_chance_1y"),
                "outbreak_chance_3d": ca.get("outbreak_chance_3d"),
                "outbreak_chance_1w": ca.get("outbreak_chance_1w"),
                "outbreak_chance_2w": ca.get("outbreak_chance_2w"),
                "civilian_casualties_est": ca.get("civilian_casualties_est"),
                "military_casualties_est": ca.get("military_casualties_est"),
            })


def save_conclusion(result: dict, run_number: int, timestamp: str,
                    active_conflicts: list[dict], snapshot_analyses: list[dict]):
    conclusions = []
    if CONCLUSIONS_FILE.exists():
        try:
            with open(CONCLUSIONS_FILE, "r", encoding="utf-8") as f:
                conclusions = json.load(f)
        except (json.JSONDecodeError, IOError):
            conclusions = []

    top = sort_conflicts_by_tier(active_conflicts)

    entry = {
        "timestamp": timestamp,
        "run_number": run_number,
        "global_risk_score": result.get("global_risk_score"),
        "global_summary": result.get("global_summary", "")[:300],
        "comparison_to_previous": result.get("comparison_to_previous", "")[:200],
        "active_count": len([c for c in active_conflicts if c.get("status") != "escalation_risk"]),
        "escalation_count": sum(1 for c in active_conflicts if c.get("status") == "escalation_risk"),
        "top_conflicts": [
            {
                "id": c["id"],
                "name": c["name"],
                "tier": compute_tier(c),
                "intensity": next(
                    (ca.get("intensity_score") for ca in snapshot_analyses
                     if ca.get("conflict_id") == c["id"]), None
                ),
            }
            for c in top[:5]
        ],
    }

    conclusions.append(entry)
    if len(conclusions) > 50:
        conclusions = conclusions[-50:]

    with open(CONCLUSIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(conclusions, f, indent=2, ensure_ascii=False)


def format_history_for_prompt(history: list[dict], conclusions: list[dict]) -> str:
    if not conclusions:
        return "## Previous Analyses\n\nNo previous analyses available. This is the first run.\n"

    lines = ["## Your Previous Analyses (most recent last)\n"]
    for c in conclusions[-10:]:
        lines.append(
            f"**Run #{c.get('run_number', '?')}** — {c.get('timestamp', '?')} | "
            f"Global Risk: **{c.get('global_risk_score', '?')}/100** | "
            f"Active: {c.get('active_count', '?')} conflicts | "
            f"Escalation risk: {c.get('escalation_count', '?')}"
        )
        top = c.get("top_conflicts", [])
        if top:
            lines.append("Top conflicts: " + ", ".join(
                f"{t['name']} (T{t['tier']}, intensity={t['intensity']})" for t in top
            ))
        if c.get("global_summary"):
            lines.append(f"Summary: {c['global_summary']}")
        if c.get("comparison_to_previous"):
            lines.append(f"Changed: {c['comparison_to_previous']}")
        lines.append("")

    return "\n".join(lines)


def trim_history_files():
    """Keep last 50 entries in history CSV and snapshots."""
    for path, limit, cols in [
        (HISTORY_CSV, 50, HISTORY_COLUMNS),
        (SNAPSHOTS_CSV, 500, SNAPSHOTS_COLUMNS),  # ~50 runs × ~10 conflicts
    ]:
        if not path.exists():
            continue
        rows = []
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                rows.append(row)
        if len(rows) > limit:
            rows = rows[-limit:]
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)


# ═══════════════════════════════════════════════════════════════════════════
# RSS news fetch
# ═══════════════════════════════════════════════════════════════════════════
def fetch_news(rss_feeds: list[dict]) -> str:
    sections = ["## Latest News — Geopolitical & Conflicts\n"]

    for feed in rss_feeds:
        url = feed["url"]
        name = feed.get("name", url)
        max_items = feed.get("max_items", 15)
        log.info("Fetching RSS: %s", name)

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                xml_data = resp.read()

            root = ET.fromstring(xml_data)
            items = root.findall(".//item")

            sections.append(f"### {name}\n")
            count = 0
            for item in items[:max_items]:
                title = (item.findtext("title") or "").strip()
                desc = item.findtext("description") or ""
                pub_date = (item.findtext("pubDate") or "").strip()

                desc_clean = re.sub(r"<[^>]+>", "", desc).strip()
                if len(desc_clean) > 400:
                    desc_clean = desc_clean[:400] + "..."

                sections.append(f"**{title}** ({pub_date})")
                if desc_clean:
                    sections.append(f"{desc_clean}\n")
                else:
                    sections.append("")
                count += 1

            log.info("Got %d items from %s", count, name)

        except Exception as e:
            log.warning("RSS fetch failed for %s: %s", name, e)
            sections.append(f"**{name}**: *Fetch failed — {e}*\n")

    return "\n".join(sections)


# ═══════════════════════════════════════════════════════════════════════════
# Claude analysis
# ═══════════════════════════════════════════════════════════════════════════
def run_analysis(
    conflicts_prompt: str,
    news_prompt: str,
    history_prompt: str,
    claude_cfg: dict,
    history: list[dict],
) -> tuple[dict, str, float | None]:

    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    run_number = (int(history[-1]["run_number"]) + 1) if history else 1

    time_since = "N/A (first run)"
    if history:
        try:
            last_ts = datetime.strptime(history[-1]["timestamp"], "%Y-%m-%d %H:%M:%S")
            delta = now - last_ts
            hours, remainder = divmod(int(delta.total_seconds()), 3600)
            minutes = remainder // 60
            if hours >= 24:
                days = hours // 24
                time_since = f"{days}d {hours % 24}h {minutes}m"
            else:
                time_since = f"{hours}h {minutes}m"
        except (ValueError, KeyError):
            time_since = "unknown"

    json_example = json.dumps({
        "conflicts": [
            {
                "conflict_id": "<id from known conflicts list>",
                "intensity_score": 75,
                "end_chance_1w": 5,
                "end_chance_1m": 15,
                "end_chance_1y": 40,
                "outbreak_chance_3d": 0,
                "outbreak_chance_1w": 0,
                "outbreak_chance_2w": 0,
                "civilian_casualties_est": 45000,
                "military_casualties_est": 80000,
                "summary": "One-line current status",
                "justification": "Why these scores — specific data points from news",
                "comparison_to_previous": "What changed vs last analysis",
            }
        ],
        "new_conflicts": [
            {
                "name": "Name of newly detected conflict",
                "region": "Region/Country",
                "start_date": "YYYY-MM-DD",
                "status": "started",
                "civilian_casualties_est": 0,
                "military_casualties_est": 0,
                "summary": "What triggered this conflict",
            }
        ],
        "ended_conflicts": [
            {
                "conflict_id": "<id>",
                "end_date": "YYYY-MM-DD",
                "reason": "Why conflict ended (ceasefire, treaty, etc.)",
            }
        ],
        "global_risk_score": 65,
        "global_summary": "200-300 word global geopolitical assessment",
        "comparison_to_previous": "What changed in overall geopolitical landscape vs last run",
    }, indent=2)

    prompt = f"""# War Analyser Signal — {now_str}

**Run #{run_number}** | Date: {now.strftime("%A, %B %d, %Y")} | Time since last analysis: {time_since}

{history_prompt}

{conflicts_prompt}

{news_prompt}

---

## Instructions

You are a geopolitical analyst. Analyse ALL data above.

**For each known active conflict** (status: ongoing, started, escalation_risk):
- Assess current intensity of fighting (0=no activity, 100=maximum intensity)
- Estimate probability of conflict ending in 1 week / 1 month / 1 year
- For escalation_risk conflicts: estimate probability of outbreak in 3 days / 1 week / 2 weeks
- Update casualty estimates based on news (if new data available)
- Write a justification citing specific data points from news
- Compare to your previous analysis (what changed, was your prediction correct?)

**Detect new conflicts** from the news that are NOT in the known list yet.
STRICT CRITERIA for new conflicts — ALL must be met:
- Must involve armed violence or direct military action (not diplomatic tensions, economic crises, natural disasters, single criminal incidents, political unrest, or accidents)
- Must be sustained or recurring (not a one-off event like an explosion or attack)
- Must involve at least two organized parties (state militaries, rebel groups, militias — not lone actors)
- Do NOT add: energy crises, government shutdowns, diplomatic disputes, cooperation pacts, earthquake/disaster relief, industrial strikes, single terrorist attacks, political purges
- When in doubt, do NOT add — it can be added in a future run if it escalates
- Prefer expanding an existing conflict's scope over creating a new entry (e.g., Houthi attacks are part of the Iran-Israel theater)

**Mark ended conflicts** if news confirms a conflict has ended (ceasefire, peace treaty, etc.).

**Tier guide** (for context — calculated automatically from casualties):
- Tier 1: 100,000+ total casualties
- Tier 2: 10,000-99,999
- Tier 3: 1,000-9,999
- Tier 4: 100-999
- Tier 5: <100 or unknown

**Intensity guide:**
- 0-20: minimal/frozen conflict
- 21-40: low intensity
- 41-60: moderate fighting
- 61-80: high intensity
- 81-100: full-scale war, maximum intensity

**Escalation risk scoring:**
- >50 = threat of outbreak is real, trending toward conflict
- <50 = situation de-escalating, mitigation more likely

Respond with ONLY this JSON structure (no other text):

```json
{json_example}
```

IMPORTANT:
- conflict_id must match exactly the IDs from the known conflicts list
- If no new conflicts detected, use empty array []
- If no conflicts ended, use empty array []
- justification must reference specific news items or data
- comparison_to_previous must reference your own previous scores/assessments
"""

    system_prompt = "You are a senior geopolitical analyst. You track ARMED CONFLICTS ONLY. Economic crises, diplomatic tensions, natural disasters, and domestic political events are OUT OF SCOPE even if they appear in the news feed. Be conservative — fewer high-quality conflict entries are better than many marginal ones. You ALWAYS respond with valid JSON only. Never use markdown formatting, never add text outside the JSON object. Be precise with numbers and cite sources in justifications."

    log.debug("=" * 40 + " PROMPT " + "=" * 40)
    log.debug(prompt)
    log.debug("=" * 40 + " END PROMPT " + "=" * 40)

    claude = ClaudeCode(
        model=claude_cfg.get("model"),
        system_prompt=system_prompt,
        timeout=claude_cfg.get("timeout", 300),
        max_budget_usd=claude_cfg.get("max_budget_usd", 1.0),
    )

    resp = claude.ask(prompt)
    model = resp.model or claude_cfg.get("model") or "default"
    cost = resp.cost_usd

    log.debug("=" * 40 + " RAW RESPONSE " + "=" * 40)
    log.debug("resp.text: %s", resp.text)
    log.debug("resp.model: %s | resp.cost_usd: %s", resp.model, resp.cost_usd)
    log.debug("=" * 40 + " END RESPONSE " + "=" * 40)

    result = _parse_json_response(resp)
    return result, model, cost


def _parse_json_response(resp) -> dict:
    if resp.raw_json and "result" in resp.raw_json:
        raw = resp.raw_json["result"]
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                pass
            parsed = _extract_json_from_text(raw)
            if parsed:
                return parsed

    text = resp.text or ""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    parsed = _extract_json_from_text(text)
    if parsed:
        return parsed

    return {"raw_text": text}


def _extract_json_from_text(text: str) -> dict | None:
    m = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    first_brace = text.find("{")
    last_brace = text.rfind("}")
    if first_brace != -1 and last_brace > first_brace:
        candidate = text[first_brace:last_brace + 1]
        try:
            parsed = json.loads(candidate)
            if "conflicts" in parsed or "global_risk_score" in parsed:
                return parsed
        except json.JSONDecodeError:
            pass

    return None


# ═══════════════════════════════════════════════════════════════════════════
# Discord message building
# ═══════════════════════════════════════════════════════════════════════════
def intensity_bar(score) -> str:
    if not isinstance(score, int):
        return "?" * 10
    filled = round(score / 10)
    return "\u2588" * filled + "\u2591" * (10 - filled)


def intensity_emoji(score) -> str:
    if not isinstance(score, int):
        return "\u2753"
    if score >= 81: return "\U0001f534"   # red
    if score >= 61: return "\U0001f7e0"   # orange
    if score >= 41: return "\U0001f7e1"   # yellow
    if score >= 21: return "\U0001f7e2"   # green
    return "\u26aa"                        # grey


def risk_emoji(score) -> str:
    if not isinstance(score, int):
        return "\u2753"
    if score >= 75: return "\U0001f6a8"   # alarm
    if score >= 50: return "\u26a0\ufe0f" # warning
    if score >= 25: return "\U0001f4ca"   # chart
    return "\u2705"                        # check


def tier_badge(tier: int) -> str:
    badges = {1: "\U0001f3c6 T1", 2: "\U0001f948 T2", 3: "\U0001f949 T3", 4: "T4", 5: "T5"}
    return badges.get(tier, "T?")


def build_discord_message(
    result: dict,
    timestamp: str,
    run_number: int,
    active_conflicts: list[dict],
    snapshot_analyses: list[dict],
    history: list[dict],
) -> str:
    global_risk = result.get("global_risk_score", "?")
    global_summary = result.get("global_summary", "")
    comparison = result.get("comparison_to_previous", "")

    # Separate active vs escalation_risk
    active = [c for c in active_conflicts if c.get("status") in {"ongoing", "started"}]
    escalation = [c for c in active_conflicts if c.get("status") == "escalation_risk"]

    # Build analyses lookup
    analyses_by_id = {ca.get("conflict_id"): ca for ca in snapshot_analyses}

    # Only show conflicts that have been scored (skip newly detected ones)
    active_sorted = [c for c in sort_conflicts_by_tier(active) if c["id"] in analyses_by_id]
    escalation_sorted = [c for c in sort_conflicts_by_tier(escalation) if c["id"] in analyses_by_id]

    # Header
    lines = [
        f"\U0001f30d **WAR ANALYSER** \u2014 Run #{run_number} \u2014 {timestamp}",
        f"{risk_emoji(global_risk)} **Global Risk: {global_risk}/100**",
        "",
    ]

    # Active conflicts section
    if active_sorted:
        lines.append(f"\u2694\ufe0f **ACTIVE CONFLICTS ({len(active_sorted)})**")
        lines.append("\u2015" * 38)
        for c in active_sorted:
            tier = compute_tier(c)
            ca = analyses_by_id.get(c["id"], {})
            intensity = ca.get("intensity_score", "?")
            civ = ca.get("civilian_casualties_est") or c.get("civilian_casualties_est", "?")
            mil = ca.get("military_casualties_est") or c.get("military_casualties_est", "?")
            end_1w = ca.get("end_chance_1w", "?")
            end_1m = ca.get("end_chance_1m", "?")
            end_1y = ca.get("end_chance_1y", "?")
            summary = ca.get("summary", "")
            justification = ca.get("justification", "")
            comparison_prev = ca.get("comparison_to_previous", "")

            civ_fmt = f"{civ:,}" if isinstance(civ, int) else str(civ)
            mil_fmt = f"{mil:,}" if isinstance(mil, int) else str(mil)

            lines.append(
                f"\n{intensity_emoji(intensity)} **{c['name']}** \u00b7 {tier_badge(tier)} \u00b7 {c.get('region', '?')}"
            )
            lines.append(f"`{intensity_bar(intensity)}` **{intensity}/100**")
            lines.append(
                f"\U0001f464 {civ_fmt} civ \u00b7 \U0001fa96 {mil_fmt} mil "
                f"\u00b7 End: 1w={end_1w}% \u00b7 1m={end_1m}% \u00b7 1y={end_1y}%"
            )
            if summary:
                lines.append(f"_{summary}_")
            if justification:
                lines.append(f"> {justification}")
            if comparison_prev:
                lines.append(f"\U0001f504 _{comparison_prev}_")

    # Escalation risk section
    if escalation_sorted:
        lines.append(f"\n\u26a0\ufe0f **ESCALATION RISKS ({len(escalation_sorted)})**")
        lines.append("\u2015" * 38)
        for c in escalation_sorted:
            ca = analyses_by_id.get(c["id"], {})
            ob_3d = ca.get("outbreak_chance_3d", "?")
            ob_1w = ca.get("outbreak_chance_1w", "?")
            ob_2w = ca.get("outbreak_chance_2w", "?")
            summary = ca.get("summary", "")
            justification = ca.get("justification", "")

            max_ob = max(
                v for v in [ob_3d, ob_1w, ob_2w] if isinstance(v, int)
            ) if any(isinstance(v, int) for v in [ob_3d, ob_1w, ob_2w]) else 0

            if max_ob >= 50:
                framing = "\U0001f534 **ZAGROŻENIE WYBUCHEM**"
            else:
                framing = "\U0001f7e2 De-eskalacja"

            lines.append(f"\n\U0001f30f **{c['name']}** \u00b7 {c.get('region', '?')} \u2014 {framing}")
            lines.append(f"Outbreak: 3d={ob_3d}% \u00b7 1w={ob_1w}% \u00b7 2w={ob_2w}%")
            if summary:
                lines.append(f"_{summary}_")
            if justification:
                lines.append(f"> {justification}")

    if not active_sorted and not escalation_sorted:
        new_count = len([c for c in active_conflicts if c["id"] not in analyses_by_id])
        if new_count:
            lines.append(f"_Wykryto {new_count} nowych konfliktów — scores dostępne od następnego runu._")
        else:
            lines.append("_Brak aktywnych konfliktów._")

    # Global summary
    if global_summary:
        lines.append("\n" + "\u2015" * 38)
        lines.append(f"\U0001f4cb **Global Assessment**")
        lines.append(global_summary)

    if comparison:
        lines.append(f"\n\U0001f504 **vs Run #{run_number - 1}:** {comparison}")

    lines.append("\n" + "\u2015" * 38)
    lines.append("_Not investment or political advice. Automated AI-generated geopolitical analysis._")

    return "\n".join(lines).strip()


def build_fallback_discord(raw_text: str, timestamp: str, run_number: int) -> str:
    return (
        f"\u2753 **WAR ANALYSER** \u2502 Run #{run_number} \u2502 {timestamp}\n"
        + "\u2550" * 40 + "\n\n"
        + "\U0001f4dd **Analysis (unstructured):**\n"
        + f"{(raw_text or 'Analysis unavailable.')[:1500]}\n"
        + "\u2500" * 40 + "\n"
        + "_This is not advice. Automated AI-generated geopolitical analysis._"
    )


# ═══════════════════════════════════════════════════════════════════════════
# Notification logic
# ═══════════════════════════════════════════════════════════════════════════
def _load_snapshot_for_run(run_id: str) -> dict[str, int]:
    """Load intensity scores from snapshot CSV for a given run_id. Returns {conflict_id: intensity}."""
    if not SNAPSHOTS_CSV.exists():
        return {}
    scores = {}
    with open(SNAPSHOTS_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("run_id") == run_id:
                try:
                    scores[row["conflict_id"]] = int(row["intensity_score"])
                except (ValueError, KeyError):
                    pass
    return scores


def should_notify(history: list[dict], result: dict) -> tuple[bool, str]:
    if not history:
        return True, "first run"

    prev = history[-1]

    try:
        last_ts = datetime.strptime(prev["timestamp"], "%Y-%m-%d %H:%M:%S")
        elapsed = datetime.now() - last_ts
        if elapsed.total_seconds() >= 7 * 24 * 3600:
            return True, f"weekly summary ({elapsed.days}d since last)"
    except (ValueError, KeyError):
        return True, "unable to determine last run time"

    # New conflicts detected
    if result.get("new_conflicts"):
        return True, f"new conflicts detected: {[c['name'] for c in result['new_conflicts']]}"

    # Conflicts ended
    if result.get("ended_conflicts"):
        return True, f"conflicts ended: {[c['conflict_id'] for c in result['ended_conflicts']]}"

    # Global risk score shifted by 10+
    try:
        prev_risk = int(prev.get("global_risk_score", 0))
        new_risk = int(result.get("global_risk_score", 0))
        if abs(new_risk - prev_risk) >= 10:
            return True, f"global risk shifted {abs(new_risk - prev_risk)} pts ({prev_risk} -> {new_risk})"
    except (ValueError, TypeError):
        pass

    # Per-conflict intensity shift >=15 vs previous run
    new_conflicts = {c["conflict_id"]: c for c in result.get("conflicts", [])}
    prev_scores = _load_snapshot_for_run(prev.get("run_id", ""))
    if prev_scores and new_conflicts:
        for cid, analysis in new_conflicts.items():
            try:
                new_i = int(analysis.get("intensity_score", 0))
                old_i = prev_scores.get(cid)
                if old_i is not None and abs(new_i - old_i) >= 15:
                    name = analysis.get("summary", cid)[:40]
                    return True, f"conflict intensity shifted {abs(new_i - old_i)} pts: {name} ({old_i} -> {new_i})"
            except (ValueError, TypeError):
                pass

    # Gradual drift: compare vs last *notified* run (non-empty discord_message)
    last_notified = None
    for h in reversed(history):
        if h.get("discord_message"):
            last_notified = h
            break

    if last_notified and last_notified is not prev:
        # Global risk drift vs last notified
        try:
            notified_risk = int(last_notified.get("global_risk_score", 0))
            new_risk = int(result.get("global_risk_score", 0))
            if abs(new_risk - notified_risk) >= 10:
                return True, f"global risk drifted {abs(new_risk - notified_risk)} pts since last notification ({notified_risk} -> {new_risk})"
        except (ValueError, TypeError):
            pass

        # Per-conflict intensity drift vs last notified
        notified_scores = _load_snapshot_for_run(last_notified.get("run_id", ""))
        if notified_scores and new_conflicts:
            for cid, analysis in new_conflicts.items():
                try:
                    new_i = int(analysis.get("intensity_score", 0))
                    notified_i = notified_scores.get(cid)
                    if notified_i is not None and abs(new_i - notified_i) >= 15:
                        name = analysis.get("summary", cid)[:40]
                        return True, f"conflict intensity drifted {abs(new_i - notified_i)} pts since last notification: {name} ({notified_i} -> {new_i})"
                except (ValueError, TypeError):
                    pass

    return False, "no significant change"


def translate_to_polish(english_msg: str, claude_cfg: dict) -> str:
    system = "You are a translator. Translate the text to Polish. Keep ALL formatting: emoji, bold (**), lines (│ ═ ─), bars (█ ░). Translate ONLY text — never change numbers, names, dates, or formatting characters. Return ONLY the translated text."
    prompt = f"Translate to Polish. Keep exact formatting:\n\n{english_msg}"
    claude = ClaudeCode(
        system_prompt=system,
        timeout=claude_cfg.get("timeout", 300),
        max_budget_usd=0.60,
    )
    resp = claude.ask(prompt)
    text = resp.text.strip() if resp.text else ""
    if not text or text.startswith("{") or "error" in text[:50].lower():
        log.warning("Translation failed, sending English only.")
        return ""
    return text


def send_discord(config: dict, message: str, webhook_url: str | None = None):
    discord_cfg = config.get("discord", {})
    url = webhook_url or discord_cfg.get("webhook_url")
    if not discord_cfg.get("active") or not url:
        log.info("Discord not configured — skipping.")
        return
    notifier = DiscordNotifier(webhook_url=url, active=True)
    notifier.send_sync(message)
    log.info("Discord notification sent to %s.", url[-20:])


# ═══════════════════════════════════════════════════════════════════════════
# Main pipeline
# ═══════════════════════════════════════════════════════════════════════════
def main():
    setup_logging()
    log.info("=" * 60)
    log.info("War Analyser Signal — Starting analysis pipeline")
    log.info("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Load config
    log.info("[1/7] Loading config...")
    config = load_config()
    analysis_cfg = config.get("analysis", {})
    claude_cfg = analysis_cfg.get("claude", {})
    rss_feeds = analysis_cfg.get("rss_feeds", [])
    lookback = analysis_cfg.get("history_lookback", 10)

    run_id = str(uuid.uuid4())[:8]
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log.info("Run ID: %s | Timestamp: %s", run_id, timestamp)

    # Step 2: Load conflicts + history
    log.info("[2/7] Loading conflicts and history...")
    conflicts = load_conflicts()
    history = load_history(lookback)
    conclusions = load_conclusions(lookback)
    run_number = int(history[-1]["run_number"]) + 1 if history else 1
    active_conflicts = get_active_conflicts(conflicts)

    conflicts_prompt = format_conflicts_for_prompt(conflicts)
    history_prompt = format_history_for_prompt(history, conclusions)

    log.info("Conflicts: %d total, %d active", len(conflicts), len(active_conflicts))
    log.info("History: %d previous analyses", len(history))

    # Step 3: Fetch news
    log.info("[3/7] Fetching news from RSS...")
    news_prompt = fetch_news(rss_feeds)

    # Step 4: Run Claude analysis
    log.info("[4/7] Running Claude analysis...")
    result, model, cost = run_analysis(
        conflicts_prompt, news_prompt, history_prompt, claude_cfg, history
    )

    is_structured = "conflicts" in result or "global_risk_score" in result

    if is_structured:
        log.info("Analysis complete. Model: %s | Cost: $%s", model, cost or "?")
        log.info("Global risk: %s | New conflicts: %d | Ended: %d",
                 result.get("global_risk_score"),
                 len(result.get("new_conflicts", [])),
                 len(result.get("ended_conflicts", [])))
    else:
        log.warning("Claude returned raw text instead of structured JSON.")

    # Step 5: Update conflicts CSV
    log.info("[5/7] Updating conflicts database...")
    if is_structured:
        conflicts = apply_analysis_to_conflicts(conflicts, result, timestamp)
        save_conflicts(conflicts)
        active_conflicts = get_active_conflicts(conflicts)
        snapshot_analyses = result.get("conflicts", [])
        log.info("Conflicts updated. Active: %d", len(active_conflicts))
    else:
        snapshot_analyses = []

    # Step 6: Should notify?
    log.info("[6/7] Evaluating significance...")

    discord_msg = ""

    if is_structured:
        save_snapshot(run_id, timestamp, snapshot_analyses, active_conflicts)
        notify, notify_reason = should_notify(history, result)
    else:
        # Analysis failed (timeout/error) — don't trigger false notification
        notify = False
        notify_reason = "analysis failed (unstructured response)"

    webhook_pl = config.get("discord", {}).get("webhook_url_pl")

    if not notify:
        log.info("SKIP — %s", notify_reason)
        if is_structured:
            save_history(result, run_number, run_id, timestamp,
                         active_conflicts, snapshot_analyses, model, cost, "")
            save_conclusion(result, run_number, timestamp, active_conflicts, snapshot_analyses)
        trim_history_files()
        # Short diagnostic
        risk = result.get("global_risk_score", "?") if is_structured else "?"
        raw_reason = result.get("raw_text", "")[:100] if not is_structured else ""
        diag_en = (
            f"\U0001f504 **War Check** \u2502 Run #{run_number} \u2502 {timestamp}\n"
            f"_{notify_reason} \u2014 no update sent_"
            + (f" | Global risk: {risk}/100" if is_structured else f" | Error: {raw_reason}")
        )
        send_discord(config, diag_en)

        if webhook_pl:
            reason_pl_map = {
                "no significant change": "brak istotnych zmian",
                "analysis failed (unstructured response)": "analiza nieudana (niestrukturalna odpowiedź)",
            }
            reason_pl = reason_pl_map.get(notify_reason, notify_reason)
            diag_pl = (
                f"\U0001f504 **Sprawdzenie wojny** \u2502 Run #{run_number} \u2502 {timestamp}\n"
                f"_{reason_pl} \u2014 brak aktualizacji_"
                + (f" | Ryzyko globalne: {risk}/100" if is_structured else f" | Błąd: {raw_reason}")
            )
            send_discord(config, diag_pl, webhook_url=webhook_pl)

        log.info("Pipeline complete (%s).", notify_reason)
        return

    log.info("NOTIFY — %s", notify_reason)

    # Step 7: Build message, translate, send
    log.info("[7/7] Building message and sending...")

    discord_msg_en = build_discord_message(
        result, timestamp, run_number, active_conflicts, snapshot_analyses, history
    )

    save_history(result, run_number, run_id, timestamp,
                 active_conflicts, snapshot_analyses, model, cost, discord_msg_en)
    save_conclusion(result, run_number, timestamp, active_conflicts, snapshot_analyses)
    trim_history_files()

    # Send EN
    log.info("\u2500" * 60)
    log.info("Discord EN message:\n%s", discord_msg_en)
    log.info("\u2500" * 60)
    send_discord(config, discord_msg_en)

    # Translate and send PL
    if webhook_pl:
        log.info("Translating to Polish...")
        discord_msg_pl = translate_to_polish(discord_msg_en, claude_cfg)
        if discord_msg_pl:
            log.info("Translation complete.")
            send_discord(config, discord_msg_pl, webhook_url=webhook_pl)
        else:
            log.warning("Translation failed — PL channel skipped.")

    log.info("Pipeline complete.")


if __name__ == "__main__":
    main()
else:
    main()
