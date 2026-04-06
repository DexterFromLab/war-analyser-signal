#!/usr/bin/env python3
"""
Send errata to Discord #errata channel.

Usage:
    python send_errata.py --date 2026-04-06 \
        --scope war \
        --changes "Zmiana 1" "Zmiana 2" \
        --effects "Efekt 1" "Efekt 2" \
        --commits 214474f 28fcb1e
"""
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path.home() / ".local/share/claude-code-ide"))
from discord_notifier import DiscordNotifier

ERRATA_WEBHOOK = (
    "https://discord.com/api/webhooks/1488966638421413898/"
    "PyRULLmqaSkE4OZuZShJHZwqL0hhaf9u8VzY3TpBT-ixF3YKTyboEGGIPFHgYCoun5gW"
)


def build_message(date: str, scope: str, changes: list[str],
                   effects: list[str], commits: list[str]) -> str:
    sep = chr(9552) * 40
    line = chr(9472) * 40

    changes_block = "\n".join(f"\u2502 \u2022 {c}" for c in changes)
    effects_block = "\n".join(f"\u2502 \u2022 {e}" for e in effects)
    commits_str = ", ".join(commits)

    return (
        f"\U0001f527 **Errata** | {date}\n"
        f"{sep}\n\n"
        f"\U0001f4cb **Zmiany:**\n"
        f"{changes_block}\n"
        f"\n\U0001f3af **Dotyczy:** {scope}\n\n"
        f"\u26a1 **Co si\u0119 zmienia:**\n"
        f"{effects_block}\n"
        f"\n\U0001f517 **Commity:** {commits_str}\n"
        f"{line}"
    )


def main():
    parser = argparse.ArgumentParser(description="Send errata to Discord")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--scope", required=True, help="stock / forex / war")
    parser.add_argument("--changes", nargs="+", required=True)
    parser.add_argument("--effects", nargs="+", required=True)
    parser.add_argument("--commits", nargs="+", required=True)
    parser.add_argument("--dry-run", action="store_true", help="Print message without sending")
    args = parser.parse_args()

    msg = build_message(args.date, args.scope, args.changes, args.effects, args.commits)

    if args.dry_run:
        print(msg)
        return

    notifier = DiscordNotifier(webhook_url=ERRATA_WEBHOOK, active=True)
    notifier.send_sync(msg)
    print("Errata sent.")


if __name__ == "__main__":
    main()
