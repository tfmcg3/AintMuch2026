"""
Check Secrets: Verify Replit Environment Variables

Safely checks for the presence of environment variables needed
by the project. Never prints actual secret values — only reports
whether each variable is set or missing.
"""

import os
import sys

REQUIRED_SECRETS = [
    ("APIFY_TOKEN", "Apify API token for deploying and running actors"),
]

OPTIONAL_SECRETS = [
    ("SESSION_SECRET", "Session secret for secure cookies"),
    ("PROXY_URL", "Custom proxy URL for requests"),
]


def check_env(name: str) -> bool:
    value = os.environ.get(name)
    return value is not None and len(value.strip()) > 0


def run_check():
    print("=" * 50)
    print("  ENVIRONMENT SECRETS CHECK")
    print("=" * 50)
    print()

    all_ok = True

    print("Required Variables:")
    print("-" * 40)
    for name, description in REQUIRED_SECRETS:
        found = check_env(name)
        status = "SET" if found else "MISSING"
        icon = "+" if found else "!"
        print(f"  [{icon}] {name}: {status}")
        print(f"      {description}")
        if not found:
            all_ok = False
    print()

    print("Optional Variables:")
    print("-" * 40)
    for name, description in OPTIONAL_SECRETS:
        found = check_env(name)
        status = "SET" if found else "NOT SET"
        icon = "+" if found else "-"
        print(f"  [{icon}] {name}: {status}")
        print(f"      {description}")
    print()

    print("=" * 50)
    print(f"  Replit Environment: {'DETECTED' if os.environ.get('REPL_ID') else 'NOT DETECTED'}")

    if all_ok:
        print("  STATUS: All required secrets are configured")
    else:
        print("  STATUS: Some required secrets are missing!")
        print("  TIP: Add them via the Secrets tab in Replit")
    print("=" * 50)

    return all_ok


if __name__ == "__main__":
    success = run_check()
    sys.exit(0 if success else 1)
