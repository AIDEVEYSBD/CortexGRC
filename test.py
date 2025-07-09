#!/usr/bin/env python3
"""
CortexGRC Framework Mapping Test Script – revamped selection UX
================================================================

This script lets you compare two cybersecurity frameworks end‑to‑end.
Key improvements in this version:
1. **Numbered menu** – Frameworks are shown once with numbers (1, 2, …). Enter the number to select.
2. **Single Supabase call for control counts** – avoids extra round‑trips.
3. **Clearer prompts & validation** – Prevents duplicate selection and bad input.

Requirements:
    pip install requests python-dotenv supabase

Environment Variables Required:
    SUPABASE_URL=your_supabase_url
    SUPABASE_ANON_KEY=your_supabase_anon_key
    CORTEXGRC_URL=http://localhost:8003  # Optional, defaults to localhost

Usage:
    python test_framework_mapping.py
"""

import os
import time
import sys
from typing import List, Dict, Optional
import json

try:
    import requests
    from supabase import create_client, Client
    from dotenv import load_dotenv
except ImportError:
    print("❌ Missing required packages. Please install:")
    print("pip install requests python-dotenv supabase")
    sys.exit(1)

# Load environment variables
load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
CORTEXGRC_URL = os.getenv("CORTEXGRC_URL", "http://localhost:8003")

# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

def init_supabase() -> Client:
    """Create a Supabase client or terminate with an error."""
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        print("❌ Missing SUPABASE_URL or SUPABASE_ANON_KEY in environment")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)


def fetch_frameworks_with_counts(supabase: Client) -> List[Dict]:
    """Fetch frameworks along with their control counts (one round‑trip each)."""
    print("📋 Fetching frameworks …", end="", flush=True)
    frameworks = supabase.table("frameworks").select("*", count="exact").execute().data
    if not frameworks:
        print("\n❌ No frameworks found.")
        sys.exit(1)

    # Pre‑fetch control counts in a single query
    controls = supabase.table("controls").select("framework_id", count="exact").execute().data
    control_count_map = {}
    for ctrl in controls:
        fid = ctrl.get("framework_id")
        control_count_map[fid] = control_count_map.get(fid, 0) + 1

    for fw in frameworks:
        fw["controls_count"] = control_count_map.get(fw["id"], 0)
    print(f" done (found {len(frameworks)}).")
    return frameworks

# ---------------------------------------------------------------------------
# User‑interaction helpers
# ---------------------------------------------------------------------------

def show_numbered_menu(frameworks: List[Dict]):
    """Print a nice numbered list of frameworks."""
    print("\n📚 Available Frameworks")
    print("=" * 23)
    for idx, fw in enumerate(frameworks, 1):
        name = fw.get("name", "Unknown")
        version = fw.get("version", "N/A")
        cnt = fw.get("controls_count", "?")
        print(f" {idx:2d}. {name} (v{version}) – {cnt} controls")


def get_selection(frameworks: List[Dict], prompt_msg: str) -> Dict:
    """Return a framework selected by number (1‑N)."""
    while True:
        try:
            choice = input(f"\n{prompt_msg} (1‑{len(frameworks)}): ").strip()
            idx = int(choice) - 1
            if 0 <= idx < len(frameworks):
                return frameworks[idx]
            print(f"❌ Enter a number between 1 and {len(frameworks)}")
        except ValueError:
            print("❌ Please enter a valid integer")
        except KeyboardInterrupt:
            print("\n👋 Exiting…")
            sys.exit(0)

# ---------------------------------------------------------------------------
# CortexGRC helpers
# ---------------------------------------------------------------------------

def check_cortexgrc_health() -> bool:
    """Verify that the CortexGRC service is reachable and healthy."""
    print(f"🔍 Checking CortexGRC at {CORTEXGRC_URL} …", end="", flush=True)
    try:
        resp = requests.get(f"{CORTEXGRC_URL}/health", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            print(" healthy ✓")
            print(f"   Version: {data.get('version', 'Unknown')}")
            return True
        print(f" failed (status {resp.status_code})")
        return False
    except requests.exceptions.RequestException as e:
        print(f" failed ({e})")
        return False


def submit_framework_comparison(src: Dict, tgt: Dict, email: str) -> Optional[str]:
    """Kick off a comparison job and return its job‑ID."""
    print("\n🚀 Submitting comparison job …")
    form = {
        "source_framework_name": src["name"],
        "target_framework_name": tgt["name"],
        "user_email": email,
        "top_k": 5,
        "generate_excel": True,
    }
    try:
        resp = requests.post(f"{CORTEXGRC_URL}/framework/compare", data=form, timeout=30)
        if resp.status_code == 200:
            job_id = resp.json().get("job_id")
            print(f"✅ Job accepted, ID: {job_id}")
            return job_id
        print(f"❌ Submit failed (status {resp.status_code}) – {resp.text}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Submit error: {e}")
    return None


def poll_job(job_id: str) -> Dict:
    """Poll the job until completion/ failure / interrupt."""
    print("\n⏳ Polling job status – Ctrl‑C to stop")
    start = time.time()
    last = None
    try:
        while True:
            try:
                resp = requests.get(f"{CORTEXGRC_URL}/jobs/{job_id}", timeout=30)
                data = resp.json() if resp.status_code == 200 else {}
                status = data.get("status", "unknown")

                if status != last:
                    print(f"   {status} (elapsed {int(time.time()-start)}s)")
                    last = status

                if status in {"completed", "failed"}:
                    return data
                time.sleep(30)
            except requests.exceptions.RequestException:
                print("⚠️  Temporary connection issue; retrying in 30s …")
                time.sleep(30)
    except KeyboardInterrupt:
        print("\n⏸️ Polling interrupted by user. You can query the job later.")
        return {"status": "interrupted", "job_id": job_id}

# ---------------------------------------------------------------------------
# Results display
# ---------------------------------------------------------------------------

def show_results(job: Dict):
    print("\n📊 Job Summary")
    print("=" * 10)
    print(json.dumps(job, indent=2))
    if job.get("status") == "completed":
        jid = job.get("job_id")
        print(f"\n💾 Download: {CORTEXGRC_URL}/jobs/{jid}/download")
        print(f"🔍 Debug:    {CORTEXGRC_URL}/jobs/{jid}/debug")

# ---------------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------------

def main():
    print("🎯 CortexGRC Framework Mapping Test Script")
    print("=" * 50)

    if not check_cortexgrc_health():
        sys.exit(1)

    supabase = init_supabase()
    frameworks = fetch_frameworks_with_counts(supabase)

    # Display once
    show_numbered_menu(frameworks)

    # Source selection
    src = get_selection(frameworks, "Select **source** framework number")

    # Target selection – remove chosen src
    remaining = [f for f in frameworks if f["id"] != src["id"]]
    show_numbered_menu(remaining)
    tgt = get_selection(remaining, "Select **target** framework number")

    # Email
    while True:
        email = input("\n📧 Your email: ").strip()
        if "@" in email:
            break
        print("❌ Enter a valid email address.")

    print("\n🔍 Summary")
    print("-" * 8)
    print(f"Source: {src['name']}")
    print(f"Target: {tgt['name']}")
    print(f"Email:  {email}")
    print("⚠️ This can take 30 min – 2 h.")

    if input("Proceed? (y/N): ").strip().lower() != "y":
        print("👋 Cancelled.")
        sys.exit(0)

    job_id = submit_framework_comparison(src, tgt, email)
    if not job_id:
        sys.exit(1)

    job_data = poll_job(job_id)
    show_results(job_data)
    print("\n✅ Done!")

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(f"\n❌ Unexpected error: {err}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
