# run_newsletter.py
import argparse
import sys
import os
import traceback
from datetime import datetime
from pathlib import Path

import boto3

# Local imports
import main_scraper.scraper as scraper
import email_writer
import subprocess  # NEW: to run api_harvester before scraper

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

def log_path() -> Path:
    ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    return LOG_DIR / f"run_newsletter_{ts}.log"

def log(msg: str, file):
    stamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{stamp} {msg}", flush=True)
    try:
        file.write(f"{stamp} {msg}\n")
        file.flush()
        os.fsync(file.fileno())
    except Exception:
        pass

def stop_instance(instance_id: str, region: str, logf):
    try:
        ec2 = boto3.client("ec2", region_name=region)
        ec2.stop_instances(InstanceIds=[instance_id])
        log(f"Requested stop for instance {instance_id} in {region}.", logf)
    except Exception as e:
        log(f"ERROR stopping instance: {e}", logf)

def run_api_harvester(logf):
    """Run API harvester before scraper to fetch URLs + filter via GPT."""
    try:
        log("Running api_harvester.py…", logf)
        result = subprocess.run(
            [sys.executable, str(BASE_DIR / "api_harvester.py")],
            cwd=BASE_DIR
        )
        if result.returncode != 0:
            raise RuntimeError("api_harvester.py failed")
        log("api_harvester.py completed successfully.", logf)
    except Exception as e:
        log(f"ERROR running api_harvester.py: {e}", logf)
        raise

def main():
    ap = argparse.ArgumentParser(description="Run scraper + newsletter orchestrator.")
    ap.add_argument("--shutdown-on-success", action="store_true",
                    help="Stop the EC2 instance only if all steps succeed.")
    ap.add_argument("--always-shutdown", action="store_true",
                    help="Always stop the instance at the end (even on failure).")
    ap.add_argument("--instance-id", default=os.environ.get("INSTANCE_ID", ""),
                    help="EC2 instance ID (or set env INSTANCE_ID).")
    ap.add_argument("--region", default=os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "")),
                    help="AWS region (or set env AWS_REGION/AWS_DEFAULT_REGION).")
    args = ap.parse_args()

    lp = log_path()
    with open(lp, "a", encoding="utf-8") as logf:
        log(f"=== Run start; cwd={os.getcwd()} base={BASE_DIR}", logf)
        ok = False
        try:
            # Ensure we run relative paths from repo root
            os.chdir(BASE_DIR)

            # Step 1: Run API harvester
            run_api_harvester(logf)

            # Step 2: Run scraper (now uses api_urls.json)
            log("Running scraper.main()…", logf)
            scraper.main()

            # Step 3: Run email writer
            log("Running email_writer.main()…", logf)
            email_writer.main()

            ok = True
            log("All steps completed successfully.", logf)

        except Exception as e:
            log("FATAL error during newsletter run:", logf)
            tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            for line in tb.rstrip().splitlines():
                log(line, logf)

        finally:
            # Decide shutdown behavior
            do_shutdown = False
            if args.always_shutdown:
                do_shutdown = True
                reason = "always-shutdown flag set"
            elif args.shutdown_on_success and ok:
                do_shutdown = True
                reason = "shutdown-on-success and run succeeded"
            else:
                reason = "no shutdown requested"

            log(f"Shutdown decision: {do_shutdown} ({reason})", logf)

            if do_shutdown:
                instance_id = args.instance_id
                region = args.region
                if not instance_id or not region:
                    log("Missing instance-id or region; skipping shutdown.", logf)
                else:
                    stop_instance(instance_id, region, logf)

            log(f"Log written to {lp}", logf)

if __name__ == "__main__":
    sys.exit(main() or 0)
