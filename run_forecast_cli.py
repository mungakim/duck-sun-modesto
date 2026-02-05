"""
Standalone forecast runner for PyInstaller exe.
This script runs the forecast and opens the Excel report.
Git operations are skipped (not needed for coworkers).
"""
import os
import sys
import traceback
from pathlib import Path

def main():
    print("=" * 50)
    print("  Duck Sun Modesto - Daily Forecast")
    print("=" * 50)
    print()

    # Get the directory where the exe/script is located
    if getattr(sys, 'frozen', False):
        # Running as compiled exe
        script_dir = Path(sys.executable).parent
    else:
        # Running as script
        script_dir = Path(__file__).parent

    os.chdir(script_dir)
    print(f"Working directory: {script_dir}")
    print()

    # Run the forecast
    print("Running forecast...")
    print("-" * 50)

    try:
        from duck_sun.scheduler import main as run_scheduler
        result = run_scheduler()

        if result != 0:
            print()
            print("[ERROR] Forecast returned non-zero exit code:", result)
            input("Press Enter to exit...")
            return 1

    except Exception as e:
        print()
        print("=" * 50)
        print("[ERROR] Forecast failed with exception:")
        print("=" * 50)
        traceback.print_exc()
        print()
        print(f"Error: {e}")
        print()
        input("Press Enter to exit...")
        return 1

    print()
    print("Opening Excel report...")

    # Find and open the latest xlsx file
    reports_dir = script_dir / "reports"
    xlsx_files = list(reports_dir.rglob("*.xlsx"))

    if xlsx_files:
        latest = max(xlsx_files, key=lambda f: f.stat().st_mtime)
        print(f"Opening: {latest}")
        os.startfile(str(latest))
    else:
        print("No xlsx file found")

    print()
    print("=" * 50)
    print("  Done!")
    print("=" * 50)

    # Brief pause so user can see the output
    import time
    time.sleep(3)

    return 0

if __name__ == "__main__":
    sys.exit(main())
