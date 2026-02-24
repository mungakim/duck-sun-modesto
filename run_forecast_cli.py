"""
Standalone forecast runner for PyInstaller exe.
This script runs the forecast and opens the Excel report.
Git operations are skipped (not needed for coworkers).
"""
import asyncio
import os
import sys
import traceback
from pathlib import Path

# PyInstaller hidden imports - these ensure all provider modules are bundled
# into the exe even though the main import chain is deferred inside main().
# Without these, PyInstaller's static analysis may miss submodules imported
# via duck_sun.providers.__init__.py.
import duck_sun.providers.google_weather  # noqa: F401
import duck_sun.providers.weather_com  # noqa: F401
import duck_sun.providers.wunderground  # noqa: F401
import duck_sun.providers.mid_org  # noqa: F401
import duck_sun.providers.metar  # noqa: F401
import duck_sun.providers.noaa  # noqa: F401
import duck_sun.providers.met_no  # noqa: F401
import duck_sun.providers.accuweather  # noqa: F401
import duck_sun.providers.open_meteo  # noqa: F401
import duck_sun.ssl_helper  # noqa: F401
import duck_sun.resilience  # noqa: F401
import duck_sun.cache_manager  # noqa: F401
import duck_sun.uncanniness  # noqa: F401
import duck_sun.excel_report  # noqa: F401

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
        # scheduler.main() is async, so we need asyncio.run()
        result = asyncio.run(run_scheduler())

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

    # Keep terminal open so user can review output and any errors
    print()
    input("Press Enter to close...")

    return 0

if __name__ == "__main__":
    sys.exit(main())
