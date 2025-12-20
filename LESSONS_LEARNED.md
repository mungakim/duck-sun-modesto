# Lessons Learned - Duck Sun Modesto

Knowledge base for preventing repeated mistakes and amplifying successful patterns.

---

## Recent Obstacles

### [2025-12-19] OBSTACLE: WSL Cannot Activate Windows Python Virtual Environments

**Context:** Attempting to run the Duck Sun scheduler from WSL bash shell in a project located on Windows filesystem (`/mnt/c/Professional Projects/duck-sun-modesto`)

**Symptom/Challenge:** Multiple failed attempts to launch Python:
1. `python -m duck_sun.scheduler` → `python: command not found`
2. `python3 -m duck_sun.scheduler` → `ModuleNotFoundError: No module named 'dotenv'` (system Python lacks dependencies)
3. `source venv/bin/activate` → `No such file or directory` (looking for Linux venv structure)
4. `source ./venv/Scripts/activate && python` → Activation appeared to work but `python` command still not found in PATH

**Root Cause:** Cross-platform virtual environment incompatibility
- Project filesystem is on Windows (`/mnt/c/...`) accessed via WSL mount
- Virtual environment was created using Windows Python (`python -m venv venv`)
- Windows venvs have `Scripts/` folder containing `.exe` executables
- Linux/WSL venvs have `bin/` folder containing shell scripts
- WSL cannot execute a Windows activation script meaningfully - it sets Windows-style paths that don't translate to bash environment
- The `activate` script modifies `PATH`, but Windows paths don't work in WSL context

**Resolution:**
Call the Windows Python executable directly without activation:
```bash
./venv/Scripts/python.exe -m duck_sun.scheduler
```

This works because:
- WSL supports Windows interop (can execute `.exe` files directly)
- Python finds its packages relative to the executable location
- No PATH manipulation needed - direct invocation bypasses the need for activation

**Time Cost:** ~15-20 minutes of repeated failed attempts and troubleshooting

**Prevention Pattern:**
1. **For projects on /mnt/c/ paths in WSL:** Always use `./venv/Scripts/python.exe` directly
2. **Never try to `source activate` a Windows venv from WSL** - activation is fundamentally incompatible
3. **Create platform-specific venvs:** If working primarily in WSL, create the venv using `python3 -m venv venv` from WSL (this creates Linux-compatible `bin/` structure)
4. **Document the runtime environment** in README or setup docs to prevent confusion

**Alternative Solutions:**
- **Option A (Current):** Keep Windows venv, use `./venv/Scripts/python.exe` from WSL
- **Option B:** Delete Windows venv, recreate with WSL Python (`python3 -m venv venv`), install dependencies again
- **Option C:** Use Docker for complete environment isolation

**Tags:** #wsl #windows #python #virtualenv #cross-platform #path-issues #environment-setup

**Confidence:** High - Root cause clearly identified, solution tested and working

---

## Victory Archive

*Successful patterns will be recorded here*

---

## Common Pitfalls Index

### Cross-Platform Development
- **WSL + Windows venv incompatibility** → Use `./venv/Scripts/python.exe` directly [2025-12-19]

---

## Technology-Specific Sections

### Python - Virtual Environments

#### WSL/Windows Interop Issues
- Windows venvs use `Scripts/` with `.exe` files
- Linux venvs use `bin/` with shell scripts
- WSL can execute Windows `.exe` via interop, but cannot meaningfully activate Windows venvs
- **Solution:** Direct invocation: `./venv/Scripts/python.exe -m module_name`

---

*Last updated: 2025-12-19*
