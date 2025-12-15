"""
Duck Sun Modesto: Uncanny Edition

A robust daily solar agent for Modesto, CA power system scheduling using the
Consensus Temperature Model that triangulates data from the world's
three most reliable public weather authorities.

This package implements a deterministic-first approach:
- Solar math is computed in Python (providers/) for 100% accuracy
- Consensus temperatures averaged from US, European, and Canadian models
- Tule Fog detection using physics-based dewpoint/wind analysis
- Claude focuses on interpretation and narrative generation (agent.py)

Architecture:
    providers/     - Multi-source data fetching:
                     * open_meteo.py - GFS/ICON/GEM ensemble
                     * nws.py        - National Weather Service (US official)
                     * met_no.py     - ECMWF via Norwegian Met Institute
                     * metar.py      - KMOD airport ground truth
    uncanniness.py - Consensus model & Tule Fog detection engine
    agent.py       - Claude SDK integration for briefing generation
    scheduler.py   - Legacy single-source orchestration

Entry Points:
    main.py               - Uncanny Edition (recommended)
    python -m duck_sun.scheduler  - Legacy single-source mode
"""

__version__ = "2.0.0"
__author__ = "Duck Sun Modesto"

