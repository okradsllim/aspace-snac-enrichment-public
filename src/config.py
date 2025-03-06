from pathlib import Path

# Base cache directory in OneDrive
CACHE_BASE_DIR = Path.home() / "OneDrive" / "SCMS_Projects" / "ASPACE-SNAC-PROJECT" / "aspace-snac-agent-constellation-caches"
ASPACE_CACHE_DIR = CACHE_BASE_DIR / "aspace_cache"
SNAC_CACHE_DIR = CACHE_BASE_DIR / "snac_cache"

# Project root (not needed for caches, but included as reference for potential use in other path definitions/robustness)
PROJECT_ROOT = Path(__file__).parent.parent