---
name: Python package installs
description: How to install Python dependencies when the base interpreter is managed by Nix.
---

When package installation fails against an immutable base Python runtime, use an available Python Tools module that includes a managed package installer. Do not try to modify `/nix/store` or create a virtual environment.

**Why:** The base runtime rejected pip installation as an externally managed environment; the Python Tools runtime allowed packages to be installed through Replit's package manager.

**How to apply:** Before adding Python dependencies, check which Python module is active and install packages through the Replit package-management flow.
