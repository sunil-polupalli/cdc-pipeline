#!/usr/bin/env python3
"""
generate_lineage_report.py
Run this script INSIDE the processor container (or locally with the right env vars)
to regenerate output/lineage_report.json at any time.

Usage (from host):
    docker exec cdc-processor python generate_lineage_report.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from lineage_report import generate_report
generate_report()
