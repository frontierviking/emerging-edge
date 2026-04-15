#!/bin/bash
# Emerging Edge — auto-start script
cd /Users/martinsjogren/AI/emerging-edge
# Set your Serper API key in a .env file or export it before running
export SERPER_API_KEY="${SERPER_API_KEY:-}"
exec /usr/bin/python3 monitor.py serve
