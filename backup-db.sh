#!/bin/bash
# Daily database backup — dumps SQLite to SQL text and pushes to GitHub
cd /Users/martinsjogren/AI/emerging-edge || exit 1
sqlite3 emerging_edge.db .dump > emerging_edge_backup.sql
git add emerging_edge_backup.sql
git diff --cached --quiet || git commit -m "Daily DB backup $(date +%Y-%m-%d)"
git push 2>/dev/null
