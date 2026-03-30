"""Write Prometheus exposition format for the last pipeline run (textfile collector)."""

import os
import time
from pathlib import Path
from typing import Any, Dict, Optional


def _line(metric: str, value: Any, labels: Optional[Dict[str, str]] = None) -> str:
    if labels:
        lb = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{metric}{{{lb}}} {value}"
    return f"{metric} {value}"


def write_run_metrics_textfile(result: Dict[str, Any], path: Optional[str] = None) -> Optional[Path]:
    """
    If path or METRICS_TEXTFILE_PATH is set, write metrics and return the path.
    """
    out = path or os.getenv("METRICS_TEXTFILE_PATH")
    if not out:
        return None

    p = Path(out)
    p.parent.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())

    lines = [
        "# HELP revenue_integrity_run_total_anomalies Anomalies recorded in the last completed run.",
        "# TYPE revenue_integrity_run_total_anomalies gauge",
        _line("revenue_integrity_run_total_anomalies", int(result.get("total_anomalies", 0) or 0)),
        "# HELP revenue_integrity_run_new_incidents Incidents created/updated in the last run.",
        "# TYPE revenue_integrity_run_new_incidents gauge",
        _line("revenue_integrity_run_new_incidents", int(result.get("new_incidents", 0) or 0)),
        "# HELP revenue_integrity_run_unixtime Unix time of the last metrics export.",
        "# TYPE revenue_integrity_run_unixtime gauge",
        _line("revenue_integrity_run_unixtime", ts),
    ]
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return p
