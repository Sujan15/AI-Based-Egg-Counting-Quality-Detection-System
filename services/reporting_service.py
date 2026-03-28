# services/reporting_service.py

import pandas as pd
from datetime import datetime
import os

from core.logger_setup import log_audit_export, log_error


class ReportingService:
    @staticmethod
    def generate_daily_report(stats_history: dict,
                               user: str = "system",
                               ip: str = "-") -> str:
        """
        Converts persistent egg history into a professional CSV report.
        Fires an audit log entry on every export.
        """
        data = []
        for track_id, info in stats_history.items():
            data.append({
                "Egg_ID":    track_id,
                "Size":      info["size"],
                "Status":    info["quality"],
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })

        df       = pd.DataFrame(data)
        os.makedirs("data/exports", exist_ok=True)
        filename = (
            f"data/exports/egg_report_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )

        try:
            df.to_csv(filename, index=False)
            # ── Audit: report exported ────────────────────────
            log_audit_export(user=user, report_type="DAILY", ip=ip)
        except Exception as exc:
            log_error("ReportingService",
                      "Failed to write daily CSV report",
                      exc=exc,
                      meta={"filename": filename})
            raise

        return filename
