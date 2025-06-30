import sqlite3
from typing import Tuple

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS cva(
    cva_id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_date TEXT, end_date TEXT,
    val REAL, vah REAL, poc REAL,
    days INTEGER
)"""


def _init_db(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.execute(CREATE_TABLE)
    con.commit()
    return con


def _overlap_ratio(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    left = max(a[0], b[0])
    right = min(a[1], b[1])
    if right <= left:
        return 0.0
    width = min(a[1] - a[0], b[1] - b[0])
    return (right - left) / width if width > 0 else 0.0


def update_cva(db_path: str, threshold: float = 0.5, max_days: int = 7) -> None:
    con = _init_db(db_path)
    cur = con.execute(
        "SELECT session_date,val,vah,poc FROM daily_va ORDER BY session_date DESC LIMIT 1"
    )
    va = cur.fetchone()
    if not va:
        con.close()
        return
    date, val, vah, poc = va
    cur = con.execute(
        "SELECT rowid,start_date,end_date,val,vah,poc,days FROM cva ORDER BY cva_id DESC LIMIT 1"
    )
    last = cur.fetchone()
    if last:
        _, s, e, cval, cvah, cpoc, days = last
        ratio = _overlap_ratio((val, vah), (cval, cvah))
        if ratio >= threshold and days < max_days:
            new_val = min(val, cval)
            new_vah = max(vah, cvah)
            con.execute(
                "UPDATE cva SET end_date=?, val=?, vah=?, poc=?, days=? WHERE cva_id=?",
                (date, new_val, new_vah, poc, days + 1, last[0]),
            )
            con.commit()
            con.close()
            return
    con.execute(
        "INSERT INTO cva(start_date,end_date,val,vah,poc,days) VALUES(?,?,?,?,?,1)",
        (date, date, val, vah, poc),
    )
    con.commit()
    con.close()


if __name__ == "__main__":
    update_cva("tpo_cva.db")
