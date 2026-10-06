#!/usr/bin/env python3
"""共享厨房过敏原交叉污染排程系统

核心规则：
1. 同一台器具的已排入批次时段不得重叠，冲突批次拒绝排入并保留原因；
2. 器具上前一批次留下的残留过敏原中，凡不在本批次配方内的，均构成
   交叉污染风险，切换前必须存在一条清洁记录满足：
   - 清洁时间在前一批次结束之后、本批次开始之前；
   - 清洁有效期（valid_until）覆盖本批次开始时间；
   - 清洁范围覆盖全部新增残留源（或标记为“全部”）；
   缺少清洁 / 清洁失效 / 覆盖不足时，批次被阻塞并保留阻塞原因；
3. 被阻塞批次不占用器具、不产生残留；调整顺序后重新计算即可。
"""
import csv
import io
import json
import os
import sqlite3
from datetime import datetime

from flask import (Flask, Response, abort, flash, g, redirect, render_template,
                   request, url_for)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE_DIR, "data", "scheduler.db"))
PORT = int(os.environ.get("PORT", "8000"))
APP_URL = os.environ.get("APP_URL", f"http://localhost:{PORT}")

ALLERGENS = ["花生", "坚果", "牛奶", "鸡蛋", "麸质", "大豆",
             "鱼类", "贝类", "芝麻", "芹菜", "芥末", "亚硫酸盐"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS equipment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS batch (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    allergens TEXT NOT NULL DEFAULT '',
    planned_start TEXT NOT NULL,
    planned_end TEXT NOT NULL,
    seq INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS batch_equipment (
    batch_id INTEGER NOT NULL REFERENCES batch(id) ON DELETE CASCADE,
    equipment_id INTEGER NOT NULL REFERENCES equipment(id) ON DELETE CASCADE,
    PRIMARY KEY (batch_id, equipment_id)
);
CREATE TABLE IF NOT EXISTS cleaning (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    equipment_id INTEGER NOT NULL REFERENCES equipment(id) ON DELETE CASCADE,
    cleaned_at TEXT NOT NULL,
    valid_until TEXT NOT NULL,
    covers TEXT NOT NULL DEFAULT '*'
);
CREATE TABLE IF NOT EXISTS allergen_revision (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES batch(id) ON DELETE CASCADE,
    old_allergens TEXT NOT NULL DEFAULT '',
    new_allergens TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL,
    changed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cleaning_deletion (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cleaning_id INTEGER NOT NULL,
    equipment_id INTEGER,
    equipment_name TEXT NOT NULL DEFAULT '',
    cleaned_at TEXT NOT NULL DEFAULT '',
    valid_until TEXT NOT NULL DEFAULT '',
    covers TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL,
    deleted_at TEXT NOT NULL,
    affected TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS handover_version (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_no INTEGER NOT NULL UNIQUE,
    published_at TEXT NOT NULL,
    batch_count INTEGER NOT NULL,
    snapshot TEXT NOT NULL
);
"""

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "allergen-scheduler-dev-key")


# ---------------------------------------------------------------- 数据库

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH, timeout=10)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
        g.db.execute("PRAGMA busy_timeout = 10000")
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    dirpath = os.path.dirname(DB_PATH)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    seed_if_empty(conn)
    conn.close()


def seed_if_empty(conn):
    """首次启动写入演示数据：覆盖 已排入/已缓解/清洁失效/覆盖不足/时段冲突 各情形。"""
    if conn.execute("SELECT COUNT(*) AS c FROM equipment").fetchone()["c"]:
        return
    today = datetime.now().date()

    def t(h, m=0):
        return datetime(today.year, today.month, today.day, h, m).isoformat(timespec="minutes")

    for name in ["台面A", "台面B", "搅拌机", "烤箱"]:
        conn.execute("INSERT INTO equipment(name) VALUES (?)", (name,))
    eq = {r["name"]: r["id"] for r in conn.execute("SELECT * FROM equipment")}

    batches = [
        ("牛奶燕麦糊", "牛奶,麸质", "台面A", t(8), t(9)),
        ("花生能量棒", "花生,大豆", "台面A", t(9, 30), t(10, 30)),
        ("芝麻饼干", "芝麻,麸质", "台面A", t(11), t(12)),
        ("清蒸鲈鱼", "鱼类", "台面B", t(8, 30), t(9, 30)),
        ("贝类意面", "贝类,麸质", "台面B", t(9), t(10)),
        ("香蕉奶昔", "牛奶", "搅拌机", t(8), t(8, 30)),
        ("杏仁奶", "坚果", "搅拌机", t(9), t(9, 30)),
    ]
    for i, (name, alg, eqname, s, e) in enumerate(batches, 1):
        cur = conn.execute(
            "INSERT INTO batch(name, allergens, planned_start, planned_end, seq)"
            " VALUES (?,?,?,?,?)", (name, alg, s, e, i))
        conn.execute("INSERT INTO batch_equipment(batch_id, equipment_id) VALUES (?,?)",
                     (cur.lastrowid, eq[eqname]))

    cleanings = [
        ("台面A", t(9), t(12), "牛奶,麸质"),      # 覆盖 B1→B2 的切换
        ("台面A", t(10, 30), t(11, 30), "花生"),   # 只覆盖花生，未覆盖大豆 → B3 阻塞
        ("搅拌机", t(8, 30), t(8, 45), "*"),        # 09:00 前已失效 → B7 阻塞
    ]
    for eqname, c, v, covers in cleanings:
        conn.execute(
            "INSERT INTO cleaning(equipment_id, cleaned_at, valid_until, covers)"
            " VALUES (?,?,?,?)", (eq[eqname], c, v, covers))
    conn.commit()


# ---------------------------------------------------------------- 工具

def parse_dt(s):
    try:
        return datetime.fromisoformat(s)
    except (TypeError, ValueError):
        return None


def split_allergens(s):
    return {a.strip() for a in (s or "").split(",") if a.strip()}


def join_alg(lst):
    return "、".join(lst) if lst else "无"


def risk_source_text(risk):
    return (f"器具「{risk['equipment']}」残留 {join_alg(risk['residual'])}"
            f"（来自批次「{risk['from_batch']}」）")


def risk_action_text(risk):
    if risk["level"] == "none":
        return "残留过敏原均在本批次配方内，无交叉污染风险"
    if risk["level"] == "mitigated":
        c = risk["cleaning"]
        covers = "全部过敏原" if c["covers"].strip() == "*" else c["covers"]
        return (f"新增风险 {join_alg(risk['new_risks'])}，已由清洁 #{c['id']} 覆盖"
                f"（{c['cleaned_at']} 清洁，有效至 {c['valid_until']}，范围：{covers}）")
    return risk["block_reason"]


def revision_note_text(rev):
    """看板与导出中展示的一行式“最近一次配方变更”说明。"""
    if not rev:
        return ""
    old = join_alg(sorted(split_allergens(rev["old_allergens"])))
    new = join_alg(sorted(split_allergens(rev["new_allergens"])))
    when = (rev["changed_at"] or "").replace("T", " ")
    return (f"配方过敏原最近变更（{when}）：{old} → {new}；"
            f"原因：{rev['reason']}")


# ---------------------------------------------------------------- 排程引擎

def compute_schedule(conn, exclude_cleaning_id=None):
    """按当前顺序（seq, 计划开始时间）逐个尝试排入，返回每批次的结果。

    exclude_cleaning_id 给出的清洁记录在本次计算中视为不存在（规则本身不变），
    用于删除清洁前预演：找出因该记录失效而被阻塞的批次。
    """
    eq_names = {r["id"]: r["name"] for r in conn.execute("SELECT * FROM equipment")}
    equip_of = {}
    for r in conn.execute("SELECT * FROM batch_equipment"):
        equip_of.setdefault(r["batch_id"], []).append(r["equipment_id"])
    cleanings = {}
    for r in conn.execute("SELECT * FROM cleaning ORDER BY cleaned_at, id"):
        if exclude_cleaning_id is not None and r["id"] == exclude_cleaning_id:
            continue
        c = dict(r)
        c["cleaned_dt"] = parse_dt(c["cleaned_at"])
        c["valid_dt"] = parse_dt(c["valid_until"])
        cleanings.setdefault(c["equipment_id"], []).append(c)
    latest_rev = latest_revisions(conn)

    usage = {}  # equipment_id -> [已排入批次的占用记录]
    results = []
    rows = conn.execute(
        "SELECT * FROM batch ORDER BY seq, planned_start, id").fetchall()
    for row in rows:
        start = parse_dt(row["planned_start"])
        end = parse_dt(row["planned_end"])
        b_allergens = split_allergens(row["allergens"])
        eqs = sorted(equip_of.get(row["id"], []))
        blocks, risks = [], []

        if start is None or end is None:
            blocks.append("时段格式无效，请使用 YYYY-MM-DDTHH:MM")
        elif end <= start:
            blocks.append("时段无效：结束时间必须晚于开始时间")
        elif not eqs:
            blocks.append("未指定台面/器具，无法排入")
        else:
            for eq in eqs:
                eq_name = eq_names.get(eq, f"#{eq}")
                usages = usage.get(eq, [])
                conflicts = [u for u in usages
                             if u["start"] < end and start < u["end"]]
                if conflicts:
                    names = "、".join(f"「{u['batch_name']}」" for u in conflicts)
                    blocks.append(
                        f"器具「{eq_name}」时段冲突：与已排入批次 {names} 时间重叠")
                    continue
                prevs = [u for u in usages if u["end"] <= start]
                if not prevs:
                    continue  # 该器具上无前置批次，无残留
                prev = max(prevs, key=lambda u: u["end"])
                residual = sorted(prev["allergens"])
                new_risks = sorted(prev["allergens"] - b_allergens)
                risk = {"equipment": eq_name, "from_batch": prev["batch_name"],
                        "residual": residual, "new_risks": new_risks,
                        "cleaning": None, "level": "none", "block_reason": None}
                if not new_risks:
                    risks.append(risk)
                    continue
                # 切换过敏原：需要覆盖全部残留源的有效清洁
                cand = [c for c in cleanings.get(eq, [])
                        if c["cleaned_dt"] and c["valid_dt"]
                        and prev["end"] <= c["cleaned_dt"] <= start]
                valid = [c for c in cand if c["valid_dt"] >= start]
                covering = [c for c in valid
                            if c["covers"].strip() == "*"
                            or set(new_risks) <= split_allergens(c["covers"])]
                src = join_alg(new_risks)
                if covering:
                    risk["level"] = "mitigated"
                    risk["cleaning"] = covering[-1]
                else:
                    risk["level"] = "blocked"
                    if not cand:
                        reason = (f"器具「{eq_name}」残留过敏原 {src}"
                                  f"（来自批次「{prev['batch_name']}」）："
                                  f"切换过敏原前缺少覆盖全部残留源的清洁记录")
                    elif not valid:
                        reason = (f"器具「{eq_name}」残留过敏原 {src}"
                                  f"（来自批次「{prev['batch_name']}」）："
                                  f"清洁已失效（有效期早于本批次开始时间）")
                    else:
                        reason = (f"器具「{eq_name}」残留过敏原 {src}"
                                  f"（来自批次「{prev['batch_name']}」）："
                                  f"现有清洁未覆盖全部残留源")
                    risk["block_reason"] = reason
                    blocks.append(reason)
                risks.append(risk)

        status = "blocked" if blocks else "accepted"
        if status == "accepted":  # 只有排入成功的批次才占用器具、留下残留
            for eq in eqs:
                usage.setdefault(eq, []).append({
                    "start": start, "end": end,
                    "batch_name": row["name"], "allergens": b_allergens})
        results.append({
            "id": row["id"], "name": row["name"],
            "allergens": sorted(b_allergens),
            "equipment_names": [eq_names.get(e, f"#{e}") for e in eqs],
            "start": start, "end": end,
            "status": status, "risks": risks, "blocks": blocks,
            "latest_revision": latest_rev.get(row["id"]),
        })
    return results


def latest_revisions(conn):
    """每个批次最近一次过敏原修订（按记录 id 最大者，即最后保存的那次）。"""
    latest = {}
    for r in conn.execute(
            "SELECT * FROM allergen_revision ar"
            " WHERE ar.id = (SELECT MAX(id) FROM allergen_revision"
            "                WHERE batch_id = ar.batch_id)"):
        latest[r["batch_id"]] = dict(r)
    return latest


def normalize_seq(conn):
    rows = conn.execute(
        "SELECT id FROM batch ORDER BY seq, planned_start, id").fetchall()
    for i, r in enumerate(rows, 1):
        conn.execute("UPDATE batch SET seq=? WHERE id=?", (i, r["id"]))
    conn.commit()


def cleaning_cover_text(covers):
    return "全部过敏原" if (covers or "").strip() == "*" else (covers or "").replace(",", "、")


def affected_of_cleaning(conn, cid):
    """删除清洁记录前按当前排程预演。

    返回 (cleaning, affected)：cleaning 为该记录行（不存在则为 None）；
    affected 为因该记录失效而由“已排入”变为“阻塞”的批次列表，
    每项包含批次信息、新阻塞原因，以及原先由本清洁缓解的风险说明。
    预演只把该记录视为不存在，排程规则仍由 compute_schedule 统一执行，
    因此级联影响（前序批次失占器具导致后续批次失去残留依据等）也会自然体现。
    """
    cleaning = conn.execute(
        "SELECT c.*, e.name AS equipment_name FROM cleaning c"
        " JOIN equipment e ON e.id = c.equipment_id WHERE c.id=?",
        (cid,)).fetchone()
    if cleaning is None:
        return None, []
    before = {r["id"]: r for r in compute_schedule(conn)}
    after = {r["id"]: r for r in compute_schedule(conn, exclude_cleaning_id=cid)}
    affected = []
    for bid, a in after.items():
        b = before.get(bid)
        if b is None or b["status"] != "accepted" or a["status"] != "blocked":
            continue
        mitigated = []
        for risk in b["risks"]:
            if risk["level"] == "mitigated" and risk["cleaning"] \
                    and risk["cleaning"]["id"] == cid:
                mitigated.append({
                    "equipment": risk["equipment"],
                    "from_batch": risk["from_batch"],
                    "new_risks": risk["new_risks"],
                    "summary": (f"器具「{risk['equipment']}」上来自批次"
                                f"「{risk['from_batch']}」的新增风险"
                                f" {join_alg(risk['new_risks'])} 原由本清洁覆盖"),
                })
        affected.append({
            "batch_id": a["id"],
            "batch_name": a["name"],
            "equipment_names": a["equipment_names"],
            "prev_status": "已排入",
            "new_status": "已阻塞",
            "block_reasons": a["blocks"],
            "mitigated_here": mitigated,
        })
    return cleaning, affected


def cleaning_deletion_rows(conn):
    """读取清洁删除审计记录，affected 快照解析为列表供页面/导出使用。"""
    rows = []
    for r in conn.execute(
            "SELECT cd.*, e.name AS equipment_name_now FROM cleaning_deletion cd"
            " LEFT JOIN equipment e ON e.id = cd.equipment_id"
            " ORDER BY cd.id DESC"):
        d = dict(r)
        try:
            d["affected_list"] = json.loads(d.get("affected") or "[]")
        except (TypeError, ValueError):
            d["affected_list"] = []
        rows.append(d)
    return rows


# -------------------------------------------------------- 生产交接版（发布/快照）

def _iso(dt):
    return dt.isoformat(timespec="minutes") if dt else ""


def _snapshot_risk(risk):
    """把 compute_schedule 的单条风险结构转为可 JSON 化的快照（清洗对象单独拍平）。"""
    snap = {
        "equipment": risk["equipment"],
        "from_batch": risk["from_batch"],
        "residual": list(risk["residual"]),
        "new_risks": list(risk["new_risks"]),
        "level": risk["level"],
        "block_reason": risk["block_reason"],
        "cleaning": None,
    }
    c = risk.get("cleaning")
    if c:
        snap["cleaning"] = {
            "id": c["id"], "equipment_id": c.get("equipment_id"),
            "cleaned_at": c["cleaned_at"], "valid_until": c["valid_until"],
            "covers": c["covers"],
        }
    return snap


def build_handover_snapshot(conn, results, version_no, published_at):
    """按排程结果生成自包含的交接版快照。

    快照只依赖传入的 results（同一事务内算出），并额外读取发布时刻的器具与
    所用清洁记录；批次的时段、过敏原、风险来源、清洁依据、修订说明与状态均
    逐批拷贝进 snapshot，此后批次/器具/清洁被修改或删除都不影响历史版本。
    """
    cleaning_ids = set()
    for r in results:
        for risk in r["risks"]:
            if risk.get("cleaning"):
                cleaning_ids.add(risk["cleaning"]["id"])
    cleanings_used = {}
    if cleaning_ids:
        placeholders = ",".join("?" * len(cleaning_ids))
        for c in conn.execute(
                f"SELECT c.*, e.name AS equipment_name FROM cleaning c"
                f" LEFT JOIN equipment e ON e.id = c.equipment_id"
                f" WHERE c.id IN ({placeholders})",
                tuple(cleaning_ids)):
            cleanings_used[c["id"]] = {
                "id": c["id"], "equipment_id": c["equipment_id"],
                "equipment_name": c["equipment_name"] or f"#{c['equipment_id']}",
                "cleaned_at": c["cleaned_at"], "valid_until": c["valid_until"],
                "covers": c["covers"],
            }
    batches = []
    for i, r in enumerate(results, 1):
        batches.append({
            "order": i,
            "batch_id": r["id"],
            "name": r["name"],
            "allergens": list(r["allergens"]),
            "equipment_names": list(r["equipment_names"]),
            "planned_start": _iso(r["start"]),
            "planned_end": _iso(r["end"]),
            "status": r["status"],
            "blocks": list(r["blocks"]),
            "risks": [_snapshot_risk(risk) for risk in r["risks"]],
            "revision_note": revision_note_text(r.get("latest_revision")),
            "cleaning_ids": sorted(
                risk["cleaning"]["id"] for risk in r["risks"]
                if risk.get("cleaning")),
        })
    return {
        "version_no": version_no,
        "published_at": published_at,
        "app_url": APP_URL,
        "batch_count": len(batches),
        "equipment": [{"id": r["id"], "name": r["name"]}
                      for r in conn.execute("SELECT id, name FROM equipment ORDER BY id")],
        "cleanings_used": [cleanings_used[cid]
                           for cid in sorted(cleanings_used)],
        "batches": batches,
    }


def publish_handover(conn):
    """发布生产交接版：重新计算 → 校验 → 同一写事务内落连续版本号与快照。

    返回 (handover_row 或 None, results, reject_reasons)。
    整个流程在 BEGIN IMMEDIATE 写事务内完成：拿到写锁后再读数据、跑排程引擎，
    保证快照内容与判定依据来自完全一致的同一数据状态，并发写入只能在事务前
    排队或触发锁错误（此时不生成任何版本，由调用方提示重试）。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        results = compute_schedule(conn)
        reasons = []
        if not results:
            reasons.append("当前没有任何批次，排程为空，无法发布生产交接版")
        blocked = [r for r in results if r["status"] != "accepted"]
        if blocked:
            names = "、".join(f"「{r['name']}」" for r in blocked)
            reasons.append(f"存在 {len(blocked)} 个被阻塞批次（{names}），"
                           f"请先消除阻塞再发布")
            for r in blocked:
                for b in r["blocks"]:
                    reasons.append(f"批次「{r['name']}」：{b}")
        if reasons:
            conn.rollback()
            return None, results, reasons
        row = conn.execute(
            "SELECT COALESCE(MAX(version_no), 0) + 1 AS next_no"
            " FROM handover_version").fetchone()
        version_no = row["next_no"]
        published_at = datetime.now().isoformat(timespec="seconds")
        snapshot = build_handover_snapshot(
            conn, results, version_no, published_at)
        cur = conn.execute(
            "INSERT INTO handover_version"
            "(version_no, published_at, batch_count, snapshot)"
            " VALUES (?,?,?,?)",
            (version_no, published_at, len(results),
             json.dumps(snapshot, ensure_ascii=False)))
        conn.commit()
        row = conn.execute(
            "SELECT * FROM handover_version WHERE id=?",
            (cur.lastrowid,)).fetchone()
        return row, results, []
    except Exception:
        conn.rollback()
        raise


def _load_snapshot(raw):
    return json.loads(raw or "{}")


def list_handovers(conn):
    rows = conn.execute(
        "SELECT * FROM handover_version ORDER BY version_no DESC").fetchall()
    out = []
    for r in rows:
        snap = _load_snapshot(r["snapshot"])
        out.append({"id": r["id"], "version_no": r["version_no"],
                    "published_at": r["published_at"],
                    "batch_count": r["batch_count"],
                    "cleaning_used_count": len(snap.get("cleanings_used", []))})
    return out


def load_handover(conn, version_no):
    r = conn.execute(
        "SELECT * FROM handover_version WHERE version_no=?",
        (version_no,)).fetchone()
    if r is None:
        return None
    return {"id": r["id"], "version_no": r["version_no"],
            "published_at": r["published_at"],
            "batch_count": r["batch_count"],
            "snapshot": _load_snapshot(r["snapshot"])}


def latest_handover(conn):
    r = conn.execute(
        "SELECT * FROM handover_version ORDER BY version_no DESC LIMIT 1"
    ).fetchone()
    if r is None:
        return None
    return {"id": r["id"], "version_no": r["version_no"],
            "published_at": r["published_at"],
            "batch_count": r["batch_count"],
            "snapshot": _load_snapshot(r["snapshot"])}


def _used_cleaning_ids(r):
    if "cleaning_ids" in r:  # 历史快照
        return list(r.get("cleaning_ids") or [])
    return sorted({risk["cleaning"]["id"] for risk in r.get("risks", [])
                   if risk.get("cleaning")})


DIFF_FIELDS = [
    ("name", "名称"), ("allergens", "过敏原"), ("equipment_names", "器具"),
    ("planned_start", "开始时间"), ("planned_end", "结束时间"),
    ("status", "状态"), ("cleaning_ids", "所用清洁"),
]


def diff_against_latest(results, latest):
    """实时排程与最新交接版按批次比对，返回 {batch_id: 变化说明}。

    标记新增（最新版中没有的批次 id）、内容变化（关键字段不同，附字段名）；
    最新版中存在、实时排程已没有的批次由调用方根据快照 id 集合标为移除。
    """
    if latest is None:
        return {}
    old = {b["batch_id"]: b for b in latest["snapshot"]["batches"]}
    diffs = {}
    for r in results:
        if r["id"] not in old:
            diffs[r["id"]] = {"kind": "added", "fields": []}
            continue
        o = old[r["id"]]
        new_fp = {
            "name": r["name"],
            "allergens": list(r["allergens"]),
            "equipment_names": list(r["equipment_names"]),
            "planned_start": _iso(r["start"]),
            "planned_end": _iso(r["end"]),
            "status": r["status"],
            "cleaning_ids": _used_cleaning_ids(r),
        }
        old_fp = {k: o.get(k) for k, _ in DIFF_FIELDS}
        changed = [label for k, label in DIFF_FIELDS if old_fp.get(k) != new_fp[k]]
        if changed:
            diffs[r["id"]] = {"kind": "changed", "fields": changed}
    return diffs


def handover_export_rows(snapshot):
    """把历史交接版快照展开为与实时导出一致的行结构（供 CSV/JSON 导出）。"""
    rows = []
    for b in snapshot["batches"]:
        base = {
            "order": b["order"], "batch": b["name"],
            "allergens": b["allergens"],
            "equipment": b["equipment_names"],
            "planned_start": b["planned_start"],
            "planned_end": b["planned_end"],
            "status": "已排入" if b["status"] == "accepted" else "已拒绝",
        }
        note = b.get("revision_note") or ""
        if b["risks"]:
            for risk in b["risks"]:
                row = dict(base)
                row["risk_source"] = risk_source_text(risk)
                row["action"] = risk_action_text(risk)
                if note:
                    row["action"] = f"{row['action']}；{note}"
                rows.append(row)
        else:
            base["risk_source"] = ("无前置残留" if b["status"] == "accepted"
                                   else "—")
            base["action"] = "；".join(b["blocks"]) if b["blocks"] else "无交叉污染风险"
            if note:
                base["action"] = f"{base['action']}；{note}"
            rows.append(base)
    return rows


# ---------------------------------------------------------------- 页面路由

@app.context_processor
def inject_globals():
    return {"app_url": APP_URL, "port": PORT, "allergen_options": ALLERGENS,
            "risk_source_text": risk_source_text,
            "risk_action_text": risk_action_text,
            "revision_note_text": revision_note_text,
            "join_alg": join_alg}


@app.route("/")
def index():
    conn = get_db()
    results = compute_schedule(conn)
    latest = latest_handover(conn)
    diffs = diff_against_latest(results, latest)
    live_ids = {r["id"] for r in results}
    removed_ids = ({b["batch_id"] for b in latest["snapshot"]["batches"]}
                   - live_ids) if latest else set()
    accepted = sum(1 for r in results if r["status"] == "accepted")
    return render_template("index.html", results=results,
                           accepted=accepted,
                           blocked_count=len(results) - accepted,
                           cleaning_deletions=cleaning_deletion_rows(conn),
                           latest=latest, diffs=diffs, removed_ids=removed_ids)


@app.post("/handovers/publish")
def publish_handover_route():
    """把当前排程发布为连续编号、可追溯的生产交接版。

    发布前按当前器具占用、过敏原残留与清洁规则重新计算；无批次或存在
    阻塞批次时拒绝发布、说明原因且不生成版本。成功则在同一事务内保存
    版本号与有序快照（批次/器具/时段/过敏原/风险来源/所用清洁/状态）。
    """
    conn = get_db()
    try:
        row, _results, reasons = publish_handover(conn)
    except sqlite3.OperationalError as exc:
        app.logger.warning("handover publish locked: %s", exc)
        flash("当前有其他修改正在进行，为避免版本与判定依据不一致已拒绝发布，"
              "请稍后重试", "error")
        return redirect(url_for("index"))
    if row is None:
        for reason in reasons:
            flash(f"未发布交接版：{reason}", "error")
        return redirect(url_for("index"))
    flash(f"生产交接版 v{row['version_no']} 已发布（{row['batch_count']} 个批次），"
          f"快照与判定依据已在同一事务内冻结保存", "ok")
    return redirect(url_for("handover_detail", version_no=row["version_no"]))


@app.route("/handovers")
def handover_list():
    return render_template("handovers.html",
                           handovers=list_handovers(get_db()))


@app.route("/handovers/<int:version_no>")
def handover_detail(version_no):
    conn = get_db()
    handover = load_handover(conn, version_no)
    if handover is None:
        abort(404)
    current_ids = {r["id"] for r in conn.execute("SELECT id FROM batch")}
    return render_template("handover_detail.html", h=handover,
                           current_ids=current_ids)


@app.route("/handovers/<int:version_no>/export.csv")
def handover_export_csv(version_no):
    h = load_handover(get_db(), version_no)
    if h is None:
        abort(404)
    snapshot = h["snapshot"]
    out = io.StringIO()
    out.write("﻿")  # BOM，便于 Excel 识别中文
    w = csv.writer(out)
    w.writerow(["交接版本", "发布时间", "顺序", "批次", "本批次过敏原", "使用器具",
                "计划开始", "计划结束", "状态", "风险来源", "处置 / 阻塞原因"])
    meta = [f"v{snapshot['version_no']}", snapshot["published_at"].replace("T", " ")]
    for row in handover_export_rows(snapshot):
        w.writerow([*meta, row["order"], row["batch"], join_alg(row["allergens"]),
                    "、".join(row["equipment"]), row["planned_start"].replace("T", " "),
                    row["planned_end"].replace("T", " "), row["status"],
                    row["risk_source"], row["action"]])
    return Response(out.getvalue(), mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition":
                             f"attachment; filename=handover_v{version_no}.csv"})


@app.route("/handovers/<int:version_no>/export.json")
def handover_export_json(version_no):
    h = load_handover(get_db(), version_no)
    if h is None:
        abort(404)
    snapshot = h["snapshot"]
    payload = {
        "exported_at": datetime.now().isoformat(timespec="seconds"),
        "app_url": APP_URL,
        "handover_version": snapshot["version_no"],
        "published_at": snapshot["published_at"],
        "batch_count": snapshot["batch_count"],
        "equipment_snapshot": snapshot.get("equipment", []),
        "cleanings_used": snapshot.get("cleanings_used", []),
        "batches": snapshot["batches"],
        "schedule": handover_export_rows(snapshot),
    }
    return Response(json.dumps(payload, ensure_ascii=False, indent=2),
                    mimetype="application/json; charset=utf-8",
                    headers={"Content-Disposition":
                             f"attachment; filename=handover_v{version_no}.json"})


@app.post("/recalculate")
def recalculate():
    flash("已按当前顺序、器具占用与清洁有效期重新计算排程", "ok")
    return redirect(url_for("index"))


@app.route("/batches", methods=["GET", "POST"])
def batches():
    conn = get_db()
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        allergens = request.form.getlist("allergens")
        equipment = request.form.getlist("equipment")
        start = request.form.get("planned_start", "")
        end = request.form.get("planned_end", "")
        if not name or not start or not end:
            flash("请填写批次名称与预计时段", "error")
        else:
            seq = conn.execute(
                "SELECT COALESCE(MAX(seq),0)+1 AS s FROM batch").fetchone()["s"]
            cur = conn.execute(
                "INSERT INTO batch(name, allergens, planned_start, planned_end, seq)"
                " VALUES (?,?,?,?,?)",
                (name, ",".join(allergens), start, end, seq))
            for eq in equipment:
                conn.execute("INSERT INTO batch_equipment VALUES (?,?)",
                             (cur.lastrowid, int(eq)))
            conn.commit()
            flash(f"批次「{name}」已录入，当前排第 {seq} 位", "ok")
        return redirect(url_for("batches"))
    rows = conn.execute("SELECT * FROM batch ORDER BY seq").fetchall()
    eq_names = {r["id"]: r["name"]
                for r in conn.execute("SELECT * FROM equipment")}
    equip_of = {}
    for r in conn.execute("SELECT * FROM batch_equipment"):
        equip_of.setdefault(r["batch_id"], []).append(eq_names.get(r["equipment_id"]))
    equipment = conn.execute("SELECT * FROM equipment ORDER BY id").fetchall()
    revisions = conn.execute(
        "SELECT ar.*, b.name AS batch_name FROM allergen_revision ar"
        " JOIN batch b ON b.id = ar.batch_id"
        " ORDER BY ar.id DESC").fetchall()
    return render_template("batches.html", rows=rows, equipment=equipment,
                           equip_of=equip_of, revisions=revisions)


@app.post("/batches/<int:bid>/allergens")
def revise_allergens(bid):
    """修改批次配方过敏原：必须填写变更原因与时间，保存前后集合的修订记录。

    只更新 batch.allergens 并追加修订行；批次顺序、器具关联与清洁记录不受影响，
    重新计算排程时自然使用最新集合。
    """
    conn = get_db()
    row = conn.execute("SELECT * FROM batch WHERE id=?", (bid,)).fetchone()
    if row is None:
        flash("批次不存在或已删除", "error")
        return redirect(url_for("batches"))
    reason = request.form.get("reason", "").strip()
    changed_at = request.form.get("changed_at", "").strip()
    new_list = [a.strip() for a in request.form.getlist("allergens") if a.strip()]
    old_set, new_set = split_allergens(row["allergens"]), set(new_list)
    if not reason:
        flash("未保存：修改过敏原必须填写变更原因", "error")
    elif not changed_at or parse_dt(changed_at) is None:
        flash("未保存：请填写有效的变更时间", "error")
    elif new_set == old_set:
        flash("未保存：过敏原集合无实际变化", "error")
    else:
        new_str = ",".join(new_list)
        conn.execute("UPDATE batch SET allergens=? WHERE id=?", (new_str, bid))
        conn.execute(
            "INSERT INTO allergen_revision"
            "(batch_id, old_allergens, new_allergens, reason, changed_at)"
            " VALUES (?,?,?,?,?)",
            (bid, row["allergens"], new_str, reason, changed_at))
        conn.commit()
        flash(f"批次「{row['name']}」过敏原已更新"
              f"（{join_alg(sorted(old_set))} → {join_alg(sorted(new_set))}），"
              f"修订记录已保存，重新计算排程将使用最新集合", "ok")
    return redirect(url_for("batches"))


@app.post("/batches/<int:bid>/delete")
def delete_batch(bid):
    conn = get_db()
    conn.execute("DELETE FROM batch WHERE id=?", (bid,))
    conn.commit()
    normalize_seq(conn)
    flash("批次已删除", "ok")
    return redirect(url_for("batches"))


@app.post("/batches/<int:bid>/move/<direction>")
def move_batch(bid, direction):
    conn = get_db()
    normalize_seq(conn)
    rows = conn.execute("SELECT id, seq FROM batch ORDER BY seq").fetchall()
    ids = [r["id"] for r in rows]
    if bid in ids:
        i = ids.index(bid)
        j = i - 1 if direction == "up" else i + 1
        if 0 <= j < len(ids):
            a, b = rows[i], rows[j]
            conn.execute("UPDATE batch SET seq=? WHERE id=?", (b["seq"], a["id"]))
            conn.execute("UPDATE batch SET seq=? WHERE id=?", (a["seq"], b["id"]))
            conn.commit()
            flash("已调整顺序并重新计算排程", "ok")
    return redirect(url_for("index"))


@app.route("/equipment", methods=["GET", "POST"])
def equipment_page():
    conn = get_db()
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        if name:
            try:
                conn.execute("INSERT INTO equipment(name) VALUES (?)", (name,))
                conn.commit()
                flash(f"器具「{name}」已添加", "ok")
            except sqlite3.IntegrityError:
                flash(f"器具「{name}」已存在", "error")
        return redirect(url_for("equipment_page"))
    equipment = conn.execute("SELECT * FROM equipment ORDER BY id").fetchall()
    cleanings = conn.execute(
        "SELECT c.*, e.name AS equipment_name FROM cleaning c"
        " JOIN equipment e ON e.id = c.equipment_id"
        " ORDER BY c.cleaned_at DESC, c.id DESC").fetchall()
    return render_template("equipment.html", equipment=equipment,
                           cleanings=cleanings)


@app.post("/equipment/<int:eid>/delete")
def delete_equipment(eid):
    conn = get_db()
    conn.execute("DELETE FROM equipment WHERE id=?", (eid,))
    conn.commit()
    flash("器具已删除（其清洁记录与批次关联一并移除）", "ok")
    return redirect(url_for("equipment_page"))


@app.post("/cleanings")
def add_cleaning():
    conn = get_db()
    equipment_id = request.form.get("equipment_id")
    cleaned_at = request.form.get("cleaned_at", "")
    valid_until = request.form.get("valid_until", "")
    covers = request.form.getlist("covers")
    cover_all = request.form.get("cover_all")
    if not (equipment_id and cleaned_at and valid_until):
        flash("请完整填写清洁记录（器具、清洁时间、有效至）", "error")
    else:
        covers_str = "*" if cover_all else ",".join(covers)
        if not covers_str:
            flash("请选择清洁覆盖的过敏原，或勾选“全部过敏原”", "error")
        else:
            conn.execute(
                "INSERT INTO cleaning(equipment_id, cleaned_at, valid_until, covers)"
                " VALUES (?,?,?,?)",
                (equipment_id, cleaned_at, valid_until, covers_str))
            conn.commit()
            flash("清洁记录已登记", "ok")
    return redirect(url_for("equipment_page"))


@app.route("/cleanings/<int:cid>/delete", methods=["GET", "POST"])
def delete_cleaning(cid):
    """删除清洁记录：

    GET  按当前排程预演，列出因该记录失效而由“已排入/风险已缓解”变为
         阻塞的批次及原因，要求操作者填写删除原因后确认；
    POST 校验记录仍存在、删除原因非空，随后在同一事务内保存删除时间、
         原因与受影响批次快照并删除记录。记录不存在或原因为空时拒绝，
         不改动任何数据。取消（返回）同样不产生改动。
    """
    conn = get_db()
    if request.method == "POST":
        cleaning = conn.execute(
            "SELECT c.*, e.name AS equipment_name FROM cleaning c"
            " JOIN equipment e ON e.id = c.equipment_id WHERE c.id=?",
            (cid,)).fetchone()
        reason = request.form.get("reason", "").strip()
        if cleaning is None:
            flash("清洁记录不存在或已被删除，未改动任何数据", "error")
            return redirect(url_for("equipment_page"))
        if not reason:
            # 拒绝操作：保留原数据，回到确认页并附原因必填提示
            cleaning_preview, affected = affected_of_cleaning(conn, cid)
            flash("未删除：请填写删除原因并确认", "error")
            return render_template(
                "cleaning_delete.html", cleaning=cleaning_preview,
                affected=affected, reason_value=reason), 400
        # 确认前再按当前排程计算一次受影响批次并保存快照
        _, affected = affected_of_cleaning(conn, cid)
        deleted_at = datetime.now().isoformat(timespec="minutes")
        with conn:
            conn.execute(
                "INSERT INTO cleaning_deletion(cleaning_id, equipment_id,"
                " equipment_name, cleaned_at, valid_until, covers, reason,"
                " deleted_at, affected) VALUES (?,?,?,?,?,?,?,?,?)",
                (cleaning["id"], cleaning["equipment_id"],
                 cleaning["equipment_name"], cleaning["cleaned_at"],
                 cleaning["valid_until"], cleaning["covers"], reason,
                 deleted_at, json.dumps(affected, ensure_ascii=False)))
            conn.execute("DELETE FROM cleaning WHERE id=?", (cid,))
        if affected:
            names = "、".join(f"「{a['batch_name']}」" for a in affected)
            flash(f"清洁记录已删除并留档；以下批次因该记录失效转为阻塞：{names}。"
                  f"看板已按现有规则重新计算", "ok")
        else:
            flash("清洁记录已删除并留档；当前排程无批次因此变为阻塞", "ok")
        return redirect(url_for("index"))

    cleaning, affected = affected_of_cleaning(conn, cid)
    if cleaning is None:
        flash("清洁记录不存在或已被删除，未改动任何数据", "error")
        return redirect(url_for("equipment_page"))
    return render_template("cleaning_delete.html", cleaning=cleaning,
                           affected=affected, reason_value="")


# ---------------------------------------------------------------- 导出

def export_rows(results):
    """把排程结果展开为可导出的行。"""
    rows = []
    for i, r in enumerate(results, 1):
        note = revision_note_text(r.get("latest_revision"))
        base = {
            "order": i,
            "batch": r["name"],
            "allergens": r["allergens"],
            "equipment": r["equipment_names"],
            "planned_start": r["start"].isoformat(timespec="minutes") if r["start"] else "",
            "planned_end": r["end"].isoformat(timespec="minutes") if r["end"] else "",
            "status": "已排入" if r["status"] == "accepted" else "已拒绝",
        }
        if r["risks"]:
            for risk in r["risks"]:
                row = dict(base)
                row["risk_source"] = risk_source_text(risk)
                row["action"] = risk_action_text(risk)
                if note:
                    row["action"] = f"{row['action']}；{note}"
                rows.append(row)
        else:
            base["risk_source"] = "无前置残留" if r["status"] == "accepted" else "—"
            base["action"] = "；".join(r["blocks"]) if r["blocks"] else "无交叉污染风险"
            if note:
                base["action"] = f"{base['action']}；{note}"
            rows.append(base)
    return rows


@app.route("/export.csv")
def export_csv():
    results = compute_schedule(get_db())
    out = io.StringIO()
    out.write("﻿")  # BOM，便于 Excel 识别中文
    w = csv.writer(out)
    w.writerow(["顺序", "批次", "本批次过敏原", "使用器具", "计划开始", "计划结束",
                "状态", "风险来源", "处置 / 阻塞原因"])
    for row in export_rows(results):
        w.writerow([row["order"], row["batch"], join_alg(row["allergens"]),
                    "、".join(row["equipment"]), row["planned_start"],
                    row["planned_end"], row["status"],
                    row["risk_source"], row["action"]])
    return Response(out.getvalue(), mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition":
                             "attachment; filename=schedule_risk.csv"})


@app.route("/export.json")
def export_json():
    conn = get_db()
    results = compute_schedule(conn)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "app_url": APP_URL,
        "schedule": export_rows(results),
        "cleaning_deletions": cleaning_deletion_export_rows(conn),
    }
    return Response(json.dumps(payload, ensure_ascii=False, indent=2),
                    mimetype="application/json; charset=utf-8",
                    headers={"Content-Disposition":
                             "attachment; filename=schedule_risk.json"})


# ---------------------------------------------------- 清洁删除审计导出

def cleaning_deletion_export_rows(conn):
    """把清洁删除审计记录展开为可导出的结构（含受影响批次快照）。"""
    rows = []
    for d in cleaning_deletion_rows(conn):
        rows.append({
            "deletion_id": d["id"],
            "cleaning_id": d["cleaning_id"],
            "equipment": d["equipment_name"],
            "cleaned_at": d["cleaned_at"],
            "valid_until": d["valid_until"],
            "covers": cleaning_cover_text(d["covers"]),
            "deleted_at": d["deleted_at"],
            "reason": d["reason"],
            "affected_count": len(d["affected_list"]),
            "affected_batches": [
                {"batch_id": a.get("batch_id"),
                 "batch": a.get("batch_name"),
                 "equipment": "、".join(a.get("equipment_names") or []),
                 "prev_status": a.get("prev_status"),
                 "new_status": a.get("new_status"),
                 "block_reasons": a.get("block_reasons", [])}
                for a in d["affected_list"]],
        })
    return rows


@app.route("/export/cleaning-deletions.csv")
def export_cleaning_deletions_csv():
    out = io.StringIO()
    out.write("﻿")  # BOM，便于 Excel 识别中文
    w = csv.writer(out)
    w.writerow(["删除序号", "清洁记录ID", "器具", "清洁时间", "有效至", "覆盖范围",
                "删除时间", "删除原因", "受影响批次数",
                "受影响批次（状态变化）", "删除后阻塞原因"])
    for d in cleaning_deletion_export_rows(get_db()):
        if d["affected_batches"]:
            names = "、".join(
                f"{a['batch']}（{a['prev_status']}→{a['new_status']}）"
                for a in d["affected_batches"])
            reasons = " | ".join(
                f"{a['batch']}：{'；'.join(a['block_reasons'])}"
                for a in d["affected_batches"])
        else:
            names, reasons = "无", "无"
        w.writerow([d["deletion_id"], d["cleaning_id"], d["equipment"],
                    d["cleaned_at"].replace("T", " "),
                    d["valid_until"].replace("T", " "), d["covers"],
                    d["deleted_at"].replace("T", " "), d["reason"],
                    d["affected_count"], names, reasons])
    return Response(out.getvalue(), mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition":
                             "attachment; filename=cleaning_deletions.csv"})


@app.route("/export/cleaning-deletions.json")
def export_cleaning_deletions_json():
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "app_url": APP_URL,
        "cleaning_deletions": cleaning_deletion_export_rows(get_db()),
    }
    return Response(json.dumps(payload, ensure_ascii=False, indent=2),
                    mimetype="application/json; charset=utf-8",
                    headers={"Content-Disposition":
                             "attachment; filename=cleaning_deletions.json"})


# ---------------------------------------------------------------- 启动

init_db()

if __name__ == "__main__":
    print(f" * 共享厨房过敏原排程系统")
    print(f" * 访问地址: {APP_URL}  (监听端口 {PORT})")
    app.run(host="0.0.0.0", port=PORT)
