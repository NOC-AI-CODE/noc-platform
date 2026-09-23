"""
NOC Platform – Travaux Programmés Covage
Deploy: Railway / Render / Local
"""
import json, os, queue, re, sqlite3, threading, time
from datetime import datetime, timedelta
from flask import Flask, Response, jsonify, request, render_template_string
from flask_cors import CORS
import requests as req
from bs4 import BeautifulSoup
try: import urllib3; urllib3.disable_warnings()
except: pass

# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════
WEBTOOL_BASE = "https://webtool.covage.com"
WEBTOOL_USER = "petancelin"
WEBTOOL_PASS = "xov85igk"
DB_PATH      = os.environ.get("DB_PATH", "noc.db")
PORT         = int(os.environ.get("PORT", 5001))
SYNC_MIN     = int(os.environ.get("SYNC_INTERVAL", 5))
SVC_RE       = re.compile(r'\b(AC\d{6}|FR\d{6}|FO\d{6}|FON\d{5,6}|SF\d{6}|BB\d{5,6}|BK\d{5,6}|CN\d{5,6})\b', re.I)
TP_ID_RE     = re.compile(r"id=([A-Z]{2,4}-[\d\-A-Z]+|TP-[^\s&'\"?]+)", re.I)
TIME_RE      = re.compile(r'\b(\d{2}:\d{2})\b')
HTTP_TO      = (8, 20)

app = Flask(__name__)
CORS(app)

# ═══════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════
_db_lock = threading.Lock()

def get_db():
    db = sqlite3.connect(DB_PATH, check_same_thread=False)
    db.row_factory = sqlite3.Row
    return db

def init_db():
    with _db_lock:
        db = get_db()
        db.executescript("""
        CREATE TABLE IF NOT EXISTS tps (
            id TEXT PRIMARY KEY, date TEXT, time_start TEXT, time_end TEXT,
            title TEXT, operator TEXT DEFAULT '', description TEXT DEFAULT '',
            status TEXT DEFAULT 'En attente', priority TEXT DEFAULT 'Normale',
            detail_url TEXT DEFAULT '', notes TEXT DEFAULT '',
            msg_count INTEGER DEFAULT 0, client_count INTEGER DEFAULT 0,
            created_by TEXT DEFAULT 'Webtool', created_at TEXT,
            synced_at TEXT, updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY, tp_id TEXT, type TEXT DEFAULT 'comment',
            content TEXT, user_name TEXT, created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS clients (
            tp_id TEXT, service_id TEXT,
            check_status TEXT DEFAULT 'pending',
            checked_at TEXT, checked_by TEXT, added_at TEXT,
            PRIMARY KEY(tp_id, service_id)
        );
        CREATE TABLE IF NOT EXISTS activity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tp_id TEXT, user_name TEXT, action TEXT, ts TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_tps_date ON tps(date);
        CREATE INDEX IF NOT EXISTS idx_messages_tp ON messages(tp_id);
        CREATE INDEX IF NOT EXISTS idx_clients_tp ON clients(tp_id);
        CREATE INDEX IF NOT EXISTS idx_activity_tp ON activity(tp_id);
        """)
        db.commit(); db.close()

init_db()

# ═══════════════════════════════════════════════════════════
# SSE – TEMPS RÉEL
# ═══════════════════════════════════════════════════════════
_clients: dict[str, queue.Queue] = {}
_clients_lock = threading.Lock()

def broadcast(event_type: str, data: dict = None):
    msg = json.dumps({"type": event_type, **(data or {})})
    with _clients_lock:
        dead = []
        for cid, q in _clients.items():
            try: q.put_nowait(msg)
            except: dead.append(cid)
        for cid in dead: _clients.pop(cid, None)

# ═══════════════════════════════════════════════════════════
# WEBTOOL SCRAPING
# ═══════════════════════════════════════════════════════════
_wt_session = None
_wt_lock    = threading.Lock()

def get_wt_session():
    global _wt_session
    with _wt_lock:
        if _wt_session:
            try:
                r = _wt_session.get(f"{WEBTOOL_BASE}/index.php", timeout=HTTP_TO, verify=False)
                if 'new_session' not in r.url: return _wt_session
            except: pass
        s = req.Session(); s.verify = False
        r = s.post(f"{WEBTOOL_BASE}//new_session.php",
                   data={"username": WEBTOOL_USER, "pwd": WEBTOOL_PASS}, timeout=HTTP_TO)
        if r.status_code != 200: raise RuntimeError(f"Login Webtool {r.status_code}")
        _wt_session = s; return s

def parse_calendar(html, date_str):
    soup = BeautifulSoup(html, 'html.parser')
    tps, seen = [], set()
    for a in soup.find_all('a', href=True):
        href = a['href']
        m    = TP_ID_RE.search(href)
        if not m: continue
        tp_id = m.group(1).strip()
        if tp_id in seen: continue
        seen.add(tp_id)
        text = a.get_text(' ', strip=True)
        if not text or len(text) < 3: continue
        cell  = a.find_parent(['td','div','li','tr']) or a.parent
        raw   = cell.get_text(' ') if cell else text
        times = TIME_RE.findall(raw)
        full  = href if href.startswith('http') else f"{WEBTOOL_BASE}/{href.lstrip('/')}"
        tps.append({'id':tp_id,'date':date_str,'title':text[:160],
                    'time_start':times[0] if times else '','time_end':times[1] if len(times)>1 else '',
                    'operator':'','detail_url':full})
    return tps

def parse_detail(html):
    soup     = BeautifulSoup(html, 'html.parser')
    text     = soup.get_text(' ')
    clients  = list(dict.fromkeys(m.upper() for m in SVC_RE.findall(text)))
    operator = ''
    for row in soup.select('tr'):
        cells = row.find_all(['td','th'])
        if len(cells) >= 2 and re.search(r'opérateur|operateur', cells[0].get_text(), re.I):
            operator = cells[1].get_text(strip=True); break
    return clients, operator

def sync_date(date_str):
    s    = get_wt_session()
    r    = s.get(f"{WEBTOOL_BASE}/reseaux/exploitation/details/index.php?date={date_str}",
                 timeout=HTTP_TO, verify=False)
    if r.status_code != 200: raise RuntimeError(f"Calendar HTTP {r.status_code}")
    tps  = parse_calendar(r.text, date_str)
    if not tps: return 0
    now  = datetime.now().isoformat()
    db   = get_db()
    with _db_lock:
        for tp in tps:
            clients, op = [], tp['operator']
            if tp['detail_url']:
                try:
                    dr = s.get(tp['detail_url'], timeout=HTTP_TO, verify=False)
                    if dr.status_code == 200: clients, op = parse_detail(dr.text)
                except: pass
            ex = db.execute("SELECT status,notes FROM tps WHERE id=?", (tp['id'],)).fetchone()
            db.execute("""
                INSERT INTO tps(id,date,time_start,time_end,title,operator,detail_url,
                    status,priority,notes,client_count,created_by,created_at,synced_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    date=excluded.date,time_start=excluded.time_start,time_end=excluded.time_end,
                    title=excluded.title,operator=excluded.operator,detail_url=excluded.detail_url,
                    client_count=excluded.client_count,synced_at=excluded.synced_at
            """, (tp['id'],date_str,tp['time_start'],tp['time_end'],tp['title'],op,tp['detail_url'],
                  ex['status'] if ex else 'En attente','Normale',ex['notes'] if ex else '',
                  len(clients),'Webtool',now,now))
            for sid in clients:
                db.execute("INSERT OR IGNORE INTO clients(tp_id,service_id,check_status,added_at) VALUES(?,?,?,?)",
                           (tp['id'],sid,'pending',now))
        db.commit(); db.close()
    broadcast('tps_updated', {'date': date_str})
    return len(tps)

# ═══════════════════════════════════════════════════════════
# BACKGROUND SYNC
# ═══════════════════════════════════════════════════════════
_last_sync = {'ts': None, 'count': 0, 'status': 'init', 'error': ''}

def background_sync():
    while True:
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            n = sync_date(today)
            if datetime.now().hour >= 20:
                tmrw = (datetime.now()+timedelta(days=1)).strftime('%Y-%m-%d')
                sync_date(tmrw)
            _last_sync.update({'ts': datetime.now().isoformat(), 'count': n, 'status': 'ok', 'error': ''})
            print(f"[Sync] {today}: {n} TP(s)")
        except Exception as e:
            _last_sync.update({'ts': datetime.now().isoformat(), 'status': 'error', 'error': str(e)})
            print(f"[Sync] Erreur: {e}")
            global _wt_session; _wt_session = None
        time.sleep(SYNC_MIN * 60)

# ═══════════════════════════════════════════════════════════
# API ROUTES
# ═══════════════════════════════════════════════════════════
@app.route('/api/tps')
def api_tps():
    date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    db   = get_db()
    rows = db.execute("SELECT * FROM tps WHERE date=? ORDER BY time_start", (date,)).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/tp/<tp_id>')
def api_tp(tp_id):
    db = get_db()
    tp = db.execute("SELECT * FROM tps WHERE id=?", (tp_id,)).fetchone()
    if not tp: db.close(); return jsonify({'error':'Not found'}), 404
    cl = db.execute("SELECT * FROM clients WHERE tp_id=? ORDER BY service_id", (tp_id,)).fetchall()
    db.close()
    return jsonify({**dict(tp), 'clients': [dict(c) for c in cl]})

@app.route('/api/tp/<tp_id>', methods=['PATCH'])
def api_tp_patch(tp_id):
    data = request.json or {}
    now  = datetime.now().isoformat()
    db   = get_db()
    with _db_lock:
        tp = db.execute("SELECT status FROM tps WHERE id=?", (tp_id,)).fetchone()
        if not tp: db.close(); return jsonify({'error':'Not found'}), 404
        old_status = tp['status']
        fields = {k: v for k, v in data.items() if k in ('status','notes','priority','operator','description')}
        if fields:
            sets = ','.join(f"{k}=?" for k in fields)
            db.execute(f"UPDATE tps SET {sets},updated_at=? WHERE id=?",
                       (*fields.values(), now, tp_id))
        if 'status' in fields and fields['status'] != old_status:
            db.execute("INSERT INTO messages(id,tp_id,type,content,user_name,created_at) VALUES(?,?,?,?,?,?)",
                       (f"sys-{int(time.time()*1000)}",tp_id,'system',
                        f"Statut : {old_status} → {fields['status']}",
                        data.get('user','Équipe NOC'), now))
            cnt = db.execute("SELECT COUNT(*) as c FROM messages WHERE tp_id=?", (tp_id,)).fetchone()['c']
            db.execute("UPDATE tps SET msg_count=? WHERE id=?", (cnt, tp_id))
            db.execute("INSERT INTO activity(tp_id,user_name,action,ts) VALUES(?,?,?,?)",
                       (tp_id, data.get('user','Équipe NOC'), f"Statut → {fields['status']}", now))
        db.commit(); db.close()
    broadcast('tp_updated', {'tp_id': tp_id})
    return jsonify({'ok': True})

@app.route('/api/tp', methods=['POST'])
def api_create_tp():
    data = request.json or {}
    if not data.get('title'): return jsonify({'error':'Titre requis'}), 400
    now  = datetime.now().isoformat()
    db   = get_db()
    tp_id = data.get('id') or f"TP-{int(time.time()*1000)}"
    with _db_lock:
        db.execute("""INSERT INTO tps(id,date,time_start,time_end,title,operator,description,
            status,priority,notes,client_count,created_by,created_at,synced_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (tp_id, data.get('date', datetime.now().strftime('%Y-%m-%d')),
             data.get('time_start',''), data.get('time_end',''),
             data['title'], data.get('operator',''), data.get('description',''),
             'En attente', data.get('priority','Normale'), '', 0,
             data.get('user','Équipe NOC'), now, now))
        clients = list(dict.fromkeys(m.upper() for m in SVC_RE.findall(data.get('clients_raw',''))))
        for sid in clients:
            db.execute("INSERT OR IGNORE INTO clients(tp_id,service_id,check_status,added_at) VALUES(?,?,?,?)",
                       (tp_id,sid,'pending',now))
        if clients:
            db.execute("UPDATE tps SET client_count=? WHERE id=?", (len(clients), tp_id))
        db.execute("INSERT INTO activity(tp_id,user_name,action,ts) VALUES(?,?,?,?)",
                   (tp_id, data.get('user','Équipe NOC'), 'TP créé', now))
        db.commit(); db.close()
    broadcast('tps_updated', {'date': data.get('date', datetime.now().strftime('%Y-%m-%d'))})
    return jsonify({'ok': True, 'id': tp_id})

@app.route('/api/messages/<tp_id>')
def api_messages(tp_id):
    db  = get_db()
    msg = db.execute("SELECT * FROM messages WHERE tp_id=? ORDER BY created_at", (tp_id,)).fetchall()
    db.close()
    return jsonify([dict(m) for m in msg])

@app.route('/api/messages', methods=['POST'])
def api_post_message():
    data = request.json or {}
    tp_id, content = data.get('tp_id'), (data.get('content') or '').strip()
    if not tp_id or not content: return jsonify({'error':'Données manquantes'}), 400
    now  = datetime.now().isoformat()
    mid  = f"msg-{int(time.time()*1000)}"
    db   = get_db()
    with _db_lock:
        db.execute("INSERT INTO messages(id,tp_id,type,content,user_name,created_at) VALUES(?,?,?,?,?,?)",
                   (mid, tp_id, data.get('type','comment'), content, data.get('user','Équipe NOC'), now))
        cnt = db.execute("SELECT COUNT(*) as c FROM messages WHERE tp_id=?", (tp_id,)).fetchone()['c']
        db.execute("UPDATE tps SET msg_count=?,updated_at=? WHERE id=?", (cnt, now, tp_id))
        db.commit(); db.close()
    broadcast('message', {'tp_id': tp_id})
    return jsonify({'ok': True, 'id': mid})

@app.route('/api/clients/<tp_id>', methods=['POST'])
def api_add_clients(tp_id):
    data    = request.json or {}
    raw     = data.get('ids_raw', '') + ' ' + ' '.join(data.get('ids', []))
    clients = list(dict.fromkeys(m.upper() for m in SVC_RE.findall(raw)))
    if not clients: return jsonify({'error':'Aucun ID valide'}), 400
    now = datetime.now().isoformat()
    db  = get_db()
    with _db_lock:
        for sid in clients:
            db.execute("INSERT OR IGNORE INTO clients(tp_id,service_id,check_status,added_at) VALUES(?,?,?,?)",
                       (tp_id, sid, 'pending', now))
        cnt = db.execute("SELECT COUNT(*) as c FROM clients WHERE tp_id=?", (tp_id,)).fetchone()['c']
        db.execute("UPDATE tps SET client_count=? WHERE id=?", (cnt, tp_id))
        db.execute("INSERT INTO activity(tp_id,user_name,action,ts) VALUES(?,?,?,?)",
                   (tp_id, data.get('user','Équipe NOC'), f"{len(clients)} client(s) ajoutés", now))
        db.commit(); db.close()
    broadcast('tp_updated', {'tp_id': tp_id})
    return jsonify({'ok': True, 'added': len(clients)})

@app.route('/api/clients/<tp_id>/<service_id>', methods=['PATCH'])
def api_update_client(tp_id, service_id):
    data = request.json or {}
    now  = datetime.now().isoformat()
    db   = get_db()
    with _db_lock:
        db.execute("UPDATE clients SET check_status=?,checked_at=?,checked_by=? WHERE tp_id=? AND service_id=?",
                   (data.get('check_status','pending'), now, data.get('user','NOC'), tp_id, service_id))
        db.commit(); db.close()
    broadcast('tp_updated', {'tp_id': tp_id})
    return jsonify({'ok': True})

@app.route('/api/sync', methods=['POST'])
def api_sync():
    date = (request.json or {}).get('date', datetime.now().strftime('%Y-%m-%d'))
    try:
        n = sync_date(date)
        return jsonify({'ok': True, 'count': n})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/status')
def api_status():
    return jsonify({'sync': _last_sync, 'interval_min': SYNC_MIN})

@app.route('/api/activity')
def api_activity():
    db   = get_db()
    acts = db.execute("SELECT * FROM activity ORDER BY ts DESC LIMIT 30").fetchall()
    db.close()
    return jsonify([dict(a) for a in acts])

@app.route('/api/events')
def api_events():
    cid = str(time.time())
    q   = queue.Queue()
    with _clients_lock: _clients[cid] = q
    def stream():
        try:
            yield "data: {\"type\":\"connected\"}\n\n"
            while True:
                try:
                    msg = q.get(timeout=25)
                    yield f"data: {msg}\n\n"
                except queue.Empty:
                    yield ": keepalive\n\n"
        finally:
            with _clients_lock: _clients.pop(cid, None)
    return Response(stream(), content_type='text/event-stream',
                    headers={'Cache-Control':'no-cache','X-Accel-Buffering':'no'})

# ═══════════════════════════════════════════════════════════
# FRONTEND HTML
# ═══════════════════════════════════════════════════════════
FRONTEND = r"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NOC Platform</title>
<style>
:root{
  --w:#fff;--bg:#f8fafc;--bg2:#f1f5f9;--bd:#e2e8f0;--bd2:#cbd5e1;
  --tx:#0f172a;--tx2:#475569;--tx3:#94a3b8;
  --blue:#2563eb;--blue-bg:#eff6ff;--blue-bd:#bfdbfe;
  --green:#16a34a;--green-bg:#f0fdf4;
  --red:#dc2626;--red-bg:#fef2f2;--red-bd:#fecaca;
  --amber:#d97706;--amber-bg:#fffbeb;
  --r:6px;--sh:0 1px 3px rgba(0,0,0,.08);--sh2:0 4px 16px rgba(0,0,0,.12);
  --sb:256px;--tb:54px
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;overflow:hidden}
body{font:13px/1.5 Inter,-apple-system,'Segoe UI',sans-serif;color:var(--tx);background:var(--bg)}
::-webkit-scrollbar{width:4px;height:4px}::-webkit-scrollbar-thumb{background:var(--bd2);border-radius:2px}
button,input,select,textarea{font:inherit;outline:none}
button{cursor:pointer;border:none;background:none}

/* ── TOPBAR ── */
#tb{position:fixed;top:0;left:0;right:0;height:var(--tb);background:var(--w);
  border-bottom:1px solid var(--bd);display:flex;align-items:center;
  padding:0 20px;gap:12px;z-index:100}
.logo{font-size:15px;font-weight:700;color:var(--tx);letter-spacing:-.3px}
.logo b{color:var(--blue)}
.divv{width:1px;height:18px;background:var(--bd)}
#datenav{display:flex;align-items:center;gap:4px}
.dbtn{width:26px;height:26px;border-radius:var(--r);border:1px solid var(--bd);
  display:flex;align-items:center;justify-content:center;color:var(--tx2);
  font-size:14px;background:var(--w);transition:.12s}
.dbtn:hover{background:var(--bg);border-color:var(--bd2)}
#dlabel{font-size:13px;font-weight:600;padding:4px 10px;border-radius:var(--r);
  border:1px solid var(--bd);background:var(--w);min-width:130px;text-align:center;cursor:pointer}
.sp{flex:1}
#sync-dot{width:7px;height:7px;border-radius:50%;background:var(--tx3);flex-shrink:0}
#sync-dot.ok{background:var(--green)}#sync-dot.err{background:var(--red)}
#sync-dot.spin{background:var(--amber);animation:p .7s infinite}
@keyframes p{0%,100%{opacity:1}50%{opacity:.3}}
#synclbl{font-size:11px;color:var(--tx2)}
.tbtn{height:32px;padding:0 12px;border-radius:var(--r);font-size:13px;font-weight:500;
  transition:.12s;display:inline-flex;align-items:center;gap:5px;white-space:nowrap}
.tb-ghost{border:1px solid var(--bd);color:var(--tx2);background:var(--w)}
.tb-ghost:hover{background:var(--bg);border-color:var(--bd2);color:var(--tx)}
.tb-primary{background:var(--blue);color:#fff;border:1px solid #1d4ed8}
.tb-primary:hover{background:#1d4ed8}

/* ── SHELL ── */
#shell{position:fixed;top:var(--tb);left:0;right:0;bottom:0;display:flex}

/* ── SIDEBAR ── */
#sb{width:var(--sb);flex-shrink:0;background:var(--w);border-right:1px solid var(--bd);
  display:flex;flex-direction:column;overflow:hidden}
.sb-nav{padding:8px 8px 0}
.nv{display:flex;align-items:center;gap:7px;padding:6px 8px;border-radius:var(--r);
  font-size:13px;color:var(--tx2);cursor:pointer;transition:.1s;font-weight:500}
.nv:hover{background:var(--bg);color:var(--tx)}
.nv.on{background:var(--blue-bg);color:var(--blue)}
.nv svg{width:15px;height:15px;flex-shrink:0;opacity:.7}
.sbd{height:1px;background:var(--bd);margin:6px 12px}
.sb-hd{display:flex;align-items:center;justify-content:space-between;padding:8px 12px 4px}
.sb-lbl{font-size:10px;font-weight:700;color:var(--tx3);letter-spacing:.7px;text-transform:uppercase}
.sb-add{width:20px;height:20px;border-radius:4px;display:flex;align-items:center;
  justify-content:center;color:var(--tx3);font-size:15px;transition:.1s}
.sb-add:hover{background:var(--bg2);color:var(--tx)}
#sbsearch{display:block;width:calc(100% - 16px);margin:0 8px 6px;background:var(--bg);
  border:1px solid var(--bd);color:var(--tx);padding:5px 10px;border-radius:var(--r);font-size:12px}
#sbsearch:focus{border-color:var(--blue);background:var(--w)}
.chips{display:flex;gap:4px;padding:0 8px 6px;flex-wrap:wrap}
.chip{padding:2px 8px;border-radius:12px;font-size:11px;font-weight:500;cursor:pointer;
  border:1px solid var(--bd);color:var(--tx2);background:var(--w);transition:.1s}
.chip:hover,.chip.on{background:var(--tx);color:var(--w);border-color:var(--tx)}
#sblist{flex:1;overflow-y:auto;padding:0 8px 8px}
.si{padding:7px 8px;border-radius:var(--r);cursor:pointer;transition:.1s;
  display:flex;align-items:flex-start;gap:7px;border:1px solid transparent;margin-bottom:3px}
.si:hover{background:var(--bg)}
.si.on{background:var(--bg);border-color:var(--bd)}
.si.incident{border-color:var(--red-bd)!important;background:var(--red-bg)}
.sdot{width:8px;height:8px;border-radius:50%;flex-shrink:0;margin-top:4px}
.si-r{flex:1;min-width:0}
.si-title{font-size:11px;font-weight:500;color:var(--tx);white-space:nowrap;
  overflow:hidden;text-overflow:ellipsis}
.si-meta{font-size:10px;color:var(--tx3);margin-top:1px}

/* ── CONTENT ── */
#cnt{flex:1;overflow:hidden;display:flex;flex-direction:column;min-width:0}

/* ── ALERT ── */
#alert{display:none;align-items:center;gap:8px;padding:7px 20px;
  background:var(--red-bg);border-bottom:1px solid var(--red-bd);
  font-size:12px;font-weight:500;color:var(--red);flex-shrink:0}
#alert.on{display:flex}
.alert-x{margin-left:auto;color:var(--tx3);font-size:16px}

/* ── VIEWS ── */
#views{flex:1;overflow:hidden;position:relative}
.view{position:absolute;inset:0;overflow-y:auto;padding:22px;display:none}
.view.on{display:block}
.vhd{display:flex;align-items:center;justify-content:space-between;margin-bottom:18px}
.vtitle{font-size:18px;font-weight:700;color:var(--tx)}
.vsub{font-size:13px;color:var(--tx2);margin-top:2px}

/* ── STAT CARDS ── */
.sg{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:20px}
.sc{background:var(--w);border:1px solid var(--bd);border-radius:var(--r);
  padding:14px;transition:.15s}
.sc:hover{box-shadow:var(--sh)}
.scn{font-size:10px;font-weight:700;letter-spacing:.6px;color:var(--tx3);
  text-transform:uppercase;margin-bottom:8px}
.scv{font-size:26px;font-weight:700;color:var(--tx);line-height:1}
.scs{font-size:11px;color:var(--tx2);margin-top:3px}

/* ── DASHBOARD GRID ── */
.dg{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.panel{background:var(--w);border:1px solid var(--bd);border-radius:var(--r);overflow:hidden}
.phd{padding:10px 14px;border-bottom:1px solid var(--bd);
  display:flex;justify-content:space-between;align-items:center}
.plbl{font-size:11px;font-weight:700;color:var(--tx2);letter-spacing:.4px;text-transform:uppercase}
.pb{padding:10px}

/* ── TABLE ── */
.tc{background:var(--w);border:1px solid var(--bd);border-radius:var(--r);overflow:hidden}
.ttb{padding:10px 14px;border-bottom:1px solid var(--bd);display:flex;align-items:center;gap:8px}
.tls{flex:1;max-width:220px;background:var(--bg);border:1px solid var(--bd);
  color:var(--tx);padding:5px 9px;border-radius:var(--r);font-size:12px}
.tls:focus{border-color:var(--blue);background:var(--w)}
.tlsel{background:var(--bg);border:1px solid var(--bd);color:var(--tx);
  padding:4px 24px 4px 9px;border-radius:var(--r);font-size:12px;
  -webkit-appearance:none;appearance:none;
  background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%2394a3b8'/%3E%3C/svg%3E");
  background-repeat:no-repeat;background-position:right 8px center}
table{width:100%;border-collapse:collapse}
thead th{padding:7px 14px;text-align:left;font-size:10px;font-weight:700;color:var(--tx3);
  letter-spacing:.6px;text-transform:uppercase;border-bottom:1px solid var(--bd);
  background:var(--bg);white-space:nowrap}
tbody tr{border-bottom:1px solid var(--bd);transition:.1s;cursor:pointer}
tbody tr:last-child{border-bottom:none}
tbody tr:hover{background:var(--bg)}
tbody td{padding:9px 14px;font-size:13px;color:var(--tx)}
.mono{font-family:monospace;font-size:11px;color:var(--blue)}
.tlight{color:var(--tx2);font-size:12px}

/* ── STATUS ── */
.st{display:inline-flex;align-items:center;gap:5px;font-size:12px;font-weight:500}
.sd{width:6px;height:6px;border-radius:50%;flex-shrink:0}
.st[data-s="En attente"] .sd{background:var(--tx3)}
.st[data-s="En cours"]   .sd{background:var(--blue)}
.st[data-s="Incident"]   .sd{background:var(--red);box-shadow:0 0 0 3px var(--red-bg)}
.st[data-s="Terminé"]    .sd{background:var(--green)}
.st[data-s="Annulé"]     .sd{background:var(--bd2)}
.st[data-s="En attente"]{color:var(--tx2)}
.st[data-s="En cours"]{color:var(--blue)}
.st[data-s="Incident"]{color:var(--red);font-weight:700}
.st[data-s="Terminé"]{color:var(--green)}
.st[data-s="Annulé"]{color:var(--tx3)}

/* ── DETAIL ── */
#view-detail{padding:0!important;display:none}
#view-detail.on{display:flex}
.dpl{flex:1;min-width:0;overflow:hidden;display:flex;flex-direction:column}
.dpr{width:296px;flex-shrink:0;border-left:1px solid var(--bd);overflow-y:auto;background:var(--w)}
.dphd{padding:18px 22px 14px;border-bottom:1px solid var(--bd);background:var(--w);flex-shrink:0}
.dpid{font-size:10px;font-family:monospace;color:var(--blue);font-weight:700;margin-bottom:3px}
.dptitle{font-size:15px;font-weight:700;color:var(--tx);margin-bottom:8px;line-height:1.3}
.dpmetas{display:flex;gap:14px;align-items:center;flex-wrap:wrap}
.dpm{font-size:12px;color:var(--tx2)}
/* messages */
.mthread{flex:1;overflow-y:auto;padding:14px 22px;display:flex;flex-direction:column;gap:0}
.mgrp{margin-top:12px}
.mgrhd{display:flex;align-items:center;gap:7px;margin-bottom:4px}
.mav{width:23px;height:23px;border-radius:50%;flex-shrink:0;display:flex;
  align-items:center;justify-content:center;font-size:9px;font-weight:700;color:#fff}
.mauth{font-size:12px;font-weight:600}
.mts{font-size:10px;color:var(--tx3)}
.mbody{margin-left:30px;font-size:13px;color:var(--tx);line-height:1.6;word-break:break-word}
.msys{margin-top:8px;margin-left:30px;font-size:11px;color:var(--tx3);display:flex;align-items:center;gap:6px}
.msys::before{content:'';flex:none;width:12px;height:1px;background:var(--bd)}
.mchip{background:var(--bg2);border:1px solid var(--bd);border-radius:var(--r);
  padding:1px 8px;font-size:11px;font-weight:500;color:var(--tx2)}
/* compose */
.compose{padding:14px 22px;border-top:1px solid var(--bd);flex-shrink:0;background:var(--w)}
.cbox{display:flex;gap:8px;align-items:flex-end}
.cta{flex:1;background:var(--bg);border:1px solid var(--bd);color:var(--tx);
  padding:7px 11px;border-radius:var(--r);resize:none;min-height:34px;max-height:100px;
  font-size:13px;line-height:1.4;transition:.12s}
.cta:focus{background:var(--w);border-color:var(--blue);box-shadow:0 0 0 3px rgba(37,99,235,.1)}
.csend{height:34px;padding:0 14px;background:var(--blue);color:#fff;
  border-radius:var(--r);font-size:13px;font-weight:600;transition:.12s}
.csend:hover{background:#1d4ed8}
.chint{font-size:10px;color:var(--tx3);margin-top:4px}
/* right panel */
.rp{padding:14px}
.rp+.rp{border-top:1px solid var(--bd)}
.rph{font-size:10px;font-weight:700;letter-spacing:.7px;color:var(--tx3);
  text-transform:uppercase;margin-bottom:8px}
.stgrid{display:flex;flex-direction:column;gap:4px}
.stbtn{width:100%;padding:6px 10px;border-radius:var(--r);border:1px solid var(--bd);
  text-align:left;font-size:12px;font-weight:500;color:var(--tx2);background:var(--w);
  cursor:pointer;transition:.12s;display:flex;align-items:center;gap:8px}
.stbtn:hover{border-color:var(--bd2);background:var(--bg)}
.stbtn.on{background:var(--bg);border-color:var(--bd2);color:var(--tx)}
.stbtn.on[data-s="En cours"]{background:var(--blue-bg);border-color:var(--blue-bd);color:var(--blue)}
.stbtn.on[data-s="Incident"]{background:var(--red-bg);border-color:var(--red-bd);color:var(--red)}
.stbtn.on[data-s="Terminé"]{background:var(--green-bg);border-color:#bbf7d0;color:var(--green)}
.ir{display:flex;justify-content:space-between;padding:4px 0;
  border-bottom:1px solid var(--bd);font-size:12px}
.ir:last-child{border-bottom:none}
.irl{color:var(--tx2)}
.irv{color:var(--tx);font-weight:500;max-width:150px;text-align:right;word-break:break-word}
.cr{display:flex;align-items:center;padding:4px 0;border-bottom:1px solid var(--bd);gap:7px}
.cr:last-child{border-bottom:none}
.cid{font-family:monospace;font-size:11px;font-weight:600;color:var(--blue);flex:1}
.cst{font-size:10px;font-weight:700;padding:1px 5px;border-radius:4px}
.cst.pending{background:var(--bg2);color:var(--tx3)}
.cst.up{background:var(--green-bg);color:var(--green)}
.cst.down{background:var(--red-bg);color:var(--red)}
.cst.error{background:var(--amber-bg);color:var(--amber)}
.rta{width:100%;background:var(--bg);border:1px solid var(--bd);color:var(--tx);
  padding:7px 9px;border-radius:var(--r);resize:none;min-height:64px;
  font-size:12px;line-height:1.4;transition:.12s}
.rta:focus{background:var(--w);border-color:var(--blue);box-shadow:0 0 0 3px rgba(37,99,235,.1)}
/* progress */
.pgw{height:4px;background:var(--bg2);border-radius:2px;overflow:hidden;margin:4px 0}
.pgb{height:100%;background:var(--blue);border-radius:2px;transition:width .3s}

/* ── BUTTONS ── */
.btn{display:inline-flex;align-items:center;gap:5px;padding:5px 11px;
  border-radius:var(--r);font-size:12px;font-weight:500;transition:.12s;
  border:1px solid transparent;cursor:pointer}
.bp{background:var(--blue);color:#fff;border-color:#1d4ed8}.bp:hover{background:#1d4ed8}
.bd{background:var(--w);color:var(--tx);border-color:var(--bd)}.bd:hover{background:var(--bg);border-color:var(--bd2)}
.bg{background:var(--w);color:var(--green);border-color:#bbf7d0}.bg:hover{background:var(--green-bg)}
.br{background:var(--w);color:var(--red);border-color:var(--red-bd)}.br:hover{background:var(--red-bg)}
.bsm{padding:3px 9px;font-size:11px}
.bgh{background:transparent;color:var(--tx2);border-color:transparent}.bgh:hover{background:var(--bg);color:var(--tx)}

/* ── MODALS ── */
.ov{position:fixed;inset:0;background:rgba(15,23,42,.4);z-index:500;
  display:none;align-items:center;justify-content:center;backdrop-filter:blur(2px)}
.ov.on{display:flex}
.modal{background:var(--w);border:1px solid var(--bd);border-radius:10px;
  width:500px;max-height:88vh;overflow-y:auto;box-shadow:var(--sh2)}
.modal.lg{width:620px}
.mhd{padding:16px 20px;border-bottom:1px solid var(--bd);
  display:flex;justify-content:space-between;align-items:center}
.mtitle{font-size:14px;font-weight:700}
.mx{color:var(--tx3);font-size:18px;line-height:1;padding:1px 5px;border-radius:4px;transition:.1s}
.mx:hover{background:var(--bg2);color:var(--tx)}
.mbody{padding:18px 20px}
.mft{padding:12px 20px;border-top:1px solid var(--bd);display:flex;gap:7px;justify-content:flex-end}
.fg{margin-bottom:12px}
.fl{display:block;font-size:11px;font-weight:600;color:var(--tx2);margin-bottom:4px;letter-spacing:.3px}
.fc{width:100%;background:var(--bg);border:1px solid var(--bd);color:var(--tx);
  padding:6px 9px;border-radius:var(--r);transition:.12s}
.fc:focus{background:var(--w);border-color:var(--blue);box-shadow:0 0 0 3px rgba(37,99,235,.1)}
select.fc{-webkit-appearance:none;appearance:none;
  background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%2394a3b8'/%3E%3C/svg%3E");
  background-repeat:no-repeat;background-position:right 9px center;padding-right:26px}
.fr{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.fhint{font-size:11px;color:var(--tx3);margin-top:3px}

/* ── TOAST ── */
#toast{position:fixed;bottom:18px;right:18px;z-index:9999;
  background:var(--tx);color:var(--w);padding:9px 14px;border-radius:var(--r);
  font-size:13px;font-weight:500;box-shadow:var(--sh2);opacity:0;
  transition:opacity .25s;pointer-events:none;max-width:300px}
#toast.on{opacity:1}

/* ── EMPTY ── */
.empty{display:flex;flex-direction:column;align-items:center;justify-content:center;
  gap:7px;padding:44px 20px;color:var(--tx3);text-align:center;font-size:13px}

/* timeline */
.tli{display:flex;gap:10px;align-items:flex-start;padding:7px 0;
  border-bottom:1px solid var(--bd);cursor:pointer;transition:.1s}
.tli:last-child{border-bottom:none}
.tli:hover{opacity:.75}
.tlt{font-size:11px;font-family:monospace;color:var(--tx2);min-width:38px;font-weight:600;padding-top:2px}
.tll{display:flex;flex-direction:column;align-items:center;flex-shrink:0}
.tld{width:8px;height:8px;border-radius:50%;margin-top:3px}
.tls2{width:1px;flex:1;background:var(--bd);min-height:6px;margin:3px 0}
.tlc{flex:1;min-width:0}
.tltit{font-size:12px;font-weight:500;color:var(--tx);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tlsub{font-size:10px;color:var(--tx3);margin-top:1px}

/* activity */
.acti{display:flex;gap:7px;padding:6px 0;border-bottom:1px solid var(--bd);font-size:12px}
.acti:last-child{border-bottom:none}
.actav{width:20px;height:20px;border-radius:50%;flex-shrink:0;display:flex;
  align-items:center;justify-content:center;font-size:8px;font-weight:700;color:#fff;margin-top:1px}
.actx{flex:1;color:var(--tx2);line-height:1.4}
.acttp{color:var(--blue);cursor:pointer;font-weight:500}
.actts{color:var(--tx3);font-size:10px;white-space:nowrap}
</style>
</head>
<body>

<!-- TOPBAR -->
<div id="tb">
  <div class="logo">NOC <b>Platform</b></div>
  <div class="divv"></div>
  <div id="datenav">
    <button class="dbtn" onclick="changeDay(-1)">&#8249;</button>
    <div id="dlabel" onclick="promptDate()"></div>
    <button class="dbtn" onclick="changeDay(1)">&#8250;</button>
  </div>
  <div style="display:flex;align-items:center;gap:6px;padding:4px 10px;
    border-radius:var(--r);border:1px solid var(--bd);background:var(--w)">
    <div id="sync-dot"></div>
    <span id="synclbl">Initialisation…</span>
  </div>
  <div class="sp"></div>
  <button class="tbtn tb-ghost" onclick="manualSync()">⟳ Sync</button>
  <button class="tbtn tb-ghost" onclick="exportCSV()">↓ Export</button>
  <button class="tbtn tb-primary" onclick="openModal('mc')">+ Nouveau TP</button>
</div>

<!-- ALERT -->
<div id="alert">
  <span id="alrt"></span>
  <button class="alert-x" onclick="document.getElementById('alert').classList.remove('on')">&#215;</button>
</div>

<!-- SHELL -->
<div id="shell">
  <!-- SIDEBAR -->
  <aside id="sb">
    <div class="sb-nav">
      <div class="nv on" data-v="dash" onclick="sv('dash')">
        <svg fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/></svg>Tableau de bord
      </div>
      <div class="nv" data-v="tps" onclick="sv('tps')">
        <svg fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></svg>Tous les TP
      </div>
    </div>
    <div class="sbd"></div>
    <div class="sb-hd">
      <span class="sb-lbl">Aujourd'hui</span>
      <button class="sb-add" onclick="openModal('mc')" title="Créer">+</button>
    </div>
    <input id="sbsearch" placeholder="Filtrer…" oninput="renderSB()">
    <div class="chips" id="chips">
      <span class="chip on" data-f="">Tous</span>
      <span class="chip" data-f="En cours">En cours</span>
      <span class="chip" data-f="Incident">Incident</span>
      <span class="chip" data-f="Terminé">Terminé</span>
    </div>
    <div id="sblist"><div class="empty" style="padding:24px;font-size:12px">Chargement…</div></div>
  </aside>

  <!-- CONTENT -->
  <div id="cnt">
    <div id="views">

      <!-- DASHBOARD -->
      <div class="view on" id="view-dash">
        <div class="vhd">
          <div><div class="vtitle">Tableau de bord</div><div class="vsub" id="dashsub"></div></div>
          <div style="display:flex;gap:7px">
            <button class="btn bd bsm" onclick="exportCSV()">Exporter CSV</button>
            <button class="btn bd bsm" onclick="openModal('mc')">+ Créer un TP</button>
          </div>
        </div>
        <div class="sg" id="stats"></div>
        <div class="dg">
          <div class="panel">
            <div class="phd"><span class="plbl">Timeline</span></div>
            <div class="pb" id="timeline" style="max-height:380px;overflow-y:auto"></div>
          </div>
          <div class="panel">
            <div class="phd"><span class="plbl">Activité récente</span></div>
            <div class="pb" id="activity" style="max-height:380px;overflow-y:auto"></div>
          </div>
        </div>
      </div>

      <!-- TPS LIST -->
      <div class="view" id="view-tps">
        <div class="vhd">
          <div class="vtitle">Travaux Programmés</div>
          <button class="btn bp bsm" onclick="openModal('mc')">+ Nouveau TP</button>
        </div>
        <div class="tc">
          <div class="ttb">
            <input class="tls" placeholder="Rechercher…" oninput="renderTbl()">
            <select class="tlsel" onchange="renderTbl()">
              <option value="">Tous les statuts</option>
              <option>En attente</option><option>En cours</option>
              <option>Incident</option><option>Terminé</option><option>Annulé</option>
            </select>
            <div style="flex:1"></div>
            <button class="btn bd bsm" onclick="exportCSV()">Export CSV</button>
          </div>
          <table><thead><tr>
            <th style="width:80px">Heure</th><th>Titre</th><th style="width:150px">Opérateur</th>
            <th style="width:70px;text-align:center">Clients</th>
            <th style="width:70px;text-align:center">Messages</th>
            <th style="width:110px">Statut</th>
          </tr></thead><tbody id="tbl"></tbody></table>
        </div>
      </div>

      <!-- DETAIL -->
      <div class="view" id="view-detail">
        <div id="dpwrap" style="display:flex;height:100%">
          <div class="empty" style="flex:1">Sélectionnez un TP</div>
        </div>
      </div>

    </div>
  </div>
</div>

<!-- MODAL CREATE -->
<div class="ov" id="mc">
  <div class="modal">
    <div class="mhd"><span class="mtitle">Créer un Travail Programmé</span><button class="mx" onclick="closeModal('mc')">&#215;</button></div>
    <div class="mbody">
      <div class="fg"><label class="fl">Titre</label><input id="f-title" class="fc" placeholder="TP-COVAGE INFRA-2026…"></div>
      <div class="fr">
        <div class="fg"><label class="fl">Date</label><input type="date" id="f-date" class="fc"></div>
        <div class="fg"><label class="fl">Priorité</label>
          <select id="f-prio" class="fc"><option>Normale</option><option>Urgente</option><option>Critique</option></select>
        </div>
      </div>
      <div class="fr">
        <div class="fg"><label class="fl">Début</label><input type="time" id="f-ts" class="fc"></div>
        <div class="fg"><label class="fl">Fin</label><input type="time" id="f-te" class="fc"></div>
      </div>
      <div class="fg"><label class="fl">Opérateur</label><input id="f-op" class="fc" placeholder="COVAGE INFRA…"></div>
      <div class="fg"><label class="fl">Description</label><textarea id="f-desc" class="fc" rows="2"></textarea></div>
      <div class="fg">
        <label class="fl">Service IDs clients</label>
        <textarea id="f-cl" class="fc" rows="4" placeholder="FR048380&#10;AC015467&#10;FR057100"></textarea>
        <div class="fhint">FR/AC/FO + 6 chiffres, un par ligne</div>
      </div>
    </div>
    <div class="mft">
      <button class="btn bd" onclick="closeModal('mc')">Annuler</button>
      <button class="btn bp" onclick="submitCreate()">Créer</button>
    </div>
  </div>
</div>

<!-- MODAL ADD CLIENTS -->
<div class="ov" id="mcl">
  <div class="modal">
    <div class="mhd"><span class="mtitle">Ajouter des clients</span><button class="mx" onclick="closeModal('mcl')">&#215;</button></div>
    <div class="mbody">
      <p style="font-size:13px;color:var(--tx2);margin-bottom:10px">Collez les Service IDs (FR/AC/FO…)</p>
      <textarea id="cl-in" class="fc" rows="8" placeholder="FR048380&#10;AC015467&#10;…" style="width:100%"></textarea>
    </div>
    <div class="mft">
      <button class="btn bd" onclick="closeModal('mcl')">Annuler</button>
      <button class="btn bp" onclick="submitClients()">Ajouter</button>
    </div>
  </div>
</div>

<!-- MODAL AUTOCHECK -->
<div class="ov" id="mac">
  <div class="modal lg">
    <div class="mhd"><span class="mtitle">AutoCheck – Vérification clients</span><button class="mx" onclick="closeModal('mac')">&#215;</button></div>
    <div class="mbody" id="ac-body"></div>
    <div class="mft">
      <button class="btn bd" onclick="closeModal('mac')">Fermer</button>
      <button class="btn bp" onclick="copyACIds()">Copier IDs SecureCRT</button>
    </div>
  </div>
</div>

<div id="toast"></div>

<script>
// ── CONSTANTS ─────────────────────────────────────────────
const STC = {'En attente':'#94a3b8','En cours':'#2563eb','Incident':'#dc2626','Terminé':'#16a34a','Annulé':'#cbd5e1'};
const SVC = /\b(AC\d{6}|FR\d{6}|FO\d{6}|FON\d{5,6}|SF\d{6}|BB\d{5,6}|BK\d{5,6}|CN\d{5,6})\b/gi;
const STATUTS = ['En attente','En cours','Incident','Terminé','Annulé'];
const USER = localStorage.getItem('noc_user') || (() => {
  const n = prompt('Votre prénom (affiché dans les messages) :') || 'NOC';
  localStorage.setItem('noc_user', n); return n;
})();

// ── STATE ──────────────────────────────────────────────────
let TPS=[], CUR_TP=null, CUR_DATE=new Date().toISOString().slice(0,10), SB_FILTER='';
let _msgPoll=null;

// ── HELPERS ────────────────────────────────────────────────
const esc = s => String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
const fmtTs = ts => { try{return new Date(ts).toLocaleString('fr-FR',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'})}catch{return ''} };
const fmtT = t => (t||'--:--').slice(0,5);
const avc = n => { const c=['#2563eb','#16a34a','#d97706','#7c3aed','#db2777','#0891b2']; let h=0; for(const x of n||'?') h=(h*31+x.charCodeAt(0))&0xffff; return c[h%c.length]; };
const ini = n => (n||'?').split(/[\s@]/)[0].slice(0,2).toUpperCase();
const mkSt = s => `<span class="st" data-s="${esc(s)}"><div class="sd"></div>${esc(s||'—')}</span>`;
const now = () => new Date().toISOString();

function toast(msg) {
  const t=document.getElementById('toast');
  t.textContent=msg; t.classList.add('on');
  clearTimeout(t._t); t._t=setTimeout(()=>t.classList.remove('on'),3000);
}
function openModal(id){document.getElementById(id).classList.add('on')}
function closeModal(id){document.getElementById(id).classList.remove('on')}
document.addEventListener('keydown',e=>{if(e.key==='Escape')document.querySelectorAll('.ov.on').forEach(o=>o.classList.remove('on'))});

// ── DATE NAV ───────────────────────────────────────────────
function updateDateLabel(){
  const d=new Date(CUR_DATE+'T12:00:00');
  const isT=CUR_DATE===new Date().toISOString().slice(0,10);
  document.getElementById('dlabel').textContent = isT ? `Aujourd'hui · ${d.toLocaleDateString('fr-FR',{day:'numeric',month:'short'})}` : d.toLocaleDateString('fr-FR',{weekday:'short',day:'numeric',month:'short'});
}
function changeDay(n){
  const d=new Date(CUR_DATE+'T12:00:00'); d.setDate(d.getDate()+n);
  CUR_DATE=d.toISOString().slice(0,10); updateDateLabel(); loadTPs();
}
function promptDate(){
  const v=prompt('Date (YYYY-MM-DD):',CUR_DATE);
  if(v&&/^\d{4}-\d{2}-\d{2}$/.test(v)){CUR_DATE=v;updateDateLabel();loadTPs();}
}

// ── API ─────────────────────────────────────────────────────
async function api(path, method='GET', body=null){
  const opts={method, headers:{'Content-Type':'application/json'}};
  if(body) opts.body=JSON.stringify(body);
  const r=await fetch(path, opts);
  return r.json();
}

// ── LOAD TPs ────────────────────────────────────────────────
async function loadTPs(){
  const data=await api(`/api/tps?date=${CUR_DATE}`).catch(()=>[]);
  TPS=Array.isArray(data)?data:[];
  TPS.sort((a,b)=>(a.time_start||'').localeCompare(b.time_start||''));
  renderSB(); renderDash(); renderTbl();
  checkAlerts();
}

// ── REAL-TIME SSE ────────────────────────────────────────────
function initSSE(){
  const es=new EventSource('/api/events');
  es.onmessage=e=>{
    const d=JSON.parse(e.data);
    if(d.type==='tps_updated'&&(!d.date||d.date===CUR_DATE)) loadTPs();
    if(d.type==='tp_updated'&&d.tp_id===CUR_TP?.id){
      api(`/api/tp/${CUR_TP.id}`).then(tp=>{CUR_TP=tp;renderDetail(tp);});
      loadTPs();
    }
    if(d.type==='message'&&d.tp_id===CUR_TP?.id) loadMessages(CUR_TP.id);
  };
  es.onerror=()=>setTimeout(initSSE,3000);
}

// ── SYNC STATUS ──────────────────────────────────────────────
async function checkSyncStatus(){
  const d=await api('/api/status').catch(()=>null);
  if(!d) return;
  const dot=document.getElementById('sync-dot'), lbl=document.getElementById('synclbl');
  if(d.sync?.status==='ok'){
    const t=new Date(d.sync.ts).toLocaleTimeString('fr-FR',{hour:'2-digit',minute:'2-digit'});
    dot.className='on'; lbl.textContent=`Sync auto · ${t} · ${d.sync.count||0} TP`;
  } else if(d.sync?.status==='error'){
    dot.className='err'; lbl.textContent='Erreur sync Webtool';
  } else {
    dot.className='spin'; lbl.textContent='Sync en cours…';
  }
}

async function manualSync(){
  document.getElementById('sync-dot').className='spin';
  document.getElementById('synclbl').textContent='Synchronisation…';
  const d=await api('/api/sync','POST',{date:CUR_DATE}).catch(e=>({ok:false,error:e.message}));
  if(d.ok){toast(`Sync OK · ${d.count} TP(s)`);await loadTPs();}
  else toast('Erreur sync: '+d.error);
  checkSyncStatus();
}

// ── RENDER SIDEBAR ───────────────────────────────────────────
function renderSB(){
  const q=(document.getElementById('sbsearch')?.value||'').toLowerCase();
  const f=SB_FILTER;
  const ord={Incident:0,'En cours':1,'En attente':2,'Terminé':3,'Annulé':4};
  const items=TPS.filter(t=>(!f||t.status===f)&&(!q||t.title.toLowerCase().includes(q)||t.id.toLowerCase().includes(q)))
    .sort((a,b)=>(ord[a.status||'En attente']||2)-(ord[b.status||'En attente']||2)||(a.time_start||'').localeCompare(b.time_start||''));
  const list=document.getElementById('sblist');
  if(!items.length){list.innerHTML='<div class="empty" style="padding:20px;font-size:12px">Aucun TP</div>';return;}
  list.innerHTML=items.map(tp=>{
    const col=STC[tp.status||'En attente'],cls=tp.status==='Incident'?' incident':tp.id===CUR_TP?.id?' on':'';
    return `<div class="si${cls}" onclick="selTP('${tp.id}')">
      <div class="sdot" style="background:${col}"></div>
      <div class="si-r">
        <div class="si-title">${esc(tp.title)}</div>
        <div class="si-meta">${fmtT(tp.time_start)}${tp.time_end?' – '+fmtT(tp.time_end):''}${tp.client_count?' · '+tp.client_count+' cl.':''}</div>
      </div>
    </div>`;
  }).join('');
  document.querySelectorAll('#chips .chip').forEach(c=>c.onclick=()=>{SB_FILTER=c.dataset.f;document.querySelectorAll('#chips .chip').forEach(x=>x.classList.toggle('on',x===c));renderSB();});
}

// ── RENDER DASHBOARD ─────────────────────────────────────────
async function renderDash(){
  const d=new Date(CUR_DATE+'T12:00:00').toLocaleDateString('fr-FR',{weekday:'long',day:'numeric',month:'long',year:'numeric'});
  document.getElementById('dashsub').textContent=d;
  const cnt={};STATUTS.forEach(s=>cnt[s]=0);TPS.forEach(t=>cnt[t.status||'En attente']++);
  document.getElementById('stats').innerHTML=[
    {n:'Total',v:TPS.length,s:'TP programmés',c:'var(--tx)'},
    {n:'En cours',v:cnt['En cours'],s:cnt['Incident']+' incident(s)',c:cnt['En cours']?'var(--blue)':'var(--tx)'},
    {n:'En attente',v:cnt['En attente'],s:'à démarrer',c:'var(--tx)'},
    {n:'Terminés',v:cnt['Terminé'],s:'complétés',c:cnt['Terminé']?'var(--green)':'var(--tx)'},
    {n:'Clients',v:TPS.reduce((a,t)=>a+(t.client_count||0),0),s:'services impactés',c:'var(--amber)'},
  ].map(({n,v,s,c})=>`<div class="sc"><div class="scn">${n}</div><div class="scv" style="color:${c}">${v}</div><div class="scs">${s}</div></div>`).join('');
  const sorted=[...TPS].sort((a,b)=>(a.time_start||'').localeCompare(b.time_start||''));
  document.getElementById('timeline').innerHTML=sorted.length
    ?sorted.map((tp,i)=>`<div class="tli" onclick="selTP('${tp.id}')">
        <div class="tlt">${fmtT(tp.time_start)}</div>
        <div class="tll"><div class="tld" style="background:${STC[tp.status||'En attente']}"></div>${i<sorted.length-1?'<div class="tls2"></div>':''}</div>
        <div class="tlc"><div class="tltit">${esc(tp.title)}</div><div class="tlsub">${mkSt(tp.status||'En attente')}${tp.operator?' · '+esc(tp.operator):''}</div></div>
      </div>`).join('')
    :'<div class="empty" style="padding:20px;font-size:12px">Aucun TP ce jour</div>';
  const acts=await api('/api/activity').catch(()=>[]);
  document.getElementById('activity').innerHTML=acts.length
    ?acts.map(a=>`<div class="acti">
        <div class="actav" style="background:${avc(a.user_name)}">${ini(a.user_name)}</div>
        <div class="actx"><span class="acttp" onclick="selTP('${a.tp_id}')">${esc(a.tp_id)}</span> – ${esc(a.action||'')}</div>
        <div class="actts">${fmtTs(a.ts)}</div>
      </div>`).join('')
    :'<div class="empty" style="padding:20px;font-size:12px">Aucune activité</div>';
}

// ── RENDER TABLE ─────────────────────────────────────────────
function renderTbl(){
  const q=(document.querySelector('.tls')?.value||'').toLowerCase();
  const sf=document.querySelector('.tlsel')?.value||'';
  const items=TPS.filter(t=>(!sf||t.status===sf)&&(!q||t.title.toLowerCase().includes(q)||t.id.toLowerCase().includes(q)));
  document.getElementById('tbl').innerHTML=items.length
    ?items.map(tp=>`<tr onclick="selTP('${tp.id}')" ${tp.id===CUR_TP?.id?'style="background:var(--blue-bg)"':''}>
        <td class="mono">${fmtT(tp.time_start)}</td>
        <td><div style="font-weight:500">${esc(tp.title)}</div><div style="font-size:10px;color:var(--tx3);font-family:monospace">${tp.id}</div></td>
        <td class="tlight">${esc(tp.operator||'—')}</td>
        <td style="text-align:center" class="tlight">${tp.client_count||0}</td>
        <td style="text-align:center" class="tlight">${tp.msg_count||0}</td>
        <td>${mkSt(tp.status||'En attente')}</td>
      </tr>`).join('')
    :'<tr><td colspan="6"><div class="empty" style="padding:32px">Aucun TP</div></td></tr>';
}

// ── SELECT TP ────────────────────────────────────────────────
async function selTP(id){
  const tp=await api(`/api/tp/${id}`);
  CUR_TP=tp; sv('detail'); renderSB(); renderTbl();
  renderDetail(tp);
}

async function renderDetail(tp){
  if(!tp) return;
  const clients=(tp.clients||[]);
  const up=clients.filter(c=>c.check_status==='up').length;
  const down=clients.filter(c=>c.check_status==='down').length;
  const done=clients.filter(c=>c.check_status!=='pending').length;
  document.getElementById('dpwrap').innerHTML=`
  <div class="dpl">
    <div class="dphd">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px">
        <div style="flex:1;min-width:0">
          <div class="dpid">${tp.id}</div>
          <div class="dptitle">${esc(tp.title)}</div>
        </div>
        <div>${mkSt(tp.status||'En attente')}</div>
      </div>
      <div class="dpmetas">
        ${tp.time_start?`<div class="dpm">${fmtT(tp.time_start)}${tp.time_end?' → '+fmtT(tp.time_end):''}</div>`:''}
        ${tp.operator?`<div class="dpm">· ${esc(tp.operator)}</div>`:''}
        ${tp.date?`<div class="dpm">· ${tp.date}</div>`:''}
        ${tp.description?`<div class="dpm" style="flex-basis:100%;color:var(--tx2)">${esc(tp.description)}</div>`:''}
      </div>
    </div>
    <div id="mthread" class="mthread"></div>
    <div class="compose">
      <div class="cbox">
        <textarea class="cta" id="cta" placeholder="Message…" rows="1"></textarea>
        <button class="csend" id="csend">Envoyer</button>
      </div>
      <div class="chint">Entrée = envoyer · Maj+Entrée = saut de ligne</div>
    </div>
  </div>
  <div class="dpr">
    <div class="rp">
      <div class="rph">Statut</div>
      <div class="stgrid">
        ${STATUTS.map(s=>`<button class="stbtn${tp.status===s?' on':''}" data-s="${s}" onclick="setStatus('${tp.id}','${s}')">
          <div class="sd" style="width:7px;height:7px;border-radius:50%;background:${STC[s]};flex-shrink:0"></div>${s}
        </button>`).join('')}
      </div>
    </div>
    <div class="rp">
      <div class="rph">Informations</div>
      <div class="ir"><span class="irl">ID</span><span class="irv" style="font-family:monospace;font-size:10px;color:var(--blue)">${tp.id}</span></div>
      <div class="ir"><span class="irl">Créé par</span><span class="irv">${esc(tp.created_by||'—')}</span></div>
      <div class="ir"><span class="irl">Opérateur</span><span class="irv">${esc(tp.operator||'—')}</span></div>
      <div class="ir"><span class="irl">Créneau</span><span class="irv">${fmtT(tp.time_start)}${tp.time_end?' → '+fmtT(tp.time_end):''}</span></div>
    </div>
    <div class="rp">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
        <div class="rph" style="margin-bottom:0">Clients (${clients.length})</div>
        <div style="display:flex;gap:4px">
          <button class="btn bd bsm" onclick="openAddCl('${tp.id}')">Ajouter</button>
          <button class="btn bg bsm" onclick="openAC('${tp.id}')">AutoCheck</button>
        </div>
      </div>
      ${clients.length?`<div style="margin-bottom:6px">
        <div class="pgw"><div class="pgb" style="width:${Math.round(done*100/clients.length)}%"></div></div>
        <div style="display:flex;justify-content:space-between;font-size:10px;color:var(--tx3);margin-top:2px">
          <span>${done}/${clients.length}</span><span style="color:var(--green)">${up} UP</span><span style="color:var(--red)">${down} DOWN</span>
        </div>
      </div>
      ${clients.map(c=>`<div class="cr">
        <span class="cid">${c.service_id}</span>
        <select class="cst ${c.check_status}" style="background:transparent;border:none;font-size:10px;font-weight:700;color:inherit;cursor:pointer"
          onchange="updClient('${tp.id}','${c.service_id}',this.value);this.className='cst '+this.value">
          <option value="pending" ${c.check_status==='pending'?'selected':''}>—</option>
          <option value="up" ${c.check_status==='up'?'selected':''}>UP</option>
          <option value="down" ${c.check_status==='down'?'selected':''}>DOWN</option>
          <option value="error" ${c.check_status==='error'?'selected':''}>ECHEC</option>
        </select>
      </div>`).join('')}`
      :'<div class="empty" style="padding:14px;font-size:12px">Aucun client</div>'}
    </div>
    <div class="rp">
      <div class="rph">Notes</div>
      <textarea class="rta" id="notes-ta" placeholder="Notes internes…" onblur="saveNotes('${tp.id}')">${esc(tp.notes||'')}</textarea>
    </div>
  </div>`;
  loadMessages(tp.id);
  setupCompose(tp.id);
}

// ── MESSAGES ─────────────────────────────────────────────────
async function loadMessages(tpId){
  const msgs=await api(`/api/messages/${tpId}`).catch(()=>[]);
  const zone=document.getElementById('mthread');
  if(!zone) return;
  if(!msgs.length){zone.innerHTML='<div class="empty" style="padding:28px;font-size:12px">Pas encore de message</div>';return;}
  let html='',lastUser='',lastTs=0;
  msgs.forEach(m=>{
    const mine=m.user_name===USER;
    const gap=(new Date(m.created_at)-lastTs)>240000;
    if(m.type==='system'){
      html+=`<div class="msys"><span class="mchip">${esc(m.content)}</span></div>`;
      lastUser=''; lastTs=new Date(m.created_at);
    } else {
      const newG=m.user_name!==lastUser||gap;
      if(newG) html+=`<div class="mgrp"><div class="mgrhd">
        <div class="mav" style="background:${avc(m.user_name)}">${ini(m.user_name)}</div>
        <span class="mauth">${esc(m.user_name)}</span>
        <span class="mts">${fmtTs(m.created_at)}</span>
      </div>`;
      html+=`<div class="mbody"${mine?' style="color:#1d4ed8"':''}>${esc(m.content).replace(/\n/g,'<br>')}</div>`;
      if(newG) html+='</div>';
      lastUser=m.user_name; lastTs=new Date(m.created_at);
    }
  });
  zone.innerHTML=html; zone.scrollTop=zone.scrollHeight;
}

function setupCompose(tpId){
  const ta=document.getElementById('cta'),btn=document.getElementById('csend');
  if(!ta||!btn) return;
  const send=async()=>{ const txt=ta.value.trim(); if(!txt) return; ta.value=''; ta.style.height='34px'; await api('/api/messages','POST',{tp_id:tpId,content:txt,user:USER}); loadMessages(tpId); loadTPs(); };
  btn.onclick=send;
  ta.onkeydown=e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();send();}};
  ta.oninput=()=>{ta.style.height='34px';ta.style.height=Math.min(ta.scrollHeight,100)+'px';};
}

// ── ACTIONS ───────────────────────────────────────────────────
async function setStatus(tpId,status){
  await api(`/api/tp/${tpId}`,'PATCH',{status,user:USER});
  await selTP(tpId); loadTPs(); toast('Statut → '+status);
}
async function saveNotes(tpId){
  const v=document.getElementById('notes-ta')?.value||'';
  await api(`/api/tp/${tpId}`,'PATCH',{notes:v,user:USER}).catch(()=>{});
}
function openAddCl(tpId){
  document.getElementById('cl-in').value='';
  document.getElementById('cl-in').dataset.tpId=tpId;
  openModal('mcl');
}
async function submitClients(){
  const tpId=document.getElementById('cl-in').dataset.tpId;
  const raw=document.getElementById('cl-in').value;
  const d=await api(`/api/clients/${tpId}`,'POST',{ids_raw:raw,user:USER});
  if(d.ok){closeModal('mcl');toast(`${d.added} client(s) ajoutés`);await selTP(tpId);loadTPs();}
  else toast('Erreur: '+d.error);
}
async function updClient(tpId,sid,status){
  await api(`/api/clients/${tpId}/${sid}`,'PATCH',{check_status:status,user:USER});
}
async function openAC(tpId){
  const tp=await api(`/api/tp/${tpId}`);
  const cl=tp.clients||[];
  if(!cl.length){toast('Aucun client');return;}
  const ids=cl.map(c=>c.service_id);
  document.getElementById('ac-body').innerHTML=`
    <p style="font-size:13px;color:var(--tx2);margin-bottom:12px"><strong>${ids.length}</strong> clients – copiez les IDs dans <code>autocheck_multi2.py</code></p>
    <div style="background:var(--bg);border:1px solid var(--bd);border-radius:var(--r);padding:10px;font-family:monospace;font-size:12px;color:var(--blue);max-height:180px;overflow-y:auto;white-space:pre;margin-bottom:14px">${ids.join('\n')}</div>
    <div style="display:flex;flex-direction:column;gap:3px;max-height:220px;overflow-y:auto">
      ${cl.map(c=>`<div style="display:flex;align-items:center;gap:8px;padding:3px 0;border-bottom:1px solid var(--bd)">
        <span style="font-family:monospace;font-size:11px;color:var(--blue);font-weight:600;min-width:100px">${c.service_id}</span>
        <select onchange="updClient('${tpId}','${c.service_id}',this.value);renderDetail(CUR_TP)"
          style="background:var(--bg);border:1px solid var(--bd);color:var(--tx);padding:2px 5px;border-radius:4px;font-size:11px">
          <option value="pending" ${c.check_status==='pending'?'selected':''}>— En attente</option>
          <option value="up" ${c.check_status==='up'?'selected':''}>UP</option>
          <option value="down" ${c.check_status==='down'?'selected':''}>DOWN</option>
          <option value="error" ${c.check_status==='error'?'selected':''}>ECHEC</option>
        </select>
      </div>`).join('')}
    </div>`;
  document.getElementById('mac').dataset.tpId=tpId;
  openModal('mac');
}
function copyACIds(){
  const tpId=document.getElementById('mac').dataset.tpId;
  api(`/api/tp/${tpId}`).then(tp=>{
    const ids=(tp.clients||[]).map(c=>c.service_id).join('\n');
    navigator.clipboard.writeText(ids).then(()=>toast('IDs copiés'));
  });
}

// ── CREATE ────────────────────────────────────────────────────
function openModal_mc(){document.getElementById('f-date').value=CUR_DATE;openModal('mc');}
async function submitCreate(){
  const title=document.getElementById('f-title').value.trim();
  if(!title){toast('Titre requis');return;}
  const d=await api('/api/tp','POST',{
    title, date:document.getElementById('f-date').value,
    time_start:document.getElementById('f-ts').value,
    time_end:document.getElementById('f-te').value,
    operator:document.getElementById('f-op').value.trim(),
    description:document.getElementById('f-desc').value.trim(),
    priority:document.getElementById('f-prio').value,
    clients_raw:document.getElementById('f-cl').value, user:USER,
  });
  if(d.ok){closeModal('mc');['f-title','f-ts','f-te','f-op','f-desc','f-cl'].forEach(i=>{const el=document.getElementById(i);if(el)el.value='';}); await loadTPs();toast('TP créé');await selTP(d.id);}
  else toast('Erreur: '+d.error);
}

// ── NAV ────────────────────────────────────────────────────────
function sv(v){
  document.querySelectorAll('.view').forEach(el=>el.classList.toggle('on',el.id==='view-'+v));
  document.querySelectorAll('.nv[data-v]').forEach(el=>el.classList.toggle('on',el.dataset.v===v));
  if(v!=='detail'&&_msgPoll){clearInterval(_msgPoll);_msgPoll=null;}
}

// ── ALERTS ────────────────────────────────────────────────────
function checkAlerts(){
  const now2=new Date(),bar=document.getElementById('alert');
  const imm=TPS.filter(tp=>{
    if(['Terminé','Annulé'].includes(tp.status)) return false;
    if(!tp.time_start) return false;
    const [h,m]=tp.time_start.split(':').map(Number);
    const d=new Date(now2.getFullYear(),now2.getMonth(),now2.getDate(),h,m);
    const df=(d-now2)/60000;
    return df>-15&&df<31;
  });
  if(imm.length){
    document.getElementById('alrt').innerHTML=`${imm.length} TP(s) imminents : ${imm.map(t=>`<strong style="cursor:pointer" onclick="selTP('${t.id}')">${t.id}</strong>`).join(', ')}`;
    bar.classList.add('on');
  } else bar.classList.remove('on');
}

// ── EXPORT ────────────────────────────────────────────────────
function exportCSV(){
  const h=['ID','Titre','Date','Début','Fin','Opérateur','Statut','Priorité','Clients','Messages'];
  const rows=TPS.map(t=>[t.id,t.title,t.date,t.time_start||'',t.time_end||'',t.operator||'',t.status||'',t.priority||'',t.client_count||0,t.msg_count||0]);
  const csv=[h,...rows].map(r=>r.map(c=>'"'+String(c).replace(/"/g,'""')+'"').join(';')).join('\n');
  const a=document.createElement('a');a.href='data:text/csv;charset=utf-8,\uFEFF'+encodeURIComponent(csv);
  a.download=`TP_NOC_${CUR_DATE}.csv`;a.click();toast('Export téléchargé');
}

// ── SIDEBAR CHIPS ─────────────────────────────────────────────
document.getElementById('chips').addEventListener('click',e=>{
  const c=e.target.closest('.chip');if(!c)return;
  SB_FILTER=c.dataset.f;
  document.querySelectorAll('#chips .chip').forEach(x=>x.classList.toggle('on',x===c));
  renderSB();
});

// ── INIT ──────────────────────────────────────────────────────
updateDateLabel();
loadTPs();
initSSE();
setInterval(checkAlerts,60000);
setInterval(checkSyncStatus,15000);
checkSyncStatus();
document.getElementById('f-date') && (document.getElementById('f-date').value=CUR_DATE);
</script>
</body>
</html>"""

@app.route('/')
def index():
    return render_template_string(FRONTEND)

# ═══════════════════════════════════════════════════════════
# START
# ═══════════════════════════════════════════════════════════
if __name__ == '__main__':
    print(f"""
╔═══════════════════════════════════════════╗
║  NOC Platform                             ║
╠═══════════════════════════════════════════╣
║  URL    : http://0.0.0.0:{PORT}              ║
║  Sync   : Webtool toutes les {SYNC_MIN} min       ║
╚═══════════════════════════════════════════╝
""")
    # Sync immédiate au démarrage
    threading.Thread(target=background_sync, daemon=True).start()
    app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False, threaded=True)
