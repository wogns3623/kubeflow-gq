#!/usr/bin/env python3
"""gqd: the gq daemon (one per user, "instance").

The only gq component that holds a Kubernetes token. It reads requests the gq CLI
drops into <root>/spool, validates them, picks a node and creates Jobs. It never
executes or imports anything from the PVC: it only parses request.json and writes
status files. See DESIGN.md (sections 5 and 7). Deployed by gqctl.

    python3 -I gqd.py --config config.json --root /gq [--dry-run] [--once]
"""
import argparse
import datetime as dt
import json
import logging
import math
import os
import posixpath
import re
import shlex
import shutil
import stat
import time

VERSION = "0.1.0"
GPU = "nvidia.com/gpu"
ID_RE = re.compile(r"^\d{6}-\d{6}-[0-9a-f]{4}(-\d{1,3})?$")
NAME_RE = re.compile(r"^[A-Za-z0-9._-]{0,63}$")
OWNER_RE = re.compile(r"^[A-Za-z0-9._:@-]{0,63}$")
INSTANCE_RE = re.compile(r"^[a-z0-9]([a-z0-9-]{0,14}[a-z0-9])?$")
TERMINAL = frozenset({"rejected", "expired", "succeeded", "failed", "timeout", "lost", "killed"})
ACTIVE = frozenset({"pending", "running"})
REQ_KEYS = frozenset({"id", "group", "task_index", "task_count", "name", "owner", "backend",
                      "gpus", "cpu", "mem", "time", "wait_timeout", "cwd", "submitted", "after"})
REUSE_GRACE_MAX = 300  # hard ceiling for reuse_grace; a config above it is an error (DESIGN 15.2)
MAX_REQ_BYTES = 64 * 1024

REQUIRED = ("instance", "namespace", "image", "volumes", "root")
DEFAULTS = {
    # Per-user keys (REQUIRED) come from `gqctl init`; see examples/.
    "service_account": "default-editor",
    "pod_labels": {},
    # Nodes gqd may place on. Empty = scheduler-placement mode: no per-node fit or pinning,
    # only gq's own limits; the Kubernetes scheduler picks the node.
    "nodes": [],
    "node_selector": {},  # optional nodeSelector for Job pods (e.g. a GPU node pool label)
    "node_capacity": {"gpu": 8, "cpu": 125, "mem": "1964Gi"},
    "cpu_job_default": {"cpu": 8, "mem": "64Gi"},
    "requests": {"cpu": 1, "mem": "4Gi"},
    "placement_grace": 60,
    # Seconds a finished GPU pod waits for a same-shape job to take it over (0 = exit at once).
    "reuse_grace": 60,
    "deadline_slack": 1800,
    "max_gpus_total": 4,
    "max_running": 8,
    "max_queue": 32,
    "max_array": 64,
    "max_time": 172800,
    "max_wait_timeout": 86400,
    "max_gpus_per_job": 8,
    "job_ttl_after_finished": 3600,
    "retention_days": 30,        # job directories (request.json, status.json)
    "log_retention_days": 1,     # job output (log) and run.sh, which holds the captured environment
    "event_retention_days": 365,  # <root>/events/YYYY-MM.jsonl, gq's own record (DESIGN 16)
    "loop_interval": 2,
    "metrics_interval": 30,
    "gc_interval": 3600,
}

log = logging.getLogger("gqd")


class Invalid(ValueError):
    """A request that gqd refuses to run."""


# ---------------------------------------------------------------- small helpers

def now_utc():
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ") if t else None


def parse_ts(s):
    if not s:
        return None
    return dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)


_MEM_UNITS = {"": 1, "k": 10**3, "K": 10**3, "M": 10**6, "G": 10**9, "T": 10**12, "P": 10**15,
              "Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40, "Pi": 2**50}


def parse_mem(q):
    """Kubernetes memory quantity -> bytes."""
    if isinstance(q, (int, float)) and not isinstance(q, bool):
        return int(q)
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(e\d+|[KMGTP]i|[kKMGTP])?", str(q).strip())
    if not m:
        raise ValueError("bad memory quantity: %r" % (q,))
    num, unit = float(m.group(1)), m.group(2) or ""
    if unit.startswith("e"):
        return int(num * 10 ** int(unit[1:]))
    return int(num * _MEM_UNITS[unit])


def parse_cpu(q):
    """Kubernetes CPU quantity -> cores."""
    if isinstance(q, (int, float)) and not isinstance(q, bool):
        return float(q)
    s = str(q).strip()
    for suffix, div in (("n", 1e9), ("u", 1e6), ("m", 1e3)):
        if s.endswith(suffix):
            return float(s[:-1]) / div
    return float(s)


def fmt_mem(b):
    return "%dGi" % (b // 2**30) if b % 2**30 == 0 else "%dMi" % math.ceil(b / 2**20)


def fmt_cpu(c):
    return str(int(c)) if float(c).is_integer() else "%dm" % round(c * 1000)


def read_small(path, limit):
    """Read a small file without following a symlink at the last component."""
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as f:
        data = f.read(limit + 1)
    if len(data) > limit:
        raise Invalid("%s is larger than %d bytes" % (os.path.basename(path), limit))
    return data


def is_real_dir(path):
    try:
        return stat.S_ISDIR(os.lstat(path).st_mode)
    except FileNotFoundError:
        return False


def write_json(path, obj):
    """Atomic write that never follows symlinks (agents can write in these directories)."""
    d, base = os.path.split(path)
    if not is_real_dir(d):
        raise OSError("not a real directory: %s" % d)
    tmp = os.path.join(d, ".%s.%d.tmp" % (base, os.getpid()))
    try:
        os.unlink(tmp)
    except FileNotFoundError:
        pass
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
    with os.fdopen(fd, "w") as f:
        json.dump(obj, f, indent=1)
        f.write("\n")
    os.replace(tmp, path)  # rename replaces a symlink itself, never its target


def unlink_quiet(path):
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def job_name(instance, rid, attempt):
    return "gq-%s-%s-a%d" % (instance, rid, attempt)


def job_name_re(instance):
    return re.compile(r"^gq-%s-\d{6}-\d{6}-[0-9a-f]{4}(-\d{1,3})?-a\d+$" % re.escape(instance))


def _longest(path, vols):
    norm = posixpath.normpath(path)
    best = None
    for v in vols:
        m = posixpath.normpath(v["mountPath"])
        if (norm == m or norm.startswith(m.rstrip("/") + "/")) and (
                best is None or len(m) > len(posixpath.normpath(best["mountPath"]))):
            best = v
    return best


def mount_of(path, cfg):
    """The volume Job pods mount that holds path, or None.

    None also when, in the notebook, path is on an excluded volume (ReadWriteOnce/local)
    nested deeper than any included one: the Job would see a different directory there."""
    inc = _longest(path, cfg["volumes"])
    exc = _longest(path, cfg.get("excluded_volumes") or [])
    if exc and (inc is None or len(posixpath.normpath(exc["mountPath"])) > len(posixpath.normpath(inc["mountPath"]))):
        return None
    return inc


def excluded_of(path, cfg):
    """The excluded volume that shadows path, if that is why mount_of() is None."""
    exc = _longest(path, cfg.get("excluded_volumes") or [])
    return exc if exc and mount_of(path, cfg) is None else None


# ---------------------------------------------------------------- config

def load_config(path=None, **overrides):
    cfg = dict(DEFAULTS)
    if path:
        with open(path) as f:
            cfg.update(json.load(f))
    cfg.update(overrides)
    missing = [k for k in REQUIRED if not cfg.get(k)]
    if missing:
        raise ValueError("config is missing %s (run `gqctl init`)" % ", ".join(missing))
    if not INSTANCE_RE.match(cfg["instance"]):
        raise ValueError("instance must be 1-16 of [a-z0-9-], starting and ending alphanumeric")
    for v in list(cfg["volumes"]) + list(cfg.get("excluded_volumes") or []):
        if not (isinstance(v, dict) and v.get("pvc") and str(v.get("mountPath", "")).startswith("/")):
            raise ValueError("each volume needs 'pvc' and an absolute 'mountPath': %r" % (v,))
    if mount_of(cfg["root"], cfg) is None:
        raise ValueError("root %s is not on any configured volume" % cfg["root"])
    cfg["root"] = posixpath.normpath(cfg["root"])
    cap = cfg["node_capacity"]
    cfg["_node"] = {"gpu": int(cap["gpu"]), "cpu": parse_cpu(cap["cpu"]), "mem": parse_mem(cap["mem"])}
    d = cfg["cpu_job_default"]
    cfg["_cpu_job"] = {"cpu": parse_cpu(d["cpu"]), "mem": parse_mem(d["mem"])}
    r = cfg["requests"]
    cfg["_req"] = {"cpu": parse_cpu(r["cpu"]), "mem": parse_mem(r["mem"])}
    g = cfg["reuse_grace"]
    if isinstance(g, bool) or not isinstance(g, int) or not 0 <= g <= REUSE_GRACE_MAX:
        raise ValueError("reuse_grace must be an integer in [0, %d] seconds" % REUSE_GRACE_MAX)
    return cfg


# ---------------------------------------------------------------- pure logic

def validate(req, cfg):
    """Return a normalized copy of a request or raise Invalid. This is the trust boundary."""
    if not isinstance(req, dict):
        raise Invalid("request must be a JSON object")
    extra = set(req) - REQ_KEYS
    if extra:
        raise Invalid("unknown fields: %s" % ", ".join(sorted(extra)))

    def num(key, kind=int):
        v = req.get(key)
        if isinstance(v, bool) or not isinstance(v, kind):
            raise Invalid("%s: expected a number" % key)
        return v

    rid = req.get("id")
    if not isinstance(rid, str) or not ID_RE.match(rid):
        raise Invalid("id: bad format")
    out = {"id": rid}

    group = req.get("group")
    if group is not None:
        if not isinstance(group, str) or not ID_RE.match(group) or not rid.startswith(group + "-"):
            raise Invalid("group: must be the id prefix")
        idx, cnt = num("task_index"), num("task_count")
        if not (1 <= cnt <= cfg["max_array"] and 0 <= idx < cnt):
            raise Invalid("task_index/task_count out of range (max_array %d)" % cfg["max_array"])
        out.update(group=group, task_index=idx, task_count=cnt)
    elif "task_index" in req or "task_count" in req:
        raise Invalid("task_index/task_count need group")

    name, owner = req.get("name") or "", req.get("owner") or ""
    if not isinstance(name, str) or not NAME_RE.match(name):
        raise Invalid("name: up to 63 characters of [A-Za-z0-9._-]")
    if not isinstance(owner, str) or not OWNER_RE.match(owner):
        raise Invalid("owner: up to 63 characters of [A-Za-z0-9._:@-]")
    out.update(name=name, owner=owner)

    if req.get("backend") != "pod":
        raise Invalid("backend: gqd only runs pod jobs")

    g = req.get("gpus")
    if isinstance(g, int) and not isinstance(g, bool):
        g = [g, g]
    if not (isinstance(g, list) and len(g) == 2
            and all(isinstance(x, int) and not isinstance(x, bool) for x in g)):
        raise Invalid("gpus: expected [min, max]")
    lo, hi = g
    max_g = min(cfg["max_gpus_per_job"], cfg["_node"]["gpu"])
    if not (0 <= lo <= hi <= max_g):
        raise Invalid("gpus: need 0 <= min <= max <= %d" % max_g)
    if lo == 0 and hi > 0:
        raise Invalid("gpus: a range cannot start at 0")
    out["gpus"] = [lo, hi]

    cpu, mem = req.get("cpu"), req.get("mem")
    if cpu is not None:
        cpu = float(num("cpu", (int, float)))
        if not 0 < cpu <= cfg["_node"]["cpu"]:
            raise Invalid("cpu: must be in (0, %g]" % cfg["_node"]["cpu"])
    if mem is not None:
        try:
            mem = parse_mem(mem)
        except ValueError as e:
            raise Invalid("mem: %s" % e)
        if not 0 < mem <= cfg["_node"]["mem"]:
            raise Invalid("mem: must be in (0, %s]" % fmt_mem(cfg["_node"]["mem"]))
    out.update(cpu=cpu, mem=mem)

    t = num("time")
    w = None if req.get("wait_timeout") is None else num("wait_timeout")  # None: wait until it starts or is killed
    if not 1 <= t <= cfg["max_time"]:
        raise Invalid("time: must be in [1, %d] seconds" % cfg["max_time"])
    if w is not None and not 1 <= w <= cfg["max_wait_timeout"]:
        raise Invalid("wait_timeout: must be in [1, %d] seconds" % cfg["max_wait_timeout"])
    out.update(time=t, wait_timeout=w)

    cwd = req.get("cwd")
    if not isinstance(cwd, str) or not cwd.startswith("/"):
        raise Invalid("cwd: expected an absolute path")
    if mount_of(cwd, cfg) is None:
        ex = excluded_of(cwd, cfg)
        if ex:
            raise Invalid("cwd: %s is on PVC %s (%s), which Job pods cannot mount; work on %s instead"
                          % (cwd, ex["pvc"], ",".join(ex.get("accessModes") or ["not shareable"]),
                             ", ".join(v["mountPath"] for v in cfg["volumes"])))
        raise Invalid("cwd: must be on a volume the Job mounts (%s)"
                      % ", ".join(v["mountPath"] for v in cfg["volumes"]))
    out["cwd"] = posixpath.normpath(cwd)

    try:
        if parse_ts(req.get("submitted")) is None:
            raise ValueError
    except (TypeError, ValueError):
        raise Invalid("submitted: expected YYYY-MM-DDTHH:MM:SSZ")
    out["submitted"] = req["submitted"]

    after = req.get("after")
    if after is not None:
        if not (isinstance(after, list) and 1 <= len(after) <= cfg["max_array"]
                and all(isinstance(x, str) and ID_RE.match(x) for x in after)):
            raise Invalid("after: expected 1-%d job ids" % cfg["max_array"])
        if rid in after:
            raise Invalid("after: a job cannot wait for itself")
        out["after"] = sorted(set(after))
    return out


def requests_for(cpu, mem, cfg):
    """Scheduler requests for a job with these limits: small and fixed, like the notebooks here.

    Placement only has to find room for the requests, so GPUs decide where a job fits;
    the limits let it burst into idle CPU/memory on the node."""
    return min(cfg["_req"]["cpu"], cpu), min(cfg["_req"]["mem"], mem)


def resources(req, gpus, cfg):
    """CPU cores and memory bytes (limits) for a request that gets `gpus` GPUs."""
    n = cfg["_node"]
    if gpus > 0:
        cpu = req["cpu"] if req["cpu"] is not None else math.floor(n["cpu"] / n["gpu"]) * gpus
        mem = req["mem"] if req["mem"] is not None else (n["mem"] // n["gpu"] // 2**30) * 2**30 * gpus
    else:
        cpu = req["cpu"] if req["cpu"] is not None else cfg["_cpu_job"]["cpu"]
        mem = req["mem"] if req["mem"] is not None else cfg["_cpu_job"]["mem"]
    return float(cpu), int(mem)


def pod_gpus(pod):
    total = 0
    for c in (pod.get("spec") or {}).get("containers") or []:
        r = c.get("resources") or {}
        v = (r.get("limits") or {}).get(GPU) or (r.get("requests") or {}).get(GPU)
        total += int(v or 0)
    return total


def pod_requests(pod):
    """Scheduler view: requests of containers plus native sidecars."""
    spec = pod.get("spec") or {}
    conts = list(spec.get("containers") or [])
    conts += [c for c in spec.get("initContainers") or [] if c.get("restartPolicy") == "Always"]
    cpu, mem = 0.0, 0
    for c in conts:
        req = (c.get("resources") or {}).get("requests") or {}
        try:
            cpu += parse_cpu(req.get("cpu", 0))
            mem += parse_mem(req.get("memory", 0))
        except ValueError:
            log.warning("unparseable requests in pod %s", pod["metadata"].get("name"))
    return cpu, mem


def job_condition(job, kind):
    for c in (job.get("status") or {}).get("conditions") or []:
        if c.get("type") == kind and c.get("status") == "True":
            return c
    return None


def job_finished(job):
    return bool(job_condition(job, "Complete") or job_condition(job, "Failed"))


def pod_job_name(pod):
    labels = pod["metadata"].get("labels") or {}
    return labels.get("batch.kubernetes.io/job-name") or labels.get("job-name")


def capacity(pods, jobs, cfg, metrics=None):
    """Estimate free resources per configured node.

    Counts every pod in the namespace (not only gq's) that is bound to a configured node
    and not finished, plus gq Jobs whose pod is not bound yet (reserved on their pinned
    node, so one loop cannot place two jobs into the same slot)."""
    n = cfg["_node"]
    nodes = {name: {"gpu_used": 0, "cpu_used": 0.0, "mem_used": 0, "cpu_actual": 0.0}
             for name in cfg["nodes"]}
    waiting, bound_jobs = 0, set()
    for p in pods:
        phase = (p.get("status") or {}).get("phase")
        if phase in ("Succeeded", "Failed"):
            continue
        labels = p["metadata"].get("labels") or {}
        node = (p.get("spec") or {}).get("nodeName")
        is_gq = labels.get("gq") == "1"
        if not node:
            if not is_gq:
                waiting += pod_gpus(p)
            continue
        if is_gq:
            bound_jobs.add(pod_job_name(p))
        if node not in nodes:
            continue
        cpu, mem = pod_requests(p)
        nd = nodes[node]
        nd["gpu_used"] += pod_gpus(p)
        nd["cpu_used"] += cpu
        nd["mem_used"] += mem
        if metrics:
            nd["cpu_actual"] += metrics.get(p["metadata"]["name"], 0.0)
    for j in jobs:
        if job_finished(j) or j["metadata"]["name"] in bound_jobs:
            continue
        a = j["metadata"].get("annotations") or {}
        nd = nodes.get(a.get("gq/node"))
        if nd is not None:
            nd["gpu_used"] += int(a.get("gq/gpus", 0))
            nd["cpu_used"] += float(a.get("gq/cpu-request", 0))
            nd["mem_used"] += int(a.get("gq/mem-request", 0))
    out = []
    for name, nd in nodes.items():
        out.append({
            "name": name,
            "gpu_free": max(0, n["gpu"] - nd["gpu_used"]),
            "gpu_used": nd["gpu_used"],
            "cpu_req_free": max(0.0, n["cpu"] - nd["cpu_used"]),
            "mem_req_free": max(0, n["mem"] - nd["mem_used"]),
            "cpu_actual": round(nd["cpu_actual"], 1) if metrics is not None else None,
        })
    return {"nodes": out, "gpu_waiting": waiting}


def reusable(req, pod, cfg):
    """Can req take over this idle pod? Same GPU count within its range, CPU/memory limits fit."""
    lo, hi = req["gpus"]
    if not lo <= pod["gpus"] <= hi:
        return False
    cpu, mem = resources(req, pod["gpus"], cfg)
    return cpu <= pod["cpu"] and mem <= pod["mem"]


def place(queue, nodes, cfg, running, gq_gpus, idle=()):
    """Decide which queued requests start now.

    queue: validated requests in FIFO order. nodes: capacity()["nodes"]. running / gq_gpus:
    gq jobs and GPUs already active, idle pods included. idle: finished GPU pods waiting to be
    taken over ({"job", "node", "gpus", "cpu", "mem", ...}). Returns (decisions, nodes_after)
    where each decision is (req, {"node", "gpus", "cpu", "mem"[, "reuse": idle pod]}) or
    (req, {"reason", "message"}).

    An idle pod is tried first: taking it over adds nothing to the caps or the nodes.

    FIFO with backfill: a later request may start when an earlier one does not fit.
    GPU jobs go to the fullest node that still fits (best fit); CPU jobs go to the node
    with the fewest free GPUs, keeping CPU on GPU-rich nodes for GPU jobs. CPU/memory
    fit is checked on requests (what the scheduler counts), not limits.

    Without configured nodes (scheduler-placement mode) only gq's limits apply and the
    decision has node None; a GPU range takes as many GPUs as the limits allow."""
    # ponytail: no reservation for big jobs, so they can starve under steady small-job
    # traffic. If that happens, stop backfilling onto the node closest to fitting the
    # head-of-line job once it has waited N minutes.
    nodes = [dict(nd) for nd in nodes]
    idle = list(idle)
    decisions = []
    for req in queue:
        lo, hi = req["gpus"]
        pod = next((p for p in idle if reusable(req, p, cfg)), None)
        if pod is not None:
            idle.remove(pod)
            decisions.append((req, {"node": pod["node"], "gpus": pod["gpus"], "cpu": pod["cpu"],
                                    "mem": pod["mem"], "reuse": pod}))
            continue
        if running >= cfg["max_running"]:
            decisions.append((req, {"reason": "Cap", "message": "gq already runs %d jobs (max_running %d)"
                                    % (running, cfg["max_running"])}))
            continue
        top = min(hi, cfg["max_gpus_total"] - gq_gpus)
        if top < lo:
            decisions.append((req, {"reason": "Cap", "message": "gq GPUs in use %d/%d; this job needs %d"
                                    % (gq_gpus, cfg["max_gpus_total"], lo)}))
            continue
        if not cfg["nodes"]:
            cpu, mem = resources(req, top, cfg)
            gq_gpus += top
            running += 1
            decisions.append((req, {"node": None, "gpus": top, "cpu": cpu, "mem": mem}))
            continue
        choice = None
        for g in range(top, lo - 1, -1):
            cpu, mem = resources(req, g, cfg)
            rcpu, rmem = requests_for(cpu, mem, cfg)
            fits = [nd for nd in nodes
                    if nd["gpu_free"] >= g and nd["cpu_req_free"] >= rcpu and nd["mem_req_free"] >= rmem]
            if fits:
                choice = (min(fits, key=lambda x: (x["gpu_free"], -x["cpu_req_free"])), g, cpu, mem, rcpu, rmem)
                break
        if choice is None:
            mx = max((nd["gpu_free"] for nd in nodes), default=0)
            tot = sum(nd["gpu_free"] for nd in nodes)
            if lo > 0:
                need = str(lo) if lo == hi else "%d-%d" % (lo, hi)
                msg = "%s GPUs needed on one node; max free on a node is %d (total free %d)" % (need, mx, tot)
            else:
                msg = "no node has %g CPU and %s free (requests)" % (rcpu, fmt_mem(rmem))
            decisions.append((req, {"reason": "NoFit", "message": msg}))
            continue
        nd, g, cpu, mem, rcpu, rmem = choice
        nd["gpu_free"] -= g
        nd["cpu_req_free"] -= rcpu
        nd["mem_req_free"] -= rmem
        gq_gpus += g
        running += 1
        decisions.append((req, {"node": nd["name"], "gpus": g, "cpu": cpu, "mem": mem}))
    return decisions, nodes


# The Job's command (DESIGN 15.1). Runs a job, then waits up to `grace` seconds for gqd to hand
# it the next one in <job dir>/next ("<id> [<task_index> <task_count>]"). Giving up, it claims
# `next` itself with an exclusive create, so it and gqd cannot both win. Exits with the last rc.
WORKER = r"""set -f; root=%(root)s; grace=%(grace)d; id=$GQ_ID
idre='^[0-9]{6}-[0-9]{6}-[0-9a-f]{4}(-[0-9]{1,3})?$'
while :; do
  d=$root/jobs/$id
  date -u +%%Y-%%m-%%dT%%H:%%M:%%SZ >"$d/started"
  bash "$d/run.sh" >"$d/log" 2>&1; rc=$?
  g=$grace
  if [ $$ -eq 1 ]; then  # pod's PID 1: kill what the job left running so the next job gets a clean GPU
    kill -9 -1 2>/dev/null
    for ((i = 0; i < 50; i++)); do
      left=; set +f; for p in /proc/[0-9]*; do [ "${p#/proc/}" = 1 ] || left=1; done; set -f
      [ -z "$left" ] && break; sleep 0.1
    done
    [ -z "$left" ] || g=0  # something would not die: end the pod instead of handing it over
  fi
  echo $rc >"$d/exit"
  [ "$g" -gt 0 ] || exit $rc
  end=$((SECONDS + grace)); line=
  while :; do
    if [ -s "$d/next" ] || { [ $SECONDS -ge $end ] && ! (set -C; echo closed >"$d/next") 2>/dev/null; }; then
      for _ in 1 2 3 4 5; do read -r line <"$d/next" && break; sleep 1; done
      break
    fi
    [ $SECONDS -ge $end ] && exit $rc
    sleep 1
  done
  set -- $line
  [[ ${1:-} =~ $idre ]] || exit $rc
  id=$1; export GQ_ID=$1
  if [[ ${2:-} =~ ^[0-9]+$ && ${3:-} =~ ^[0-9]+$ ]]; then export GQ_TASK_INDEX=$2 GQ_TASK_COUNT=$3
  else unset GQ_TASK_INDEX GQ_TASK_COUNT; fi
done"""


def build_job(req, node, gpus, cpu, mem, attempt, cfg):
    """The only Job shape gqd ever creates. Command and user env stay on the PVC (run.sh).

    cpu/mem are limits; requests come from requests_for(). The pod mounts every volume of
    the user's notebook at the same path, so paths and files look the same as there."""
    rid, inst = req["id"], cfg["instance"]
    rcpu, rmem = requests_for(cpu, mem, cfg)
    labels = {"gq": "1", "gq-instance": inst, "gq-id": rid}
    env = [{"name": "GQ_ID", "value": rid},
           {"name": "GQ_GPUS", "value": str(gpus)},
           {"name": "GQ_CPU", "value": str(max(1, math.floor(cpu)))}]
    if req.get("group"):
        labels["gq-group"] = req["group"]
        env += [{"name": "GQ_TASK_INDEX", "value": str(req["task_index"])},
                {"name": "GQ_TASK_COUNT", "value": str(req["task_count"])}]
    limits = {"cpu": fmt_cpu(cpu), "memory": fmt_mem(mem)}
    if gpus:
        limits[GPU] = str(gpus)
    wrapper = WORKER % {"root": shlex.quote(cfg["root"]), "grace": cfg["reuse_grace"]}
    mounts, volumes = [], []
    for i, v in enumerate(cfg["volumes"]):
        m = {"name": "v%d" % i, "mountPath": v["mountPath"]}
        if v.get("subPath"):
            m["subPath"] = v["subPath"]
        if v.get("readOnly"):
            m["readOnly"] = True
        mounts.append(m)
        volumes.append({"name": "v%d" % i, "persistentVolumeClaim": {"claimName": v["pvc"]}})
    job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name(inst, rid, attempt),
            "labels": labels,
            "annotations": {"gq/name": req["name"], "gq/owner": req["owner"], "gq/node": node or "",
                            "gq/gpus": str(gpus), "gq/cpu": str(cpu), "gq/mem": str(mem),
                            "gq/cpu-request": str(rcpu), "gq/mem-request": str(rmem),
                            "gq/attempt": str(attempt), "gq/reuse-grace": str(cfg["reuse_grace"])},
        },
        "spec": {
            "backoffLimit": 0,
            # Backstop only: gqd enforces `time` from container start (DESIGN 5.4).
            "activeDeadlineSeconds": req["time"] + cfg["placement_grace"] + cfg["deadline_slack"],
            "ttlSecondsAfterFinished": cfg["job_ttl_after_finished"],
            "template": {
                "metadata": {"labels": dict(labels, **cfg["pod_labels"])},
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "enableServiceLinks": False,
                    "containers": [{
                        "name": "main",
                        "image": cfg["image"],
                        "command": ["bash", "-lc", wrapper],
                        "env": env,
                        "resources": {"requests": {"cpu": fmt_cpu(rcpu), "memory": fmt_mem(rmem)},
                                      "limits": limits},
                        "volumeMounts": mounts + [{"name": "dshm", "mountPath": "/dev/shm"}],
                    }],
                    "volumes": volumes + [{"name": "dshm", "emptyDir": {"medium": "Memory"}}],
                },
            },
        },
    }
    pod = job["spec"]["template"]["spec"]
    if node:  # pin to the node gqd chose; matchFields uses the node name itself, not a label
        pod["affinity"] = {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [{"matchFields": [{"key": "metadata.name", "operator": "In", "values": [node]}]}]}}}
    if cfg["node_selector"]:
        pod["nodeSelector"] = dict(cfg["node_selector"])
    return job


def main_running(pod):
    cs = next((c for c in (pod or {}).get("status", {}).get("containerStatuses") or [] if c.get("name") == "main"), None)
    return "running" in ((cs or {}).get("state") or {})


def derive_state(job, pod, exit_code, started=None):
    """State of a placed pod job from what Kubernetes and the PVC show.

    exit_code / started: the job's `exit` and `started` files. The exit file wins even while
    the container still runs: the worker may be waiting to take over the next job (DESIGN 15).
    Returns a dict with state, reason, message, started, finished, exit_code, node.
    Kills and time limits are applied by the loop, not here."""
    def r(state, reason=None, message=None, started=None, finished=None, code=None, node=None):
        return {"state": state, "reason": reason, "message": message, "started": started,
                "finished": finished, "exit_code": code, "node": node}

    if job is not None:
        failed = job_condition(job, "Failed")
        if failed and failed.get("reason") == "DeadlineExceeded":
            return r("timeout", "DeadlineExceeded", "backstop activeDeadlineSeconds reached",
                     node=pod and pod["spec"].get("nodeName"))
    if pod is not None:
        node = pod["spec"].get("nodeName")
        status = pod.get("status") or {}
        cs = next((c for c in status.get("containerStatuses") or [] if c.get("name") == "main"), None)
        st = (cs or {}).get("state") or {}
        if exit_code is not None and "running" in st:
            return r("succeeded" if exit_code == 0 else "failed", None if exit_code == 0 else "Error",
                     started=started or st["running"].get("startedAt"), code=exit_code, node=node)
        if "terminated" in st:
            t = st["terminated"]
            code = exit_code if exit_code is not None else t.get("exitCode")
            if code == 0:
                return r("succeeded", started=started or t.get("startedAt"), finished=t.get("finishedAt"), code=0,
                         node=node)
            return r("failed", t.get("reason") or "Error", t.get("message"), started or t.get("startedAt"),
                     t.get("finishedAt"), code, node)
        if "running" in st:
            return r("running", started=started or st["running"].get("startedAt"), node=node)
        if status.get("phase") == "Failed":  # evicted or killed before main ran
            if exit_code is not None:
                return r("succeeded" if exit_code == 0 else "failed", code=exit_code, node=node)
            return r("lost", status.get("reason") or "PodFailed", status.get("message"), node=node)
        sched = next((c for c in status.get("conditions") or [] if c.get("type") == "PodScheduled"), None)
        if sched and sched.get("status") == "False":
            return r("pending", sched.get("reason") or "Unschedulable", sched.get("message"), node=node)
        waiting = st.get("waiting") or {}
        return r("pending", waiting.get("reason") or "Starting", waiting.get("message"), node=node)
    if exit_code is not None:
        return r("succeeded" if exit_code == 0 else "failed", None if exit_code == 0 else "Error", code=exit_code)
    if job is not None:
        failed = job_condition(job, "Failed")
        if failed:
            return r("lost", failed.get("reason") or "JobFailed", failed.get("message"))
        if job_condition(job, "Complete"):
            return r("lost", "NoExitFile", "job completed but no exit file was found")
        return r("pending", "Creating")
    return r("lost", "Gone", "job and pod disappeared without an exit code")


# ---------------------------------------------------------------- Kubernetes I/O

class Kube:
    """Thin wrapper over the Kubernetes client. Returns plain dicts (kubectl JSON shape)."""

    def __init__(self, namespace, instance, dry_run=False):
        from kubernetes import client, config
        from kubernetes.client.rest import ApiException
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self.ns, self.dry_run, self.ApiException = namespace, dry_run, ApiException
        self.own_job = job_name_re(instance)
        self.core, self.batch = client.CoreV1Api(), client.BatchV1Api()
        self.custom = client.CustomObjectsApi()
        self._fake = {}  # dry-run: Jobs we pretend to have created

    def list_pods(self):
        r = self.core.list_namespaced_pod(self.ns, _preload_content=False)
        return json.loads(r.data)["items"]

    def list_jobs(self):
        # All gq instances in the namespace: their unbound Jobs reserve capacity for everyone.
        r = self.batch.list_namespaced_job(self.ns, label_selector="gq=1", _preload_content=False)
        return json.loads(r.data)["items"] + list(self._fake.values())

    def pod_metrics(self):
        r = self.custom.list_namespaced_custom_object("metrics.k8s.io", "v1beta1", self.ns, "pods")
        return {i["metadata"]["name"]: sum(parse_cpu(c["usage"]["cpu"]) for c in i.get("containers", []))
                for i in r.get("items", [])}

    def create_job(self, body):
        """True if created, False if a Job with that name already exists."""
        try:
            self.batch.create_namespaced_job(self.ns, body, dry_run="All" if self.dry_run else None)
        except self.ApiException as e:
            if e.status == 409:
                return False
            raise
        if self.dry_run:
            self._fake[body["metadata"]["name"]] = dict(body, status={})
            log.info("dry-run: Job %s accepted by the API server", body["metadata"]["name"])
        return True

    def patch_job_deadline(self, name, seconds):
        if not self.own_job.match(name):
            raise ValueError("refusing to patch a Job this instance did not create: %r" % name)
        if name in self._fake:
            self._fake[name]["spec"]["activeDeadlineSeconds"] = seconds
            return
        self.batch.patch_namespaced_job(name, self.ns, {"spec": {"activeDeadlineSeconds": seconds}},
                                        dry_run="All" if self.dry_run else None)

    def delete_job(self, name):
        if not self.own_job.match(name):
            raise ValueError("refusing to delete a Job this instance did not create: %r" % name)
        if self._fake.pop(name, None) is not None:
            return
        try:
            self.batch.delete_namespaced_job(name, self.ns, propagation_policy="Background")
        except self.ApiException as e:
            if e.status != 404:
                raise


# ---------------------------------------------------------------- daemon

class Daemon:
    def __init__(self, cfg, root, kube, clock=now_utc):
        self.cfg, self.root, self.kube, self.clock = cfg, root, kube, clock
        self.spool = os.path.join(root, "spool")
        self.jobs_dir = os.path.join(root, "jobs")
        self.events_dir = os.path.join(root, "events")
        for d in (self.spool, self.jobs_dir, self.events_dir):
            os.makedirs(d, exist_ok=True)
        self.tracked = {}   # id -> record (status.json content plus private "_" keys)
        self._written = {}  # id -> last status.json text
        self._metrics, self._metrics_at, self._gc_at = None, 0.0, 0.0
        self.idle = {}      # job name -> finished GPU pod waiting for a takeover (DESIGN 15.2)
        self.pods = {}      # job name -> {"created", "node", "gpus", "tasks"} for this instance's live Jobs
        self._own = job_name_re(cfg["instance"])
        self._ended = self._logged_pod_ends()  # Jobs whose pod_ended is already in the event log
        self._recover()

    # -- files

    def _dir(self, rid):
        return os.path.join(self.jobs_dir, rid)

    def _read_json(self, rid, name):
        return json.loads(read_small(os.path.join(self._dir(rid), name), MAX_REQ_BYTES))

    def _exit_code(self, rid):
        try:
            return int(read_small(os.path.join(self._dir(rid), "exit"), 32).strip())
        except (OSError, ValueError, Invalid):
            return None

    # -- event log (DESIGN 16): append-only, written only by gqd

    def _event(self, kind, **fields):
        now = self.clock()
        doc = {"ts": iso(now), "event": kind}
        doc.update({k: v for k, v in fields.items() if v is not None})
        path = os.path.join(self.events_dir, now.strftime("%Y-%m") + ".jsonl")
        try:
            fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o644)
            try:
                os.write(fd, (json.dumps(doc, sort_keys=True) + "\n").encode())
            finally:
                os.close(fd)
        except OSError as e:
            log.warning("cannot write event %s: %s", kind, e)

    def _logged_pod_ends(self):
        """Job names with a pod_ended in this or last month's log, so a restart does not log them twice."""
        now = self.clock()
        months = {now.strftime("%Y-%m"), (now.replace(day=1) - dt.timedelta(days=1)).strftime("%Y-%m")}
        names = set()
        for m in months:
            try:
                with open(os.path.join(self.events_dir, m + ".jsonl"), errors="replace") as f:
                    for line in f:
                        if '"pod_ended"' in line:
                            try:
                                names.add(json.loads(line).get("job"))
                            except ValueError:
                                pass
            except OSError:
                pass
        return names

    def _queue(self, rec, reason, message, position=None):
        if rec.get("state") != "queued" or rec.get("reason") != reason:
            self._event("queued", id=rec["id"], reason=reason, message=message)
        rec.update(state="queued", reason=reason, message=message, queue_position=position)

    def _track_pods(self, jobs, pods_by_job, now):
        """Log pod_ended once per Job: when it completes/fails, or disappears (deleted)."""
        listed = {}
        for j in jobs:
            name = j["metadata"]["name"]
            if not self._own.match(name):
                continue
            listed[name] = j
            ann = j["metadata"].get("annotations") or {}
            if name not in self.pods and name not in self._ended:  # e.g. created before a restart
                try:
                    gpus = int(ann.get("gq/gpus") or 0)
                except ValueError:
                    gpus = None
                self.pods[name] = {"created": j["metadata"].get("creationTimestamp"), "node": ann.get("gq/node") or None,
                                   "gpus": gpus, "tasks": sorted(r["id"] for r in self.tracked.values()
                                                                 if r.get("job") == name)}
            pod = pods_by_job.get(name)
            if pod is not None and name in self.pods:
                self.pods[name]["node"] = pod["spec"].get("nodeName") or self.pods[name]["node"]
            if job_finished(j) and name not in self._ended:
                cond = job_condition(j, "Complete") or job_condition(j, "Failed")
                ended = (j.get("status") or {}).get("completionTime") or cond.get("lastTransitionTime") or iso(now)
                self._pod_ended(name, ended, cond.get("reason") or cond["type"])
        for name in list(self.pods):
            if name not in listed:
                self._pod_ended(name, iso(now), "Deleted")
        self._ended &= set(listed)

    def _pod_ended(self, name, ended, outcome):
        p = self.pods.pop(name, None) or {}
        created, end = parse_ts(p.get("created")), parse_ts(ended)
        self._event("pod_ended", job=name, node=p.get("node"), gpus=p.get("gpus"), created=p.get("created"),
                    ended=ended, seconds=int((end - created).total_seconds()) if created and end else None,
                    tasks=p.get("tasks"), outcome=outcome)
        self._ended.add(name)
        self.idle.pop(name, None)

    def _started(self, rid):
        try:
            text = read_small(os.path.join(self._dir(rid), "started"), 64).decode().strip()
            return text if parse_ts(text) else None
        except (OSError, ValueError, Invalid):
            return None

    def _done(self, rid):
        """Has job rid ended (any terminal state)? A directory already gc'ed counts as ended."""
        rec = self.tracked.get(rid)
        if rec is not None:
            return rec["state"] in TERMINAL
        if not is_real_dir(self._dir(rid)):
            return True
        try:
            return self._read_json(rid, "status.json").get("state") in TERMINAL
        except (OSError, ValueError, Invalid):
            return False

    def _markers(self, kind):
        out = []
        for name in os.listdir(self.spool):
            rid, _, ext = name.partition(".")
            if ext == kind and ID_RE.match(rid):
                out.append(rid)
        return sorted(out)

    def _write_status(self, rec):
        rec["updated"] = iso(self.clock())
        public = {k: v for k, v in rec.items() if not k.startswith("_")}
        text = json.dumps({k: v for k, v in public.items() if k != "updated"}, sort_keys=True)
        if self._written.get(rec["id"]) == text:
            return
        try:
            write_json(os.path.join(self._dir(rec["id"]), "status.json"), public)
            self._written[rec["id"]] = text
        except OSError as e:
            log.warning("cannot write status for %s: %s", rec["id"], e)

    def _recover(self):
        for rid in os.listdir(self.jobs_dir):
            if not ID_RE.match(rid) or not is_real_dir(self._dir(rid)):
                continue
            try:
                rec = self._read_json(rid, "status.json")
            except (OSError, ValueError, Invalid):
                continue
            queued_ok = rec.get("state") != "queued" or os.path.exists(os.path.join(self.spool, rid + ".submit"))
            if rec.get("backend") == "pod" and rec.get("state") in ACTIVE | {"queued"} and rec.get("id") == rid and queued_ok:
                try:
                    rec["_req"] = validate(self._read_json(rid, "request.json"), self.cfg)
                except (OSError, ValueError, Invalid):
                    continue
                self.tracked[rid] = rec
        log.info("recovered %d active jobs", len(self.tracked))

    # -- one loop

    def loop_once(self):
        now = self.clock()
        self._heartbeat(now)
        pods, jobs = self.kube.list_pods(), self.kube.list_jobs()
        metrics = self._get_metrics()
        jobs_by_name = {j["metadata"]["name"]: j for j in jobs}
        pods_by_job = {}
        for p in pods:
            if (p["metadata"].get("labels") or {}).get("gq") != "1":
                continue
            jn = pod_job_name(p)
            prev = pods_by_job.get(jn)
            if prev is None or p["metadata"].get("creationTimestamp", "") >= prev["metadata"].get("creationTimestamp", ""):
                pods_by_job[jn] = p

        for rid in self._markers("kill"):
            self._kill(rid, now)
            unlink_quiet(os.path.join(self.spool, rid + ".kill"))

        for rec in list(self.tracked.values()):
            if rec["state"] in ACTIVE:
                self._update_placed(rec, jobs_by_name.get(rec.get("job")), pods_by_job.get(rec.get("job")), now)
        self._prune_idle(pods_by_job, now)
        self._track_pods(jobs, pods_by_job, now)

        cap = capacity(pods, jobs, self.cfg, metrics)
        queue = self._load_queue(now)
        active = [r for r in self.tracked.values() if r["state"] in ACTIVE]
        idle = list(self.idle.values())
        decisions, nodes_after = place(queue, cap["nodes"], self.cfg, len(active) + len(idle),
                                       sum(r.get("gpus") or 0 for r in active) + sum(p["gpus"] for p in idle), idle)
        position, blocked = 0, False
        for req, dec in decisions:
            rec = self.tracked[req["id"]]
            if "reuse" in dec:
                if not self._handoff(rec, req, dec["reuse"], jobs_by_name.get(dec["reuse"]["job"]), now):
                    position += 1
                    rec.update(queue_position=position)
            elif "node" in dec:
                self._create(rec, req, dec, now)
            else:
                position += 1
                blocked = blocked or dec["reason"] in ("Cap", "NoFit")
                self._queue(rec, dec["reason"], dec["message"], position)
        if blocked:  # an idle pod nobody took must not keep my next job waiting
            for name in list(self.idle):
                log.info("releasing idle pod %s: a queued job cannot use it", name)
                self.kube.delete_job(name)
                self._event("pod_released", job=name, after=self.idle[name]["rid"], reason="a queued job cannot use it")
                del self.idle[name]

        for rec in list(self.tracked.values()):
            self._write_status(rec)
            if rec["state"] in TERMINAL:
                del self.tracked[rec["id"]]
                self._written.pop(rec["id"], None)
        self._write_capacity(now, cap, nodes_after, position)
        if time.monotonic() - self._gc_at > self.cfg["gc_interval"]:
            self._gc_at = time.monotonic()
            self._gc(now)

    def _heartbeat(self, now):
        try:
            write_json(os.path.join(self.root, "gqd.heartbeat"),
                       {"ts": iso(now), "version": VERSION, "pid": os.getpid(), "instance": self.cfg["instance"],
                        "mounts": [v["mountPath"] for v in self.cfg["volumes"]],
                        "excluded": [v["mountPath"] for v in self.cfg.get("excluded_volumes") or []],
                        "dry_run": bool(getattr(self.kube, "dry_run", False))})
        except OSError as e:
            log.warning("cannot write heartbeat: %s", e)

    def _get_metrics(self):
        if time.monotonic() - self._metrics_at > self.cfg["metrics_interval"]:
            self._metrics_at = time.monotonic()
            try:
                self._metrics = self.kube.pod_metrics()
            except Exception as e:  # metrics are informational only
                log.warning("pod metrics unavailable: %s", e)
                self._metrics = None
        return self._metrics

    def _finish(self, rec, state, reason, message, now):
        rec.update(state=state, reason=reason, message=message, queue_position=None,
                   finished=rec.get("finished") or iso(now))
        sub, start, end = parse_ts(rec.get("submitted")), parse_ts(rec.get("started")), parse_ts(rec["finished"])
        self._event("finished", id=rec["id"], state=state, reason=reason, message=message,
                    exit_code=rec.get("exit_code"), job=rec.get("job"), node=rec.get("node"), gpus=rec.get("gpus"),
                    reused=rec.get("reused"), started=rec.get("started"),
                    ran_seconds=int((end - start).total_seconds()) if start and end else None,
                    waited_seconds=int((start - sub).total_seconds()) if start and sub else None)

    def _kill(self, rid, now):
        rec = self.tracked.get(rid)
        if rec is None:
            # Submitted and killed before gqd ever loaded it: drop the request.
            marker = os.path.join(self.spool, rid + ".submit")
            if os.path.exists(marker) and is_real_dir(self._dir(rid)):
                unlink_quiet(marker)
                rec = self.tracked[rid] = {"id": rid, "backend": "pod", "submitted": None}
                self._finish(rec, "killed", "Killed", "killed by gq kill before it was queued", now)
            return
        if rec["state"] in TERMINAL:
            return
        if rec.get("job"):
            self.kube.delete_job(rec["job"])
        unlink_quiet(os.path.join(self.spool, rid + ".submit"))
        self._finish(rec, "killed", "Killed", "killed by gq kill", now)

    def _update_placed(self, rec, job, pod, now):
        started = self._started(rec["id"])
        if rec.get("reused") and started is None:
            # Handed to an idle pod that has not started it yet. Never fall back to the container's
            # start time here: that belongs to the previous job and would time this one out.
            if main_running(pod) and (now - parse_ts(rec["placed"])).total_seconds() < self.cfg["reuse_grace"] + 60:
                return
            self._requeue(rec, "HandoffFailed", "the pod ended before taking this job; placing it again")
            return
        st = derive_state(job, pod, self._exit_code(rec["id"]), started)
        if st["state"] in TERMINAL:
            rec.update({k: v for k, v in st.items() if v is not None or k in ("reason", "message")})
            self._finish(rec, st["state"], st["reason"], st["message"], now)
            self._maybe_idle(rec, job, pod, now)
            return
        prev = rec["state"]
        rec.update(state=st["state"], reason=st["reason"], message=st["message"],
                   node=st["node"] or rec.get("node"), started=rec.get("started") or st["started"])
        if prev != "running" and rec["state"] == "running":
            self._event("started", id=rec["id"], job=rec.get("job"), node=rec["node"], gpus=rec.get("gpus"),
                        started=rec["started"], reused=rec.get("reused"))
        started, waiting_since = parse_ts(rec.get("started")), parse_ts(rec.get("ready") or rec["submitted"])
        if rec["state"] == "running" and started and (now - started).total_seconds() > rec["time"]:
            self.kube.delete_job(rec["job"])
            self._finish(rec, "timeout", "TimeLimit", "ran longer than --time (%ds)" % rec["time"], now)
        elif rec["state"] == "pending" and rec["wait_timeout"] and (now - waiting_since).total_seconds() > rec["wait_timeout"]:
            self.kube.delete_job(rec["job"])
            self._finish(rec, "expired", "WaitTimeout",
                         "did not start within --wait-timeout (%ds)" % rec["wait_timeout"], now)
        elif (rec["state"] == "pending" and rec["reason"] == "Unschedulable" and rec.get("pinned", True)
              and (now - parse_ts(rec["placed"])).total_seconds() > self.cfg["placement_grace"]):
            # Our capacity estimate was wrong (someone took the slot). Re-place on a new Job name.
            self.kube.delete_job(rec["job"])
            log.info("re-placing %s: %s", rec["id"], rec.get("message"))
            self._requeue(rec, "Replace", "lost the scheduling race; re-placing")

    def _requeue(self, rec, reason, message):
        self._event("requeued", id=rec["id"], reason=reason, message=message, job=rec.get("job"))
        rec.update(state="queued", reason=reason, message=message, job=None, node=None, gpus=None, cpu=None,
                   mem=None, cpu_request=None, mem_request=None, reused=None, pod_since=None, started=None,
                   attempt=rec.get("attempt", 1) + 1)
        os.close(os.open(os.path.join(self.spool, rec["id"] + ".submit"),
                         os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o644))

    def _maybe_idle(self, rec, job, pod, now):
        """A GPU job ended by its own exit while its pod still runs: the worker now waits for a takeover."""
        try:
            grace = int(((job or {}).get("metadata", {}).get("annotations") or {}).get("gq/reuse-grace") or 0)
        except ValueError:
            grace = 0
        if grace <= 0 or self.cfg["reuse_grace"] <= 0 or not rec.get("gpus") or rec.get("exit_code") is None \
                or not main_running(pod):
            return
        self.idle[rec["job"]] = {"job": rec["job"], "rid": rec["id"], "node": pod["spec"].get("nodeName"),
                                 "gpus": rec["gpus"], "cpu": float(rec["cpu"]), "mem": parse_mem(rec["mem"]),
                                 "since": now, "grace": grace, "pod_since": rec.get("pod_since") or rec.get("placed")}
        self._event("pod_idle", job=rec["job"], after=rec["id"], node=pod["spec"].get("nodeName"), grace=grace)

    def _prune_idle(self, pods_by_job, now):
        margin = 2 * self.cfg["loop_interval"] + 1  # the worker started waiting a little before we saw it
        for name, p in list(self.idle.items()):
            if not main_running(pods_by_job.get(name)) or (now - p["since"]).total_seconds() > p["grace"] - margin:
                del self.idle[name]

    def _handoff(self, rec, req, pod, job, now):
        """Give queued job rec to an idle pod: extend the Job's deadline, then write <prev>/next."""
        del self.idle[pod["job"]]  # used or unusable after this either way
        try:
            start = parse_ts(((job or {}).get("status") or {}).get("startTime")) or now
            old = ((job or {}).get("spec") or {}).get("activeDeadlineSeconds") or 0
            need = int((now - start).total_seconds()) + req["time"] + pod["grace"] + self.cfg["deadline_slack"]
            self.kube.patch_job_deadline(pod["job"], max(old, need))
        except Exception as e:
            log.warning("cannot extend %s for %s, not reusing it: %s", pod["job"], rec["id"], e)
            return False
        line = rec["id"] + (" %d %d" % (req["task_index"], req["task_count"]) if req.get("group") else "") + "\n"
        try:
            fd = os.open(os.path.join(self._dir(pod["rid"]), "next"),
                         os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
        except OSError as e:  # FileExistsError: the worker gave up first
            log.info("pod of %s closed before taking %s: %s", pod["rid"], rec["id"], e)
            return False
        try:
            os.write(fd, line.encode())
        finally:
            os.close(fd)
        rcpu, rmem = requests_for(pod["cpu"], pod["mem"], self.cfg)
        rec.update(state="pending", reason="Handoff", message="taking over the pod of %s" % pod["rid"],
                   queue_position=None, node=pod["node"], gpus=pod["gpus"], cpu=pod["cpu"], mem=fmt_mem(pod["mem"]),
                   cpu_request=rcpu, mem_request=fmt_mem(rmem), job=pod["job"], placed=iso(now), pinned=False,
                   reused=True, pod_since=pod["pod_since"])
        unlink_quiet(os.path.join(self.spool, rec["id"] + ".submit"))
        self.pods.setdefault(pod["job"], {"created": None, "node": pod["node"], "gpus": pod["gpus"], "tasks": []})
        self.pods[pod["job"]]["tasks"].append(rec["id"])
        self._event("handoff", id=rec["id"], job=pod["job"], node=pod["node"], gpus=pod["gpus"], after=pod["rid"])
        log.info("handed %s to the pod of %s (%s, %d GPU)", rec["id"], pod["rid"], pod["node"], pod["gpus"])
        return True

    def _reject(self, rid, reason, message, now):
        rec = self.tracked.get(rid) or {"id": rid, "backend": "pod", "submitted": None}
        self._finish(rec, "rejected", reason, message, now)
        self.tracked[rid] = rec
        unlink_quiet(os.path.join(self.spool, rid + ".submit"))
        log.info("rejected %s: %s", rid, message)

    def _load_queue(self, now):
        queue = []
        for rid in self._markers("submit"):
            rec = self.tracked.get(rid)
            if rec is not None and rec["state"] in ACTIVE | TERMINAL:
                unlink_quiet(os.path.join(self.spool, rid + ".submit"))  # stale marker
                continue
            if rec is None:
                if not is_real_dir(self._dir(rid)):
                    unlink_quiet(os.path.join(self.spool, rid + ".submit"))
                    continue
                try:
                    req = validate(self._read_json(rid, "request.json"), self.cfg)
                    if req["id"] != rid:
                        raise Invalid("id does not match its directory")
                    missing = [a for a in req.get("after") or [] if not is_real_dir(self._dir(a))]
                    if missing:
                        raise Invalid("after: no such job %s" % ", ".join(missing))
                except (OSError, ValueError, Invalid) as e:
                    self._reject(rid, "Invalid", str(e), now)
                    continue
                rec = {k: req.get(k) for k in ("id", "group", "task_index", "task_count", "name", "owner",
                                                "time", "wait_timeout", "submitted")}
                rec.update(backend="pod", state="queued", reason=None, message=None, queue_position=None,
                           gpus=None, gpus_requested=req["gpus"], cpu=None, mem=None, node=None, job=None,
                           attempt=1, placed=None, started=None, finished=None, exit_code=None,
                           after=req.get("after"), ready=None, _req=req)
                self.tracked[rid] = rec
                self._event("submitted", id=rid, name=req["name"] or None, owner=req["owner"] or None,
                            gpus=req["gpus"], time=req["time"], after=req.get("after"), group=req.get("group"),
                            submitted=req["submitted"])
            deps = [a for a in rec["_req"].get("after") or [] if not self._done(a)]
            if deps:  # not eligible yet; does not hold up the FIFO or count toward max_queue
                self._queue(rec, "After", "waiting for %s" % ", ".join(deps[:3])
                            + (" and %d more" % (len(deps) - 3) if len(deps) > 3 else ""))
                continue
            if rec["_req"].get("after") and not rec.get("ready"):
                rec["ready"] = iso(now)  # --wait-timeout counts from here
            if rec["wait_timeout"] and (now - parse_ts(rec.get("ready") or rec["submitted"])).total_seconds() > rec["wait_timeout"]:
                unlink_quiet(os.path.join(self.spool, rid + ".submit"))
                self._finish(rec, "expired", "WaitTimeout",
                             "did not start within --wait-timeout (%ds); last: %s"
                             % (rec["wait_timeout"], rec.get("message")), now)
                continue
            queue.append(rec["_req"])
        for req in queue[self.cfg["max_queue"]:]:
            self._reject(req["id"], "QueueFull", "more than max_queue (%d) jobs waiting" % self.cfg["max_queue"], now)
        return queue[:self.cfg["max_queue"]]

    def _create(self, rec, req, dec, now):
        body = build_job(req, dec["node"], dec["gpus"], dec["cpu"], dec["mem"], rec.get("attempt", 1), self.cfg)
        try:
            created = self.kube.create_job(body)
        except Exception as e:
            log.warning("creating %s failed: %s", body["metadata"]["name"], e)
            self._queue(rec, "ApiError", str(e)[:300])
            return
        if not created:
            log.info("%s already exists; treating as created", body["metadata"]["name"])
        rcpu, rmem = requests_for(dec["cpu"], dec["mem"], self.cfg)
        rec.update(state="pending", reason="Created", message=None, queue_position=None, node=dec["node"],
                   gpus=dec["gpus"], cpu=dec["cpu"], mem=fmt_mem(dec["mem"]), cpu_request=rcpu,
                   mem_request=fmt_mem(rmem), job=body["metadata"]["name"], placed=iso(now),
                   pinned=dec["node"] is not None)
        unlink_quiet(os.path.join(self.spool, rec["id"] + ".submit"))
        name = body["metadata"]["name"]
        self.pods[name] = {"created": iso(now), "node": dec["node"], "gpus": dec["gpus"], "tasks": [rec["id"]]}
        self._ended.discard(name)
        self._event("placed", id=rec["id"], job=name, node=dec["node"], gpus=dec["gpus"], cpu=dec["cpu"],
                    mem=fmt_mem(dec["mem"]), attempt=rec.get("attempt", 1))
        log.info("placed %s on %s (%d GPU)", rec["id"], dec["node"], dec["gpus"])

    def _write_capacity(self, now, cap, nodes_after, queued):
        active = [r for r in self.tracked.values() if r["state"] in ACTIVE]
        idle = list(self.idle.values())
        nodes = [{"name": nd["name"], "gpu_free": nd["gpu_free"],
                  "gpu_used": self.cfg["_node"]["gpu"] - nd["gpu_free"],
                  "cpu_req_free": round(nd["cpu_req_free"], 1),
                  "mem_req_free_gib": round(nd["mem_req_free"] / 2**30, 1),
                  "cpu_actual": nd["cpu_actual"]} for nd in nodes_after]
        doc = {
            "updated": iso(now), "estimated": True, "instance": self.cfg["instance"],
            "placement": "nodes" if self.cfg["nodes"] else "scheduler",
            "nodes": sorted(nodes, key=lambda x: -x["gpu_free"]),
            "total_gpu_free": sum(nd["gpu_free"] for nd in nodes) if nodes else None,
            "max_gpu_free_on_node": max(nd["gpu_free"] for nd in nodes) if nodes else None,
            "gpu_waiting": cap["gpu_waiting"],
            "gq_gpus_in_use": sum(r.get("gpus") or 0 for r in active) + sum(p["gpus"] for p in idle),
            "gq_gpus_cap": self.cfg["max_gpus_total"],
            "gq_running": len(active) + len(idle), "gq_max_running": self.cfg["max_running"],
            "idle_pods": [{"job": p["job"], "node": p["node"], "gpus": p["gpus"],
                           "idle_seconds": int((now - p["since"]).total_seconds()), "after": p["rid"]} for p in idle],
            "queue_length": queued,
            "node_capacity": self.cfg["node_capacity"],
        }
        try:
            write_json(os.path.join(self.root, "capacity.json"), doc)
        except OSError as e:
            log.warning("cannot write capacity: %s", e)

    def _gc(self, now):
        """Hourly: job output after log_retention_days, job dirs after retention_days, event files
        after event_retention_days. A value <= 0 keeps that kind forever."""
        self._gc_events(now)
        keep, log_keep = self.cfg["retention_days"] * 86400, self.cfg["log_retention_days"] * 86400
        if keep <= 0 and log_keep <= 0:
            return
        removed = trimmed = 0
        for rid in os.listdir(self.jobs_dir):
            d = self._dir(rid)
            if not ID_RE.match(rid) or rid in self.tracked or not is_real_dir(d):
                continue
            ref = None
            try:
                st = self._read_json(rid, "status.json")
                if st.get("state") in TERMINAL:
                    ref = parse_ts(st.get("finished"))
            except (OSError, ValueError, Invalid):
                pass
            if ref is None:  # local jobs: the supervisor writes exit when done
                try:
                    ref = dt.datetime.fromtimestamp(os.lstat(os.path.join(d, "exit")).st_mtime, dt.timezone.utc)
                except OSError:
                    continue
            age = (now - ref).total_seconds()
            if keep > 0 and age > keep:
                shutil.rmtree(d, ignore_errors=True)
                removed += 1
            elif log_keep > 0 and age > log_keep and os.path.lexists(os.path.join(d, "log")):
                for f in ("log", "run.sh"):
                    unlink_quiet(os.path.join(d, f))
                trimmed += 1
        if removed or trimmed:
            log.info("gc: removed %d job directories, trimmed output of %d jobs", removed, trimmed)

    def _gc_events(self, now):
        keep = self.cfg["event_retention_days"] * 86400
        if keep <= 0:
            return
        for name in os.listdir(self.events_dir):
            m = re.match(r"^(\d{4})-(\d{2})\.jsonl$", name)
            if not m:
                continue
            y, mo = int(m.group(1)), int(m.group(2))
            month_end = dt.datetime(y + mo // 12, mo % 12 + 1, 1, tzinfo=dt.timezone.utc)
            if (now - month_end).total_seconds() > keep:
                unlink_quiet(os.path.join(self.events_dir, name))
                log.info("gc: removed event log %s", name)


def main(argv=None):
    ap = argparse.ArgumentParser(description="gq daemon (holds the Kubernetes token; see DESIGN.md)")
    ap.add_argument("--config", required=True)
    ap.add_argument("--root", required=True, help="gq root as seen by this pod (PVC subPath gq)")
    ap.add_argument("--dry-run", action="store_true",
                    help="create Jobs with server-side dry-run and keep them in memory only")
    ap.add_argument("--once", action="store_true", help="run a single loop and exit")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(args.config)
    daemon = Daemon(cfg, args.root, Kube(cfg["namespace"], cfg["instance"], args.dry_run))
    log.info("gqd %s started (instance=%s, root=%s, dry_run=%s)", VERSION, cfg["instance"], args.root, args.dry_run)
    while True:
        t0 = time.monotonic()
        try:
            daemon.loop_once()
        except Exception:
            log.exception("loop failed")
        if args.once:
            return
        time.sleep(max(0.0, cfg["loop_interval"] - (time.monotonic() - t0)))


if __name__ == "__main__":
    main()
