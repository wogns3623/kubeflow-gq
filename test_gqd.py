#!/usr/bin/env python3
"""Tests for gqd's pure logic, gqctl discovery/rendering, gq + gqd end to end (fake
Kubernetes, two instances), and gq --local.

    python3 test_gqd.py
"""
import contextlib
import copy
import datetime as dt
import importlib.machinery
import importlib.util
import glob
import io
import json
import os
import shutil
import sys
import subprocess
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import gqd  # noqa: E402

def load_script(name, filename):
    loader = importlib.machinery.SourceFileLoader(name, os.path.join(HERE, filename))
    mod = importlib.util.module_from_spec(importlib.util.spec_from_loader(name, loader))
    loader.exec_module(mod)
    return mod


gq = load_script("gq_cli", "gq")
gqctl = load_script("gqctl_cli", "gqctl")

NODES = ["n-a", "n-b", "n-c", "n-d"]


def cfg(**kw):
    base = dict(instance="t", namespace="ns", image="img", volumes=[{"pvc": "ws", "mountPath": "/home/irteam"}],
                root="/home/irteam/gq", nodes=NODES, pod_labels={"zone": "z"})
    base.update(kw)
    return gqd.load_config(None, **base)


def req(rid="261108-061530-a1b2", gpus=(1, 1), **kw):
    r = {"id": rid, "name": "t", "owner": "o", "backend": "pod", "gpus": list(gpus), "cpu": None, "mem": None,
         "time": 3600, "wait_timeout": 3600, "cwd": "/home/irteam/p", "submitted": "2026-11-08T06:00:00Z"}
    r.update(kw)
    return r


def pod(name, node, gpus=0, phase="Running", cpu="500m", mem="1Gi", labels=None, sidecar=True):
    p = {"metadata": {"name": name, "labels": labels or {}, "creationTimestamp": "2026-11-08T06:00:00Z"},
         "spec": {"nodeName": node, "containers": [{"name": "main", "resources": {
             "requests": {"cpu": cpu, "memory": mem}, "limits": {"nvidia.com/gpu": str(gpus)} if gpus else {}}}]},
         "status": {"phase": phase}}
    if sidecar:
        p["spec"]["initContainers"] = [{"name": "istio-proxy", "restartPolicy": "Always",
                                        "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}}}]
    return p


def free(gpus_free):
    """capacity nodes with the given free GPUs and plenty of CPU/memory."""
    return [{"name": n, "gpu_free": g, "gpu_used": 8 - g, "cpu_req_free": 120.0, "mem_req_free": 1900 * 2**30,
             "cpu_actual": None} for n, g in zip(NODES, gpus_free)]


# ---------------------------------------------------------------- pure logic

def test_quantities():
    assert gqd.parse_mem("1964Gi") == 1964 * 2**30
    assert gqd.parse_mem("512Mi") == 512 * 2**20
    assert gqd.parse_mem("1G") == 10**9
    assert gqd.parse_mem("129e6") == 129 * 10**6
    assert gqd.parse_cpu("500m") == 0.5 and gqd.parse_cpu("2") == 2.0 and gqd.parse_cpu("5000000n") == 0.005
    assert gqd.fmt_mem(245 * 2**30) == "245Gi" and gqd.fmt_cpu(15.0) == "15" and gqd.fmt_cpu(0.5) == "500m"


def test_validate():
    c = cfg()
    ok = gqd.validate(req(gpus=(1, 4), mem="64Gi", cwd="/home/irteam/p/../q"), c)
    assert ok["gpus"] == [1, 4] and ok["mem"] == 64 * 2**30 and ok["cwd"] == "/home/irteam/q"
    assert gqd.validate(dict(req(), gpus=2), c)["gpus"] == [2, 2]
    assert gqd.validate(req(wait_timeout=None), c)["wait_timeout"] is None   # no deadline: waits until it starts
    bad = [
        dict(req(), extra=1),                                   # unknown field
        req(rid="../../etc"),                                   # path injection via id
        req(cwd="/etc"), req(cwd="/home/irteam/../etc"),        # cwd outside the mounted volumes
        req(cwd="relative/path"), req(cwd="/home/irteamX"),
        req(gpus=(2, 1)), req(gpus=(0, 2)), req(gpus=(1, 9)),   # bad GPU ranges
        req(time=10**9), req(wait_timeout=0), req(wait_timeout="6h"), req(time=True),   # limits / bool is not a number
        req(cpu=500), req(mem="9Ti"), req(mem="lots"),          # over node size / garbage
        req(backend="local"), req(name="has space"), req(owner="x" * 64),
        req(rid="261108-061530-a1b2-1", group="261108-061530-ffff", task_index=1, task_count=2),
        req(rid="261108-061530-a1b2-1", group="261108-061530-a1b2", task_index=2, task_count=2),
        req(task_index=0),
        req(submitted="yesterday"),
        "not a dict",
    ]
    for b in bad:
        try:
            gqd.validate(b, c)
        except gqd.Invalid:
            continue
        raise AssertionError("accepted %r" % (b,))


def test_config_validation():
    for bad in (dict(instance="Has_Caps"), dict(instance="x" * 17), dict(root="/elsewhere/gq"),
                dict(volumes=[{"mountPath": "/home/irteam"}]), dict(image="")):
        try:
            cfg(**bad)
        except ValueError:
            continue
        raise AssertionError("accepted config %r" % (bad,))
    two = cfg(volumes=[{"pvc": "ws", "mountPath": "/home/irteam"}, {"pvc": "data", "mountPath": "/home/irteam/data-vol1"}])
    assert gqd.mount_of("/home/irteam/data-vol1/x", two)["pvc"] == "data"   # longest mount wins
    assert gqd.validate(req(cwd="/home/irteam/data-vol1/exp"), two)["cwd"] == "/home/irteam/data-vol1/exp"
    # A node-local volume nested in the shared home: in a Job that path would be an empty
    # directory of the home volume, so it is refused with the reason.
    local = cfg(excluded_volumes=[{"pvc": "scratch", "mountPath": "/home/irteam/scratch",
                                   "accessModes": ["ReadWriteOnce"], "storageClass": "local-path"}])
    try:
        gqd.validate(req(cwd="/home/irteam/scratch/exp"), local)
        raise AssertionError("accepted a cwd on an excluded volume")
    except gqd.Invalid as e:
        assert "scratch" in str(e) and "ReadWriteOnce" in str(e)
    assert gqd.validate(req(cwd="/home/irteam/p"), local)["cwd"] == "/home/irteam/p"
    # The other way round: shared data volume inside a non-shareable home is fine.
    inner = cfg(volumes=[{"pvc": "data", "mountPath": "/home/irteam/data-vol1"}], root="/home/irteam/data-vol1/gq",
                excluded_volumes=[{"pvc": "home", "mountPath": "/home/irteam"}])
    assert gqd.validate(req(cwd="/home/irteam/data-vol1/x"), inner)
    for bad_cwd in ("/home/irteam/x", "/home/irteam"):
        try:
            gqd.validate(req(cwd=bad_cwd), inner)
            raise AssertionError("accepted %s" % bad_cwd)
        except gqd.Invalid:
            pass


def test_resources_defaults_fit_a_node():
    c = cfg()
    r = gqd.validate(req(gpus=(8, 8)), c)
    cpu, mem = gqd.resources(r, 8, c)
    assert (cpu, mem) == (120.0, 1960 * 2**30)          # 15 CPU and 245Gi per GPU, fits 125 / 1964Gi
    assert gqd.resources(gqd.validate(req(gpus=(0, 0)), c), 0, c) == (8.0, 64 * 2**30)


def test_capacity():
    c = cfg()
    pods = [
        pod("nb1", "n-a", gpus=4),
        pod("nb2", "n-a", gpus=2, cpu="16", mem="64Gi"),
        pod("done", "n-b", gpus=8, phase="Succeeded"),        # finished: ignored
        pod("other", "elsewhere", gpus=8),                     # node outside the zone: ignored
        pod("waiting", None, gpus=2, phase="Pending"),         # someone else waiting for GPUs
        pod("gqp", "n-c", gpus=1, labels={"gq": "1", "batch.kubernetes.io/job-name": "gq-x-a1"}),
    ]
    jobs = [  # one gq job bound (counted via its pod), one not yet bound (reserved via annotations)
        {"metadata": {"name": "gq-x-a1", "annotations": {"gq/node": "n-c", "gq/gpus": "1", "gq/cpu-request": "1",
                                                        "gq/mem-request": str(4 * 2**30)}}, "status": {}},
        {"metadata": {"name": "gq-y-a1", "annotations": {"gq/node": "n-d", "gq/gpus": "3", "gq/cpu-request": "1",
                                                        "gq/mem-request": str(4 * 2**30)}}, "status": {}},
    ]
    cap = gqd.capacity(pods, jobs, c, metrics={"nb1": 3.5})
    by = {n["name"]: n for n in cap["nodes"]}
    assert [by[n]["gpu_free"] for n in NODES] == [2, 8, 7, 5]
    assert by["n-d"]["cpu_req_free"] == 124.0       # unbound gq job reserves its request, not its limit
    assert abs(by["n-a"]["cpu_req_free"] - (125 - 0.6 - 16.1)) < 1e-6   # sidecar requests count too
    assert by["n-a"]["cpu_actual"] == 3.5 and cap["gpu_waiting"] == 2


def test_place_best_fit_and_fragmentation():
    c = cfg()
    q = lambda *specs: [gqd.validate(req(rid="261108-061530-%04x" % i, gpus=g), c) for i, g in enumerate(specs)]
    # Today's situation: 2 + 1 + 1 + 0 free. A 4-GPU job cannot run anywhere.
    dec, _ = gqd.place(q((4, 4)), free([2, 1, 1, 0]), c, 0, 0)
    assert dec[0][1]["reason"] == "NoFit" and "max free on a node is 2 (total free 4)" in dec[0][1]["message"]
    # Two 1-GPU jobs take the 1-free nodes and keep the 2-GPU slot intact; then a 2-GPU job fits it.
    dec, after = gqd.place(q((1, 1), (1, 1), (2, 2)), free([2, 1, 1, 0]), c, 0, 0)
    assert [d["node"] for _, d in dec] == ["n-b", "n-c", "n-a"]
    assert [n["gpu_free"] for n in after] == [0, 0, 0, 0]
    # A range takes as many as one node can give.
    dec, _ = gqd.place(q((1, 4)), free([2, 1, 1, 0]), c, 0, 0)
    assert dec[0][1]["gpus"] == 2 and dec[0][1]["node"] == "n-a"
    # Backfill: the big job waits, the small one behind it starts.
    dec, _ = gqd.place(q((4, 4), (1, 1)), free([2, 1, 1, 0]), c, 0, 0)
    assert dec[0][1]["reason"] == "NoFit" and dec[1][1]["node"] == "n-b"
    # CPU jobs go to the node with the fewest free GPUs.
    dec, _ = gqd.place(q((0, 0)), free([2, 1, 1, 0]), c, 0, 0)
    assert dec[0][1]["node"] == "n-d"
    # Fit is checked on requests (1 CPU / 4Gi), not limits: a node whose CPU requests are nearly
    # used up still takes a 2-GPU job (limit 30 CPU), but not one with less than 1 CPU of room.
    nodes = free([2, 2, 0, 0])
    nodes[0]["cpu_req_free"], nodes[1]["cpu_req_free"] = 0.5, 22.4
    dec, after = gqd.place(q((2, 2)), nodes, c, 0, 0)
    assert dec[0][1]["node"] == "n-b" and dec[0][1]["cpu"] == 30.0 and after[1]["cpu_req_free"] == 21.4


def test_place_caps():
    c = cfg(max_gpus_total=4, max_running=8)
    q = [gqd.validate(req(rid="261108-061530-a1b2-%d" % k, gpus=(1, 1), group="261108-061530-a1b2",
                          task_index=k, task_count=6), c) for k in range(6)]
    dec, _ = gqd.place(q, free([8, 8, 8, 8]), c, 0, 0)
    assert sum("node" in d for _, d in dec) == 4
    assert [d.get("reason") for _, d in dec[4:]] == ["Cap", "Cap"]
    dec, _ = gqd.place(q[:1], free([8, 8, 8, 8]), c, 8, 0)              # max_running reached
    assert dec[0][1]["reason"] == "Cap"
    dec, _ = gqd.place([gqd.validate(req(gpus=(1, 4)), c)], free([8, 8, 8, 8]), c, 0, 3)
    assert dec[0][1]["gpus"] == 1                                        # range trimmed to the cap


def test_build_job_keeps_secrets_off_the_spec():
    c = cfg()
    r = gqd.validate(req(rid="261108-061530-a1b2-3", group="261108-061530-a1b2", task_index=3, task_count=4), c)
    j = gqd.build_job(r, "n-b", 2, 30.0, 490 * 2**30, 1, c)
    spec = j["spec"]["template"]["spec"]
    assert j["metadata"]["name"] == "gq-t-261108-061530-a1b2-3-a1" and gqd.job_name_re("t").match(j["metadata"]["name"])
    assert not gqd.job_name_re("other").match(j["metadata"]["name"])
    assert j["metadata"]["labels"]["gq-instance"] == "t"
    assert spec["automountServiceAccountToken"] is False
    term = spec["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"][0]
    assert term["matchFields"][0]["values"] == ["n-b"]
    assert j["spec"]["template"]["metadata"]["labels"]["zone"] == "z"
    main = spec["containers"][0]
    assert main["resources"]["limits"] == {"cpu": "30", "memory": "490Gi", "nvidia.com/gpu": "2"}
    assert main["resources"]["requests"] == {"cpu": "1", "memory": "4Gi"}       # small, like the notebooks
    assert j["metadata"]["annotations"]["gq/cpu-request"] == "1.0"
    tiny = gqd.build_job(r, "n-b", 0, 0.5, 2**30, 1, c)                          # request never exceeds limit
    assert tiny["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"] == {"cpu": "500m", "memory": "1Gi"}
    env = {e["name"]: e["value"] for e in main["env"]}
    assert env == {"GQ_ID": r["id"], "GQ_GPUS": "2", "GQ_CPU": "30", "GQ_TASK_INDEX": "3", "GQ_TASK_COUNT": "4"}
    assert [v["name"] for v in spec["volumes"]] == ["v0", "dshm"] and "root=/home/irteam/gq;" in main["command"][2]
    assert j["metadata"]["annotations"]["gq/reuse-grace"] == "60"
    multi = cfg(volumes=[{"pvc": "ws", "mountPath": "/home/irteam"},
                         {"pvc": "data", "mountPath": "/home/irteam/data-vol1", "subPath": "u1", "readOnly": True}])
    mj = gqd.build_job(gqd.validate(req(), multi), "n-a", 1, 15.0, 245 * 2**30, 1, multi)
    mm = mj["spec"]["template"]["spec"]["containers"][0]["volumeMounts"]
    assert mm[1] == {"name": "v1", "mountPath": "/home/irteam/data-vol1", "subPath": "u1", "readOnly": True}
    assert j["spec"]["activeDeadlineSeconds"] == 3600 + 60 + 1800
    cpu_job = gqd.build_job(gqd.validate(req(gpus=(0, 0)), c), "n-d", 0, 8.0, 64 * 2**30, 1, c)
    assert "nvidia.com/gpu" not in cpu_job["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]


def kpod(job, node="n-a", **status):
    p = pod("p-" + job, node, labels={"gq": "1", "batch.kubernetes.io/job-name": job})
    p["status"] = dict({"phase": "Running"}, **status)
    return p


def test_derive_state():
    job = {"metadata": {"name": "gq-x-a1"}, "status": {}}
    running = kpod("gq-x-a1", containerStatuses=[{"name": "main", "state": {"running": {"startedAt": "2026-11-08T06:00:05Z"}}}])
    assert gqd.derive_state(job, running, None)["state"] == "running"
    ok = kpod("gq-x-a1", phase="Succeeded", containerStatuses=[{"name": "main", "state": {"terminated": {"exitCode": 0}}}])
    assert gqd.derive_state(job, ok, 0)["state"] == "succeeded"
    oom = kpod("gq-x-a1", phase="Failed", containerStatuses=[{"name": "main", "state": {"terminated": {"exitCode": 137, "reason": "OOMKilled"}}}])
    s = gqd.derive_state(job, oom, None)
    assert (s["state"], s["reason"], s["exit_code"]) == ("failed", "OOMKilled", 137)
    unsched = kpod("gq-x-a1", node=None, phase="Pending", conditions=[{"type": "PodScheduled", "status": "False",
                                                                       "reason": "Unschedulable", "message": "0/4 nodes"}])
    assert gqd.derive_state(job, unsched, None)["reason"] == "Unschedulable"
    deadline = {"metadata": {"name": "gq-x-a1"}, "status": {"conditions": [{"type": "Failed", "status": "True", "reason": "DeadlineExceeded"}]}}
    assert gqd.derive_state(deadline, oom, None)["state"] == "timeout"
    evicted = kpod("gq-x-a1", phase="Failed", reason="Evicted", message="node pressure")
    assert gqd.derive_state(job, evicted, None)["state"] == "lost"
    assert gqd.derive_state(None, None, 3)["state"] == "failed"         # Job gone (ttl), exit file remains
    assert gqd.derive_state(None, None, None)["state"] == "lost"
    assert gqd.derive_state(job, None, None)["reason"] == "Creating"


# ---------------------------------------------------------------- end to end (gq + gqd, fake Kubernetes)

class FakeKube:
    """One per gqd instance; instances can share pods/jobs (same namespace) via share()."""
    dry_run = False

    def __init__(self, instance="t"):
        self.pods, self.jobs, self.deleted, self.created_nodes = [], {}, [], {}
        self.own = gqd.job_name_re(instance)

    def share(self, other, instance):
        twin = FakeKube(instance)
        twin.__dict__.update({k: v for k, v in other.__dict__.items() if k != "own"})
        return twin

    def list_pods(self):
        return copy.deepcopy(self.pods)

    def list_jobs(self):
        return copy.deepcopy(list(self.jobs.values()))

    def pod_metrics(self):
        return {}

    def create_job(self, body):
        name = body["metadata"]["name"]
        if name in self.jobs:
            return False
        self.jobs[name] = dict(copy.deepcopy(body), status={})
        self.created_nodes[name] = body["metadata"]["annotations"]["gq/node"]
        return True

    def patch_job_deadline(self, name, seconds):
        assert self.own.match(name), "patched another instance's Job: " + name
        self.jobs[name]["spec"]["activeDeadlineSeconds"] = seconds

    def delete_job(self, name):
        assert self.own.match(name), "deleted another instance's Job: " + name
        self.jobs.pop(name, None)
        self.pods[:] = [p for p in self.pods if gqd.pod_job_name(p) != name]
        self.deleted.append(name)

    def run(self, name, node, started=None):
        started = started or dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # "now": --time is live
        self.pods[:] = [p for p in self.pods if gqd.pod_job_name(p) != name]
        p = kpod(name, node, containerStatuses=[{"name": "main", "state": {"running": {"startedAt": started}}}])
        p["spec"]["containers"][0]["resources"] = self.jobs[name]["spec"]["template"]["spec"]["containers"][0]["resources"]
        self.pods.append(p)

    def finish(self, name, code):
        for p in self.pods:
            if gqd.pod_job_name(p) == name:
                p["status"] = {"phase": "Succeeded" if code == 0 else "Failed", "containerStatuses": [
                    {"name": "main", "state": {"terminated": {"exitCode": code, "startedAt": "2026-11-08T06:00:05Z",
                                                              "finishedAt": "2026-11-08T06:10:00Z"}}}]}
        self.jobs[name]["status"] = {"conditions": [{"type": "Complete" if code == 0 else "Failed", "status": "True"}]}


def kube_node_of(job, kube):
    return kube.created_nodes.get(job)


@contextlib.contextmanager
def sandbox():
    tmp = tempfile.mkdtemp(prefix="gqtest-")
    home = os.path.join(tmp, "home")
    os.makedirs(os.path.join(home, "proj"))
    old = {k: os.environ.get(k) for k in ("GQ_ROOT", "GQ_MOUNTS", "GQ_POLL", "GQ_OWNER")}
    os.environ.update(GQ_ROOT=os.path.join(home, "gq"), GQ_MOUNTS=home, GQ_POLL="0.1", GQ_OWNER="tester")
    cwd = os.getcwd()
    os.chdir(os.path.join(home, "proj"))
    try:
        yield home
    finally:
        os.chdir(cwd)
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        shutil.rmtree(tmp, ignore_errors=True)


def run_gq(*argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = gq.main(list(argv))
    return code, out.getvalue(), err.getvalue()


def status(rid):
    with open(os.path.join(os.environ["GQ_ROOT"], "jobs", rid, "status.json")) as f:
        return json.load(f)


class Clock:
    def __init__(self):
        self.t = gqd.now_utc()

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += dt.timedelta(seconds=seconds)


def test_end_to_end():
    with sandbox() as home:
        c = cfg(volumes=[{"pvc": "ws", "mountPath": home}], root=home + "/gq")
        kube, clock = FakeKube(), Clock()
        # Today's cluster: n-a 2 free, n-b 1, n-c 1, n-d 0.
        kube.pods = [pod("nb-a", "n-a", gpus=6), pod("nb-b", "n-b", gpus=7), pod("nb-c", "n-c", gpus=7),
                     pod("nb-d", "n-d", gpus=8)]
        d = gqd.Daemon(c, os.environ["GQ_ROOT"], kube, clock=clock)

        code, out, _ = run_gq("submit", "--gpus", "1", "--time", "1h", "--name", "one", "--env", "SECRET=s3cr3t",
                              "--", "python", "-c", "print('hi there')")
        assert code == 0
        rid = json.loads(out)["id"]
        assert status(rid)["state"] == "submitted"
        d.loop_once()
        st = status(rid)
        assert (st["state"], st["node"], st["gpus"]) == ("pending", "n-b", 1)    # best fit, not the 2-GPU node
        job = kube.jobs[st["job"]]
        assert "s3cr3t" not in json.dumps(job) and "hi there" not in json.dumps(job)
        run_sh = open(os.path.join(home, "gq", "jobs", rid, "run.sh")).read()
        assert "export SECRET=s3cr3t" in run_sh and "GQ_ROOT" not in run_sh
        assert "cd %s/proj " % home in run_sh                    # logical home path, even though getcwd is real

        kube.run(st["job"], "n-b")
        d.loop_once()
        assert status(rid)["state"] == "running"
        kube.finish(st["job"], 0)
        with open(os.path.join(home, "gq", "jobs", rid, "exit"), "w") as f:
            f.write("0\n")
        d.loop_once()
        assert status(rid)["state"] == "succeeded" and rid not in d.tracked
        assert run_gq("wait", rid)[0] == 0

        # A 4-GPU job does not fit anywhere: it waits as NoFit, without a pending pod; then it is killed.
        rid4 = json.loads(run_gq("submit", "--gpus", "4", "--time", "1h", "--", "true")[1])["id"]
        d.loop_once()
        st = status(rid4)
        assert (st["state"], st["reason"]) == ("queued", "NoFit") and st["job"] is None
        cap = json.loads(run_gq("capacity", "--json")[1])
        assert cap["max_gpu_free_on_node"] == 2 and cap["total_gpu_free"] == 4
        assert run_gq("capacity", "--fit", "4")[0] == 1 and run_gq("capacity", "--fit", "2")[0] == 0
        run_gq("kill", rid4)
        d.loop_once()
        assert status(rid4)["state"] == "killed"
        code, _, err = run_gq("wait", rid4)
        assert code == 125 and '"killed"' in err

        # Array split around fragmentation: 4 x 1 GPU fills 2 + 1 + 1.
        out = json.loads(run_gq("submit", "--array", "4", "--gpus", "1", "--time", "1h", "--", "true")[1])
        d.loop_once()
        nodes = sorted(status(t)["node"] for t in out["tasks"])
        assert nodes == ["n-a", "n-a", "n-b", "n-c"], nodes
        ps = json.loads(run_gq("ps", "--json", "--mine")[1])
        assert {s["id"] for s in ps} >= set(out["tasks"])
        run_gq("kill", out["id"])
        d.loop_once()
        assert all(status(t)["state"] == "killed" for t in out["tasks"])
        assert all(n.endswith("-a1") for n in kube.deleted[-4:])

        # Time limit: a running job past --time is deleted as timeout.
        rid_t = json.loads(run_gq("submit", "--gpus", "1", "--time", "60", "--", "sleep", "999")[1])["id"]
        d.loop_once()
        name = status(rid_t)["job"]
        kube.run(name, status(rid_t)["node"], started=gqd.iso(clock()))
        d.loop_once()
        clock.advance(61)
        d.loop_once()
        assert status(rid_t)["state"] == "timeout" and name in kube.deleted

        # Lost scheduling race: Unschedulable past placement_grace -> re-placed under a new Job name.
        rid_r = json.loads(run_gq("submit", "--gpus", "1", "--time", "1h", "--", "true")[1])["id"]
        d.loop_once()
        first = status(rid_r)["job"]
        kube.pods.append(kpod(first, node=None, phase="Pending", conditions=[
            {"type": "PodScheduled", "status": "False", "reason": "Unschedulable", "message": "taken"}]))
        d.loop_once()
        clock.advance(61)
        d.loop_once()  # deletes the stuck Job and re-places in the same loop under a new name
        st = status(rid_r)
        assert first in kube.deleted
        assert st["state"] == "pending" and st["job"].endswith("-a2") and st["attempt"] == 2
        assert st["node"] != kube_node_of(first, kube)  # the old reservation still counts this loop

        # gqd restart keeps tracking active jobs.
        d2 = gqd.Daemon(c, os.environ["GQ_ROOT"], kube, clock=clock)
        assert rid_r in d2.tracked

        # Tampered request is rejected, not run.
        rid_x = json.loads(run_gq("submit", "--gpus", "1", "--time", "1h", "--", "true")[1])["id"]
        p = os.path.join(home, "gq", "jobs", rid_x, "request.json")
        r = json.load(open(p))
        r["image"] = "evil:latest"
        json.dump(r, open(p, "w"))
        d2.loop_once()
        assert status(rid_x)["state"] == "rejected" and "unknown fields: image" in status(rid_x)["message"]

        # Never waits forever: queued past --wait-timeout -> expired.
        rid_w = json.loads(run_gq("submit", "--gpus", "8", "--time", "1h", "--wait-timeout", "120", "--", "true")[1])["id"]
        d2.loop_once()
        clock.advance(400)  # submitted uses the real clock; the fake clock is far ahead after this
        d2.loop_once()
        assert status(rid_w)["state"] == "expired"
        # Without --wait-timeout it waits as long as it takes.
        rid_n = json.loads(run_gq("submit", "--gpus", "8", "--time", "1h", "--", "true")[1])["id"]
        d2.loop_once()
        clock.advance(30 * 86400)
        d2.loop_once()
        assert status(rid_n)["state"] == "queued" and json.load(open(os.path.join(
            os.environ["GQ_ROOT"], "jobs", rid_n, "request.json")))["wait_timeout"] is None
        run_gq("kill", rid_n)
        d2.loop_once()

        # A second user's gqd in the same namespace: its unbound Job reserves capacity for us,
        # and neither instance ever deletes the other's Jobs (FakeKube asserts that).
        other_root = os.path.join(home, "other-gq")
        os.makedirs(os.path.join(other_root, "jobs"))
        ou = gqd.Daemon(cfg(instance="u", volumes=[{"pvc": "ws2", "mountPath": home}], root=other_root),
                        other_root, kube.share(kube, "u"), clock=clock)
        before = json.loads(run_gq("capacity", "--json")[1])["total_gpu_free"]
        orid = "261108-070000-beef"
        os.makedirs(os.path.join(other_root, "jobs", orid))
        json.dump(req(rid=orid, cwd=home, submitted=gqd.iso(clock())),
                  open(os.path.join(other_root, "jobs", orid, "request.json"), "w"))
        open(os.path.join(other_root, "spool", orid + ".submit"), "w").close()
        ou.loop_once()
        assert any(n.startswith("gq-u-") for n in kube.jobs)
        d2.loop_once()
        assert json.loads(run_gq("capacity", "--json")[1])["total_gpu_free"] == before - 1

        # Usage errors.
        assert run_gq("submit", "--gpus", "1", "--time", "1h", "--cwd", "/tmp", "--", "true")[0] == 2
        assert run_gq("submit", "--time", "1h", "--local", "--gpus", "1", "--", "true")[0] == 2
        assert run_gq("submit", "--time", "soon", "--", "true")[0] == 2


NOTEBOOK = {
    "metadata": {"name": "kim-gpu4", "namespace": "p-x",
                 "labels": {"app": "kim-gpu4", "h200-pod-default": "true", "private-z1-pod-default": "true"}},
    "spec": {"template": {"spec": {
        "serviceAccountName": "default-editor",
        "containers": [{"name": "kim-gpu4", "image": "reg/img:1", "volumeMounts": [
            {"name": "dshm", "mountPath": "/dev/shm"},
            {"name": "data", "mountPath": "/home/irteam/data-vol1"},
            {"name": "ws", "mountPath": "/home/irteam"}]}],
        "volumes": [{"name": "dshm", "emptyDir": {"medium": "Memory"}},
                    {"name": "data", "persistentVolumeClaim": {"claimName": "kim-data"}},
                    {"name": "ws", "persistentVolumeClaim": {"claimName": "kim-ws"}}]}}},
}
PODDEFAULTS = [
    {"spec": {"selector": {"matchLabels": {"h200-pod-default": "true"}}, "annotations": {"example.com/gpu": "H200"}}},
    {"spec": {"selector": {"matchLabels": {"private-z1-pod-default": "true"}},
              "annotations": {"mlx.navercorp.com/zone": "z1"}}},
    {"spec": {"selector": {"matchLabels": {"someone-else": "true"}}, "annotations": {"mlx.navercorp.com/zone": "z9"}}},
]


RWX = {"accessModes": ["ReadWriteMany"], "storageClassName": "ddn"}
RWO_LOCAL = {"accessModes": ["ReadWriteOnce"], "storageClassName": "local-path"}


def test_gqctl_keeps_only_shareable_volumes():
    # Shared home + node-local data volume: the data volume is left out and reported.
    c, w = gqctl.discover(NOTEBOOK, PODDEFAULTS, nodes=["n1"], pvc_info={"kim-ws": RWX, "kim-data": RWO_LOCAL})
    assert c["volumes"] == [{"pvc": "kim-ws", "mountPath": "/home/irteam"}]
    assert c["excluded_volumes"][0]["pvc"] == "kim-data" and c["excluded_volumes"][0]["storageClass"] == "local-path"
    assert any("kim-data" in x and "not visible to jobs" in x for x in w)
    loaded = gqd.load_config(None, **c)
    assert gqd.mount_of("/home/irteam/data-vol1/x", loaded) is None              # shadowed in the notebook
    assert all(m["mountPath"] != "/home/irteam/data-vol1"
               for m in gqctl.render_probe(c)["spec"]["template"]["spec"]["containers"][0]["volumeMounts"])
    # Read-only mounts may use ReadOnlyMany.
    nb = copy.deepcopy(NOTEBOOK)
    nb["spec"]["template"]["spec"]["containers"][0]["volumeMounts"][1]["readOnly"] = True
    c2, _ = gqctl.discover(nb, PODDEFAULTS, nodes=["n1"], pvc_info={
        "kim-ws": RWX, "kim-data": {"accessModes": ["ReadOnlyMany"], "storageClassName": "ddn"}})
    assert [v["pvc"] for v in c2["volumes"]] == ["kim-data", "kim-ws"]
    # Nothing shareable and writable: refuse instead of producing a config that cannot work.
    try:
        gqctl.discover(NOTEBOOK, PODDEFAULTS, nodes=["n1"], pvc_info={"kim-ws": RWO_LOCAL, "kim-data": RWO_LOCAL})
        raise AssertionError("accepted a notebook without an RWX volume")
    except gqctl.Fail as e:
        assert "ReadWriteMany" in str(e)
    # PVCs unreadable: keep everything but say so.
    c3, w3 = gqctl.discover(NOTEBOOK, PODDEFAULTS, nodes=["n1"], pvc_info=None)
    assert len(c3["volumes"]) == 2 and any("could not read PVC access modes" in x for x in w3)


def test_gq_refuses_excluded_cwd():
    with sandbox() as home:
        os.environ["GQ_EXCLUDED"] = os.path.join(home, "proj")      # pretend proj is a node-local volume
        try:
            code, _, err = run_gq("submit", "--gpus", "1", "--time", "1h", "--", "true")
            assert code == 2 and "cannot mount" in err and "--local" in err, err
            code, out, _ = run_gq("submit", "--local", "--time", "30s", "--", "true")   # local runs here: fine
            assert code == 0 and run_gq("wait", json.loads(out)["id"], "--timeout", "15s")[0] == 0
        finally:
            os.environ.pop("GQ_EXCLUDED", None)


def test_gqctl_discover_and_render():
    c, warnings = gqctl.discover(NOTEBOOK, PODDEFAULTS, nodes=["n1", "n2"], pvc_info={"kim-ws": RWX, "kim-data": RWX})
    assert (c["instance"], c["namespace"], c["image"], c["zone"], c["root"]) == ("kim", "p-x", "reg/img:1", "z1", "/home/irteam/gq")
    assert c["volumes"] == [{"pvc": "kim-data", "mountPath": "/home/irteam/data-vol1"},
                            {"pvc": "kim-ws", "mountPath": "/home/irteam"}]
    assert c["pod_labels"] == {"h200-pod-default": "true", "private-z1-pod-default": "true"}
    assert c["platform"] == "mlxp" and c["node_capacity"] == gqctl.MLXP_NODE_CAPACITY
    gqd.load_config(None, **c)                                   # discovered config is valid as is
    dep = gqctl.render_deployment(c)
    pod = dep["spec"]["template"]["spec"]
    assert dep["metadata"]["name"] == "gqd-kim" and pod["serviceAccountName"] == "default-editor"
    assert pod["containers"][0]["volumeMounts"][0] == {"name": "root", "mountPath": "/gq", "subPath": "gq"}
    assert pod["volumes"][0]["persistentVolumeClaim"]["claimName"] == "kim-ws"   # root's volume, not the data one
    assert pod["volumes"][1]["configMap"]["name"] == "gqd-kim"
    assert dep["spec"]["template"]["metadata"]["labels"]["private-z1-pod-default"] == "true"
    probe = gqctl.render_probe(c, gpu=True)["spec"]["template"]["spec"]
    assert probe["automountServiceAccountToken"] is False and "nvidia-smi" in probe["containers"][0]["command"][2]
    assert [m["mountPath"] for m in probe["containers"][0]["volumeMounts"]] == ["/home/irteam/data-vol1", "/home/irteam"]
    # No PVC at home: root goes on the shortest writable mount and the user is told to set GQ_ROOT.
    nb = copy.deepcopy(NOTEBOOK)
    nb["spec"]["template"]["spec"]["containers"][0]["volumeMounts"][2]["mountPath"] = "/home/irteam/kim-ws"
    c2, w2 = gqctl.discover(nb, PODDEFAULTS, nodes=["n1"], instance="kim2", pvc_info={"kim-ws": RWX, "kim-data": RWX})
    assert c2["root"] == "/home/irteam/kim-ws/gq" and any("GQ_ROOT" in w for w in w2)
    assert gqctl.root_volume(c2)[1] == "gq"


def test_gqctl_plain_kubeflow():
    # A plain Kubeflow notebook: a PodDefault selected by label (no MLXP zone), no node list.
    nb = copy.deepcopy(NOTEBOOK)
    nb["metadata"]["labels"] = {"app": "kim-gpu4", "access-ml-pipeline": "true", "unrelated": "x"}
    pds = [{"spec": {"selector": {"matchLabels": {"access-ml-pipeline": "true"}}}}]
    c, w = gqctl.discover(nb, pds, pvc_info={"kim-ws": RWX, "kim-data": RWX},
                          node_selector={"node-pool": "gpu"})
    assert c["platform"] == "kubeflow" and c["zone"] is None and c["nodes"] == []
    assert c["pod_labels"] == {"access-ml-pipeline": "true"}           # only labels that select a PodDefault
    assert c["node_selector"] == {"node-pool": "gpu"}
    assert any("scheduler-placement mode" in x for x in w) and any("node_capacity is the default" in x for x in w)
    gqd.load_config(None, **c)
    # PodDefaults unreadable: fall back to MLXP's *-pod-default naming.
    nb2 = copy.deepcopy(NOTEBOOK)
    c2, w2 = gqctl.discover(nb2, None, pvc_info={"kim-ws": RWX, "kim-data": RWX})
    assert c2["pod_labels"] == {"h200-pod-default": "true", "private-z1-pod-default": "true"}
    assert any("could not read PodDefaults" in x for x in w2)
    # Node capacity from Node objects: the smallest GPU node, memory rounded down to Gi.
    nodes = [{"status": {"allocatable": {"nvidia.com/gpu": "8", "cpu": "126", "memory": "2060000000Ki"}}},
             {"status": {"allocatable": {"nvidia.com/gpu": "4", "cpu": "64", "memory": "1000Gi"}}},
             {"status": {"allocatable": {"cpu": "32", "memory": "64Gi"}}}]    # CPU-only node ignored
    assert gqctl.node_capacity_of(nodes) == {"gpu": 4, "cpu": 64, "mem": "1000Gi"}
    assert gqctl.node_capacity_of([]) is None


def test_scheduler_placement_mode():
    c = cfg(nodes=[], node_selector={"node-pool": "gpu"})
    q = [gqd.validate(req(rid="261108-061530-%04x" % i, gpus=g), c) for i, g in enumerate([(4, 4), (1, 8)])]
    dec, after = gqd.place(q, [], c, 0, 0)
    assert dec[0][1] == {"node": None, "gpus": 4, "cpu": 60.0, "mem": 980 * 2**30}   # no per-node fit, just limits
    assert dec[1][1]["reason"] == "Cap"                                                 # max_gpus_total 4 used up
    dec, _ = gqd.place(q[1:], [], c, 0, 1)
    assert dec[0][1]["node"] is None and dec[0][1]["gpus"] == 3                        # range trimmed to the limit
    j = gqd.build_job(q[0], None, 4, 60.0, 980 * 2**30, 1, c)
    spec = j["spec"]["template"]["spec"]
    assert "affinity" not in spec and spec["nodeSelector"] == {"node-pool": "gpu"}
    assert j["metadata"]["annotations"]["gq/node"] == ""
    with sandbox() as home:
        c = cfg(nodes=[], volumes=[{"pvc": "ws", "mountPath": home}], root=home + "/gq")
        kube, clock = FakeKube(), Clock()
        d = gqd.Daemon(c, os.environ["GQ_ROOT"], kube, clock=clock)
        rid = json.loads(run_gq("submit", "--gpus", "2", "--time", "1h", "--", "true")[1])["id"]
        d.loop_once()
        st = status(rid)
        assert st["state"] == "pending" and st["node"] is None and st["pinned"] is False
        name = st["job"]
        kube.pods.append(kpod(name, node=None, phase="Pending", conditions=[
            {"type": "PodScheduled", "status": "False", "reason": "Unschedulable", "message": "no GPUs"}]))
        d.loop_once()
        clock.advance(120)
        d.loop_once()
        assert name not in kube.deleted and status(rid)["state"] == "pending"   # waits for the scheduler
        cap = json.loads(run_gq("capacity", "--json")[1])
        assert cap["placement"] == "scheduler" and cap["max_gpu_free_on_node"] is None
        code, out, _ = run_gq("capacity", "--fit", "1")
        assert code == 1 and json.loads(out)["fit"] is None
        assert "no node list configured" in run_gq("capacity")[1]


def test_gqctl_notebook_side_scripts():
    """The scripts gqctl install runs in the notebook, run here against a temp $HOME."""
    import subprocess
    tmp = tempfile.mkdtemp(prefix="gqrc-")
    try:
        home, root = os.path.join(tmp, "home"), os.path.join(tmp, "home", "gq")
        os.makedirs(os.path.join(root, "bin"))
        env = dict(os.environ, HOME=home)
        with open(os.path.join(root, "env.sh"), "w") as f:
            f.write(gqctl.render_env_sh(root))
        # shell rc: added once, idempotent, ~/.zshrc only if present, symlinks left alone
        script = gqctl.render_rc_script(root)
        out1 = subprocess.run(["bash", "-s"], input=script, text=True, capture_output=True, env=env, check=True).stdout
        out2 = subprocess.run(["bash", "-s"], input=script, text=True, capture_output=True, env=env, check=True).stdout
        assert "ADDED %s/.bashrc" % home in out1 and "PRESENT %s/.bashrc" % home in out2 and ".zshrc" not in out1
        rc = open(os.path.join(home, ".bashrc")).read()
        assert rc.count(gqctl.RC_MARKER) == 1
        os.symlink(os.path.join(tmp, "managed-zshrc"), os.path.join(home, ".zshrc"))
        open(os.path.join(tmp, "managed-zshrc"), "w").close()
        out3 = subprocess.run(["bash", "-s"], input=script, text=True, capture_output=True, env=env, check=True).stdout
        assert "SKIP_SYMLINK %s/.zshrc" % home in out3 and open(os.path.join(tmp, "managed-zshrc")).read() == ""
        # sourcing the rc (twice) gives GQ_ROOT and gq on PATH exactly once
        probe = 'source "$HOME/.bashrc"; source "$HOME/.bashrc"; echo "$GQ_ROOT"; echo "$PATH" | tr ":" "\\n" | grep -c "^%s/bin$"' % root
        res = subprocess.run(["bash", "-c", probe], text=True, capture_output=True, env=env, check=True).stdout.split()
        assert res == [root, "1"], res
        # Claude settings: created, merged into existing (with one backup), idempotent, unparseable left alone
        sp = os.path.join(home, ".claude", "settings.json")
        run_py = lambda: subprocess.run(["python3", "-", sp, json.dumps(gqctl.CLAUDE_RULES)],
                                        input=gqctl.CLAUDE_SETTINGS_PY, text=True, capture_output=True, check=True).stdout
        assert run_py().startswith("UPDATED")
        json.dump({"model": "x", "permissions": {"allow": ["Bash(ls *)"], "deny": []}}, open(sp, "w"))
        assert run_py().startswith("UPDATED") and run_py().startswith("PRESENT")
        merged = json.load(open(sp))
        assert merged["model"] == "x" and merged["permissions"]["allow"] == ["Bash(ls *)", "Bash(gq *)"]
        assert merged["permissions"]["deny"] == ["Bash(kubectl *)"] and os.path.exists(sp + ".before-gq")
        open(sp, "w").write("{not json")
        assert run_py().startswith("SKIP_UNPARSEABLE") and open(sp).read() == "{not json"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    # lockdown picks every notebook that mounts one of the shared volumes
    nbs = [{"metadata": {"name": n}, "spec": {"template": {"spec": {"volumes": [
        {"name": "v", "persistentVolumeClaim": {"claimName": c}} for c in claims]}}}}
        for n, claims in (("alice-cpu", ["alice-ws"]), ("alice-gpu", ["alice-ws", "alice-data"]),
                          ("bob-gpu", ["bob-ws"]), ("alice-tmp", []))]
    assert gqctl.notebooks_using(nbs, ["alice-ws"]) == ["alice-cpu", "alice-gpu"]
    # unlock targets only the locked ones
    nbs[1]["spec"]["template"]["spec"]["automountServiceAccountToken"] = False
    assert gqctl.locked(nbs) == ["alice-gpu"]


def test_gqctl_set():
    """`gqctl set` changes only the named keys, and a value gqd would refuse never reaches the file."""
    tmp = tempfile.mkdtemp()
    try:
        path = os.path.join(tmp, "c.json")
        shutil.copy(os.path.join(os.path.dirname(os.path.abspath(__file__)), "examples", "config.mlxp.json"), path)
        before = json.load(open(path))
        run = lambda *args: gqctl.main(["--config", path, "set", *args, "--no-up"])
        assert run("max_gpus_total=8", "reuse_grace=30") == 0
        after = json.load(open(path))
        assert after.pop("max_gpus_total") == 8 and after.pop("reuse_grace") == 30
        before.pop("max_gpus_total", None)
        assert after == before
        for bad in ("max_gpus_total=-1", "max_gpus_total=x", "max_gpus_total=1.5", "reuse_grace=301",
                    "nope=1", "instance=other", "max_gpus_total"):
            assert run(bad) == 1, bad
        assert json.load(open(path))["max_gpus_total"] == 8 and os.listdir(tmp) == ["c.json"]
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_completion():
    """Generated bash completion: subcommands, per-subcommand options, job ids, value options."""
    tmp = tempfile.mkdtemp()
    try:
        os.makedirs(os.path.join(tmp, "jobs", "251108-101010-ab12"))
        script = os.path.join(tmp, "c.bash")
        open(script, "w").write(gqctl.completion_script())

        def comp(*words):
            sh = ('source %s; COMP_WORDS=(%s); COMP_CWORD=%d; _%s_complete; echo "${COMPREPLY[*]}"'
                  % (script, " ".join("'%s'" % w for w in words), len(words) - 1, words[0]))
            return subprocess.run(["bash", "-c", sh], capture_output=True, text=True, check=True,
                                  env=dict(os.environ, GQ_ROOT=tmp)).stdout.split()
        assert {"run", "capacity", "--help"} <= set(comp("gq", "")) and "unlock" not in comp("gq", "")
        assert comp("gq", "st") == ["status"]
        assert "251108-101010-ab12" in comp("gq", "logs", "")
        assert comp("gq", "run", "--t") == ["--time"]
        assert comp("gq", "run", "--time", "") == []          # value: default completion
        assert comp("gq", "run", "--time", "1h", "--", "py") == []
        assert comp("gqctl", "--config", "x.json", "un") == ["unlock"]
        assert comp("gqctl", "bogus", "") == []
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_worker_script():
    """The Job's bash worker: runs a job, takes a handed-over one, then closes `next` and exits."""
    tmp = tempfile.mkdtemp()
    try:
        jobs = os.path.join(tmp, "jobs")
        a, b = "261108-070000-aaaa", "261108-070001-bbbb-2"
        for rid, body in ((a, "echo A; exit 3"), (b, 'echo "$GQ_ID $GQ_TASK_INDEX $GQ_TASK_COUNT"')):
            os.makedirs(os.path.join(jobs, rid))
            open(os.path.join(jobs, rid, "run.sh"), "w").write(body + "\n")
        quick = gqd.WORKER % {"root": tmp, "grace": 0}
        r = subprocess.run(["bash", "-c", quick], env=dict(os.environ, GQ_ID=a), timeout=10)
        assert r.returncode == 3 and not os.path.exists(os.path.join(jobs, a, "next"))   # grace 0: old behaviour
        os.remove(os.path.join(jobs, a, "exit"))
        w = subprocess.Popen(["bash", "-c", gqd.WORKER % {"root": tmp, "grace": 2}], env=dict(os.environ, GQ_ID=a))
        end = time.monotonic() + 10
        while not os.path.exists(os.path.join(jobs, a, "exit")) and time.monotonic() < end:
            time.sleep(0.05)
        fd = os.open(os.path.join(jobs, a, "next"), os.O_WRONLY | os.O_CREAT | os.O_EXCL)
        os.write(fd, ("%s 2 5\n" % b).encode())
        os.close(fd)
        assert w.wait(timeout=15) == 0                                   # exit code of the last job
        read = lambda rid, f: open(os.path.join(jobs, rid, f)).read().strip()
        assert read(a, "exit") == "3" and read(b, "exit") == "0" and read(b, "log") == "%s 2 5" % b
        assert gqd.parse_ts(read(b, "started")) and read(b, "next") == "closed"   # closed it, so gqd cannot
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_worker_cleanup_as_pid1():
    """As the pod's PID 1 the worker kills what a job left running before the next job starts.
    Needs bash as PID 1, so it runs in a container; skipped without docker and a local debian image."""
    image = "debian:stable-slim"
    if subprocess.run(["docker", "image", "inspect", image], capture_output=True).returncode:
        print("    skipped: needs docker and %s" % image)
        return
    tmp = tempfile.mkdtemp()
    try:
        a, b = "261109-100000-aaaa", "261109-100001-bbbb"
        for rid, body in ((a, "sleep 300 & setsid sleep 300 & nohup sleep 300 >/dev/null 2>&1 &\n"
                              "bash -c 'trap \"\" TERM; sleep 300' &"),
                          (b, "ls /proc | grep -cE '^[0-9]+$'")):
            os.makedirs(os.path.join(tmp, "jobs", rid))
            open(os.path.join(tmp, "jobs", rid, "run.sh"), "w").write(body + "\n")
        w = subprocess.Popen(["docker", "run", "--rm", "-e", "GQ_ID=" + a, "-v", tmp + ":/r", image,
                              "bash", "-c", gqd.WORKER % {"root": "/r", "grace": 5}])
        end = time.monotonic() + 60
        while not os.path.exists(os.path.join(tmp, "jobs", a, "exit")) and time.monotonic() < end:
            time.sleep(0.1)
        fd = os.open(os.path.join(tmp, "jobs", a, "next"), os.O_WRONLY | os.O_CREAT | os.O_EXCL)
        os.write(fd, (b + "\n").encode())
        os.close(fd)
        assert w.wait(timeout=60) == 0
        # B sees PID 1, its own bash, ls and grep: none of A's four sleeps survived
        assert open(os.path.join(tmp, "jobs", b, "log")).read().strip() == "4"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_reuse_rules():
    c = cfg(max_gpus_total=2)
    idle = [{"job": "gq-t-x-a1", "rid": "x", "node": "n-a", "gpus": 1, "cpu": 15.0, "mem": 245 * 2**30}]
    nodes = gqd.capacity([], [], c)["nodes"]
    one, two = gqd.validate(req(), c), gqd.validate(req("261108-061530-b2b2", gpus=(2, 2)), c)
    # cap is full (1 idle + 1 running), yet the 1-GPU job takes over the idle pod: it adds nothing
    dec = gqd.place([one], nodes, c, running=2, gq_gpus=2, idle=idle)[0]
    assert dec[0][1]["reuse"] is idle[0] and dec[0][1]["node"] == "n-a"
    assert "reuse" not in gqd.place([two], nodes, c, 1, 1, idle)[0][0][1]       # other GPU count: no
    big_mem = gqd.validate(req(mem="300Gi"), c)
    assert not gqd.reusable(big_mem, idle[0], c)                                   # needs more memory than the pod
    assert gqd.reusable(gqd.validate(req(gpus=(1, 4)), c), idle[0], c)             # range containing it: yes
    for bad in (301, -1, 1.5, True):
        try:
            cfg(reuse_grace=bad)
            raise AssertionError("reuse_grace %r accepted" % (bad,))
        except ValueError:
            pass
    assert gqd.validate(req(after=["261108-061530-c3c3", "261108-061530-c3c3"]), c)["after"] == ["261108-061530-c3c3"]
    for bad in ([], ["nope"], ["261108-061530-a1b2"], "261108-061530-c3c3"):
        try:
            gqd.validate(req(after=bad), c)
            raise AssertionError("after %r accepted" % (bad,))
        except gqd.Invalid:
            pass


def test_reuse_end_to_end():
    with sandbox() as home:
        c = cfg(volumes=[{"pvc": "ws", "mountPath": home}], root=home + "/gq", max_gpus_total=2)
        kube, clock = FakeKube(), Clock()
        d = gqd.Daemon(c, os.environ["GQ_ROOT"], kube, clock=clock)
        jd = lambda rid: os.path.join(os.environ["GQ_ROOT"], "jobs", rid)
        put = lambda rid, f, text: open(os.path.join(jd(rid), f), "w").write(text)
        sub = lambda *a: json.loads(run_gq("submit", "--time", "1h", *a, "--", "true")[1])["id"]

        a = sub("--gpus", "1")
        d.loop_once()
        job_a, node_a = status(a)["job"], status(a)["node"]
        kube.run(job_a, node_a)
        put(a, "started", gqd.iso(clock()))
        d.loop_once()
        assert status(a)["state"] == "running"
        b = sub("--gpus", "1", "--after", a)
        d.loop_once()
        assert (status(b)["state"], status(b)["reason"]) == ("queued", "After")

        put(a, "exit", "0\n")                     # A ends; its pod keeps running (the worker waits)
        d.loop_once()
        sb = status(b)
        assert status(a)["state"] == "succeeded" and status(b)["after"] == [a]
        assert (sb["state"], sb["reason"], sb["job"], sb["node"], sb["reused"]) == ("pending", "Handoff", job_a, node_a, True)
        assert open(os.path.join(jd(a), "next")).read() == b + "\n" and len(kube.jobs) == 1
        assert kube.jobs[job_a]["spec"]["activeDeadlineSeconds"] >= 3600 + 60 + 1800
        clock.advance(5)
        d.loop_once()
        assert status(b)["state"] == "pending"     # no `started` yet: not the old container's start time
        put(b, "started", gqd.iso(clock()))
        d.loop_once()
        assert status(b)["state"] == "running" and status(b)["started"] == gqd.iso(clock())
        assert "reused pod (held" in gq.row(status(b))[6]

        put(b, "exit", "0\n")
        d.loop_once()
        assert status(b)["state"] == "succeeded"
        capdoc = json.load(open(os.path.join(os.environ["GQ_ROOT"], "capacity.json")))
        assert [p["job"] for p in capdoc["idle_pods"]] == [job_a] and capdoc["gq_gpus_in_use"] == 1
        # a 2-GPU job cannot use the idle 1-GPU pod and is over the cap because of it: release it
        c2 = sub("--gpus", "2")
        d.loop_once()
        assert job_a in kube.deleted and not d.idle and status(c2)["reason"] == "Cap"
        d.loop_once()
        assert status(c2)["state"] == "pending"
        kube.delete_job(status(c2)["job"])

        # the worker closed first: the handoff fails and the job is placed normally
        e = sub("--gpus", "1")
        d.loop_once()
        kube.run(status(e)["job"], status(e)["node"])
        d.loop_once()
        f = sub("--gpus", "1", "--after", e, "--wait-timeout", "6h")
        d.loop_once()
        d.tracked[f]["submitted"] = gqd.iso(clock() - dt.timedelta(hours=7))  # as if E ran 7h: past f's --wait-timeout (6h)
        put(e, "next", "closed\n")
        put(e, "exit", "0\n")
        d.loop_once()
        assert status(f)["state"] == "queued" and not d.idle
        d.loop_once()
        assert status(f)["state"] == "pending" and status(f)["job"] != status(e)["job"] and not status(f).get("reused")
        d.loop_once()
        assert status(f)["state"] == "pending"   # --wait-timeout counts from when E ended, not from submit

        # the pod died after the handoff but before starting the job: placed again
        job_f = status(f)["job"]
        kube.run(job_f, status(f)["node"])
        d.loop_once()
        g = sub("--gpus", "1")
        put(f, "exit", "0\n")
        d.loop_once()
        assert status(g)["job"] == job_f and status(g)["reason"] == "Handoff"
        kube.finish(job_f, 0)
        d.loop_once()
        sg = status(g)
        assert sg["reason"] in ("HandoffFailed", "Created") and sg["attempt"] == 2 and not sg.get("reused")

        # the event log tells the whole story, including how long each pod lived and what ran in it
        evs = [json.loads(l) for f in sorted(glob.glob(os.path.join(os.environ["GQ_ROOT"], "events", "*.jsonl")))
               for l in open(f)]
        of = lambda rid: [e["event"] for e in evs if e.get("id") == rid]
        assert of(a) == ["submitted", "placed", "started", "finished"]
        assert of(b) == ["submitted", "queued", "handoff", "started", "finished"]
        assert of(g)[:3] == ["submitted", "handoff", "requeued"]
        ended = {e["job"]: e for e in evs if e["event"] == "pod_ended"}
        assert ended[job_a]["tasks"] == [a, b] and ended[job_a]["outcome"] == "Deleted"
        assert ended[job_f]["tasks"] == [f, g] and ended[job_f]["outcome"] == "Complete"
        assert any(e["event"] == "pod_released" and e["job"] == job_a for e in evs)
        code, out, _ = run_gq("events", "--job", job_a)
        assert code == 0 and "pod_ended" in out and "lived" in out and "takes over the pod of " + a in out
        # a restarted gqd does not log the same pod_ended twice
        d2 = gqd.Daemon(c, os.environ["GQ_ROOT"], kube, clock=clock)
        d2.loop_once()
        again = [l for f in glob.glob(os.path.join(os.environ["GQ_ROOT"], "events", "*.jsonl")) for l in open(f)
                 if '"pod_ended"' in l and job_f in l]
        assert len(again) == 1


def test_retention():
    with sandbox() as home:
        c = cfg(volumes=[{"pvc": "ws", "mountPath": home}], root=home + "/gq")
        clock = Clock()
        d = gqd.Daemon(c, os.environ["GQ_ROOT"], FakeKube(), clock=clock)
        rid = json.loads(run_gq("submit", "--local", "--time", "1m", "--", "true")[1])["id"]
        run_gq("wait", rid, "--timeout", "30s")
        jd = os.path.join(os.environ["GQ_ROOT"], "jobs", rid)
        old = os.path.join(os.environ["GQ_ROOT"], "events", "2025-01.jsonl")
        open(old, "w").write("{}\n")
        clock.advance(2 * 86400)                                     # past log_retention_days (1)
        d._gc(clock())
        assert not os.path.exists(os.path.join(jd, "log")) and not os.path.exists(os.path.join(jd, "run.sh"))
        assert os.path.exists(os.path.join(jd, "status.json")) and status(rid)["state"] == "succeeded"
        assert not os.path.exists(old)                               # event files older than a year go
        code, _, err = run_gq("logs", rid)
        assert code == 0 and "was removed" in err
        clock.advance(31 * 86400)                                    # past retention_days (30)
        d._gc(clock())
        assert not os.path.exists(jd)


def test_local_backend():
    with sandbox():
        code, out, err = run_gq("run", "--local", "--time", "30s", "--", "bash", "-c", "echo hello; exit 3")
        assert code == 3, (code, out, err)
        assert "hello" in out
        rid = json.loads(err.splitlines()[0])["id"]
        assert status(rid)["state"] == "failed" and status(rid)["exit_code"] == 3

        code, _, err = run_gq("run", "--local", "--time", "1s", "--", "sleep", "30")
        assert code == 125 and '"timeout"' in err

        rid = json.loads(run_gq("submit", "--local", "--time", "1m", "--", "sleep", "30")[1])["id"]
        assert status(rid)["state"] == "running"
        t0 = time.monotonic()
        run_gq("kill", rid)
        code, _, err = run_gq("wait", rid, "--timeout", "15s")
        assert code == 125 and '"killed"' in err and time.monotonic() - t0 < 15

        out = json.loads(run_gq("submit", "--local", "--array", "3", "--time", "30s", "--",
                                "bash", "-c", "echo task $GQ_TASK_INDEX of $GQ_TASK_COUNT")[1])
        assert run_gq("wait", out["id"], "--timeout", "15s")[0] == 0
        logs = run_gq("logs", out["id"])[1]
        assert "[2] task 2 of 3" in logs, logs


if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    for name, fn in tests:
        t0 = time.monotonic()
        fn()
        print("ok  %-45s %.1fs" % (name, time.monotonic() - t0))
    print("all %d tests passed" % len(tests))
