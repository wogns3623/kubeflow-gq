---
name: gq
description: Run GPU or heavy CPU work on the cluster from this notebook with the `gq` CLI (submit, wait, logs, kill, capacity). Use whenever a task needs a GPU (training, evaluation, CUDA checks, nvidia-smi), more CPU than a quick command, or anything longer than a few minutes; also to check free GPUs or manage running gq jobs. This notebook has no Kubernetes access, so never use kubectl.
---

# gq: GPU/CPU jobs on the cluster

This notebook has **no Kubernetes token**. `kubectl` fails here by design.
Run GPU or heavy work through `gq`, which hands it to the user's `gqd` daemon. Each job runs in its own pod on a GPU node. The pod has the same image, the same mounted volumes (your files at the same paths) and the environment you submit from (conda env, PATH and so on).

## Before you start

```bash
export GQ_OWNER="claude:$(date +%s)"   # once per session; `gq ps --mine` and kills use it
gq capacity                             # nodes, free GPUs per node, gq limits
```

The nodes are shared with other people. **A job runs on one node only.** A 4-GPU job needs 4 free GPUs on a single node. Total free GPUs do not count.

## Pick the command

| Situation | Command |
|---|---|
| Quick GPU check (< 1 min) | `gq run --gpus 1 --time 5m -- python -c "import torch; print(torch.cuda.is_available())"` |
| Long job | `gq submit --gpus 2 --time 6h --name my-eval -- python eval.py ...`, then `gq wait <id> --timeout 30m` |
| Independent pieces (shards, seeds, models, sweeps) | `gq submit --array 8 --gpus 1 --time 2h -- bash -c 'python eval.py --shard $GQ_TASK_INDEX --num-shards $GQ_TASK_COUNT'` |
| Several light pieces (each uses a fraction of one GPU) | One `--gpus 1` job running them together; see "Light pieces share one GPU" below |
| Program that adapts to any GPU count | `gq submit --gpus 1-4 --time 8h -- bash -c 'torchrun --nproc_per_node $GQ_GPUS train.py'` |
| Next step after another job, regardless of its result | `gq submit --after <id> --gpus 1 --time 1h -- python eval.py` |
| CPU job that fits this notebook | `gq submit --local --time 1h -- python preprocess.py` |
| Larger CPU job | `gq submit --cpu 32 --mem 128Gi --time 2h -- python preprocess.py` |

- **Working directory:** run commands from the project directory. The job starts in the same directory. Use `--cwd` to change it.
- **Files the job can see:** only shared (ReadWriteMany) volumes are mounted in jobs. If gq says a path "is on a volume Job pods cannot mount", the job cannot see those files. Move the work to a shared volume (the error lists them) or use `--local`. Code, data and conda envs a job needs must also live on a shared volume.
- **Resources:** `--cpu` and `--mem` are **limits**. The scheduler request stays small, so placement depends on free GPUs. A job may use more CPU when the node is idle but gets less when it is busy. Raise `--mem` if a job is OOMKilled.

## Deciding how to split GPU work

1. Check `gq capacity`.
2. If the work splits into independent pieces, use `--array N --gpus 1` (or 2). Small tasks start as soon as any GPU frees up.
   - **Light pieces share one GPU instead.** If each piece uses only a small part of a GPU (small model, low utilization, a few GB of memory), run 2–4 of them as background processes inside one `--gpus 1` job. Two gq jobs never share a GPU: each job gets whole GPUs.

     ```bash
     gq submit --gpus 1 --time 2h --name light-pair -- bash -c '
       python a.py > a.log 2>&1 & p1=$!
       python b.py > b.log 2>&1 & p2=$!
       wait $p1; r1=$?; wait $p2; r2=$?
       exit $(( r1 || r2 ))'
     ```

   - Cap each process's GPU memory so one cannot take it all (PyTorch: `torch.cuda.set_per_process_memory_fraction(1/k)`; JAX: `XLA_PYTHON_CLIENT_MEM_FRACTION`).
   - Wait on each pid and exit non-zero if any failed. A bare `wait` returns 0 even when a piece failed. Write each piece's output to its own log file.
   - Set `--time` for the slowest piece. If they contend for the GPU, each runs slower than alone.
3. If one program truly needs N GPUs, run `gq capacity --fit N`. Exit 0 means a node has N free now.
4. If nothing fits now but the program can run on fewer GPUs, use `--gpus LO-HI`.
5. Otherwise submit and wait. Set `--wait-timeout` (default 6h) to how long you are willing to wait.

## Follow-up jobs keep the GPU

When a GPU job ends, its pod waits about a minute. A queued job with the **same GPU count** (and CPU/memory limits no larger) takes it over at once instead of queueing again. You do not choose the pod; gqd does.

- After a job finishes, read its result and submit the next step promptly (within a minute) with the same `--gpus` to keep the GPU.
- To chain without waiting, use `--after <id>`. It starts when that job ends, whether it succeeded or failed. Decide branches yourself: check the result, or let the next script check for its inputs.
- Do not hold a GPU on purpose (sleep loops, idle jobs). If nothing follows, let the pod go.

## Rules

- **Always set `--time`** to about 1.5× the expected run time. Jobs are killed when it runs out (state `timeout`).
- **Wait with `gq wait <id> --timeout ...`**, not sleep loops. Exit 124 means your wait timed out but the job is still running.
- Do not resubmit the same job repeatedly when it is `queued`. Read the reason first (`gq status <id>`).
  - `NoFit`: no node has room. Wait, or split it.
  - `Cap`: the gq limit for this user is reached (`gq capacity` shows `gq used/limit`). Wait for your other jobs.
- Kill only your own jobs (`gq ps --mine`). Kill jobs you no longer need right away. GPUs are shared.
- `--name` is visible to everyone in the namespace. Never put secrets in it.
- Never use `kubectl`. It has no credentials here.

## Reading results

- `gq status <id>` (or `--json`) shows the state, reason, node, GPUs and exit code.
- `gq logs <id> [-f] [--tail 100]` prints the log. Logs live in `$GQ_ROOT/jobs/<id>/log` (default `~/gq`).
- `gq ps` shows recent jobs. Add `--json` for machine-readable output.
- Job output (`log`) is deleted a day after the job ends. Write anything you need to keep to your project directory from the program itself. `gq events --job <id>` keeps gq's record of the job (placement, start, end, exit code) for a year.
- Exit codes of `gq run` and `gq wait`:
  - The job's own exit code when it ran.
  - **125** when it did not end normally (`rejected`, `expired`, `timeout`, `lost`, `killed`). The reason is printed as JSON on stderr.
  - **124** when `wait --timeout` passed.
- States: `submitted` → `queued` → `pending` → `running` → `succeeded` / `failed`. Abnormal ends: `timeout`, `expired` (never started within `--wait-timeout`), `lost` (pod vanished, e.g. node failure), `killed`, `rejected` (invalid request).
- `failed` with reason `OOMKilled` means the job ran out of memory. Raise `--mem`.
- If commands warn that the gqd heartbeat is missing or old, gqd is down. Tell the user; you cannot fix it from here.

## Inside a job

- Env vars available: `GQ_ID`, `GQ_GPUS` (GPUs granted), `GQ_CPU`, and for arrays `GQ_TASK_INDEX` and `GQ_TASK_COUNT`.
- `OMP_NUM_THREADS` defaults to CPU / GPUs unless you set it.
- Write results to your project directory. Job directories are deleted after the retention period (default 30 days).
