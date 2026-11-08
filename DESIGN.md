# gq 설계 — Kubeflow 노트북용 GPU/CPU 작업 실행기

상태: v0.1 구현됨, 배포 전 (설치와 사용법은 README.md) · 작성: 2026-11-08

상시 실행 중인 GPU 없는 노트북에서 GPU/CPU 작업을 던지고, 실행 중인 모든 작업을 한 곳에서 관리한다.
작업은 사람과 에이전트(Claude Code 등)가 모두 실행할 수 있고, 에이전트가 쓰더라도 공유 클러스터에서 사고가 나지 않아야 한다.

처음에는 MLXP(여러 사용자가 쓰는 Kubeflow 기반 GPU 플랫폼)의 한 사용자 환경을 기준으로 설계했고, 이후 누구나 자기 인스턴스를 띄워 쓸 수 있게, 그리고 일반 Kubeflow에서도 동작하게 일반화했다(14장). 본문의 이름과 값(`alice`, `team-ml`, `h200-node-a` 등)은 **가상의 예시**다.

---

## 1. 요구사항

1. nogpu2 세션에서 노트북을 따로 띄우지 않고 GPU/CPU 작업을 실행한다.
2. GPU 자원은 작업이 끝나면 바로 반납한다. 여러 사람이 함께 쓰는 클러스터다.
3. 실행 중인 작업 전체를 한 곳에서 보고(`ps`, `logs`) 멈출(`kill`) 수 있다.
4. nogpu2 안의 에이전트가 사람의 개입 없이 작업을 시작하고 관리할 수 있다.
5. 에이전트가 네임스페이스의 다른 리소스(다른 사람의 노트북, PVC 등)를 건드릴 수 없어야 한다.
6. 노드별 남은 자원을 알 수 있고, 단편화를 고려해 작업을 배치하거나 쪼갤 수 있다. 워크스페이스는 H200 8장 서버 4대이고, 한 작업은 한 노드에만 올라간다. 예를 들어 네 노드에 GPU가 1장씩만 남아 있으면 4장짜리 작업은 배정할 수 없다.

## 2. 환경 (2026-11-08 확인)

| 항목 | 값 |
|---|---|
| context / namespace | `<context>` / `team-ml` (여러 사람이 함께 쓰는 네임스페이스) |
| k8s | v1.33.4 |
| 내 노트북 | `alice-cpu` (상시, CPU 64, 메모리 512Gi), `alice-gpu1`, `alice-gpu1-big`, `alice-gpu2`, `alice-gpu2-b`, `alice-gpu4` |
| 공유 볼륨 | PVC `alice-ws` → `/home/irteam` (실제 경로 `/home1/irteam`, 공유 파일시스템) |
| 이미지 | `example/mlxp-notebook:cu128` (모든 노트북 공통, 기본 사용자 `irteam`(uid 500), kubectl·kfp 2.7·kubernetes 파이썬 클라이언트·tmux 포함) |
| 셸 환경 | `bash -lc`에서만 conda `base`가 잡힌다. 다른 env(예: `sliderquant`)는 PVC의 `~/.conda/envs`에 있다 |
| 노트북 정지/시작 | annotation `kubeflow-resource-stopped=<ts>` 추가/삭제 |
| 노트북 SA | `default-editor`. 노트북·PVC 삭제, secret·Job 생성, pods/exec 가능. Role·RoleBinding·NetworkPolicy 생성은 **불가**. 노드 목록 조회도 불가 |
| 스케줄링 | MLXP 웹훅이 zone 라벨을 요구한다. pod 라벨 `h200-pod-default: "true"`, `private-h200-acme-0-pod-default: "true"`를 붙이면 PodDefault가 zone/gpu-type annotation을 넣고, 웹훅이 nodeSelector(H200, zone)와 priorityClass `private-zone`을 넣는다 |
| GPU 노드 | zone `private-h200-acme-0`의 4대: `h200-node-a`, `h200-node-d`, `h200-node-c`, `h200-node-b`. 각 H200 8장 |
| 자원 조회 권한 | Zone 객체: 사용자 계정은 읽을 수 있고 SA(`default-editor`)는 **불가**. Node 객체: 둘 다 불가. `team-ml` pod 목록과 pod metrics(`metrics.k8s.io`): 둘 다 가능 |
| 워크스페이스 | `acme`에는 프로젝트가 `ark`(=`team-ml`) 하나뿐이다. 이 4대의 GPU를 쓰는 것은 사실상 `team-ml` pod뿐이다 |
| 사용량 예 (2026-11-08) | node-d 8/8, node-b 7/8, node-c 7/8, node-a 6/8. 남은 GPU는 총 4장이지만 한 노드에 최대 2장 |
| `ManagementQuota` | GPU 수 정보가 없다 |
| istio | 네임스페이스에 injection 활성. istio-proxy는 native sidecar(initContainer, `restartPolicy: Always`)로 들어가므로 Job 완료를 막지 않는다 |
| 사용 현황 | 노트북 중심으로 운영되고 Job·PyTorchJob·Workflow·KFP run은 거의 쓰지 않는다. 일부는 `sleep infinity` bare Pod에 exec하는 방식을 쓴다 |

## 3. 설계 결정

| 결정 | 이유 | 버린 대안 |
|---|---|---|
| **작업 1개 = k8s Job 1개** | 끝나면 pod가 내려가서 GPU가 자동으로 반납된다. 노트북 켜기/끄기와 idle 감시가 필요 없다 | 노트북을 켜고 exec 후 idle 정지: 동작은 확인됐지만 시작/정지/idle 로직이 필요하고 요구사항 1과 맞지 않는다 |
| **plain Job** | 명령을 그대로 실행하고 PVC를 직접 마운트한다. 에이전트가 쓰기에 가장 단순하다 | KFP: component로 감싸야 하고 `kfp-kubernetes`가 없으며 시작이 느리다 · PyTorchJob/TrainJob: 단일 노드 작업에는 과하다 · SkyPilot/Ray/Kueue: 같은 웹훅 문제가 있거나, 관리자 권한이 필요하거나, 과하다 |
| **상태는 파일과 k8s에 둔다 (gqd는 무상태)** | 대기열은 spool 파일 자체, 실행 상태는 k8s Job, 결과는 PVC 파일에 둔다. gqd가 재시작되어도 잃는 것이 없다 | DB/큐 서버: 필요 없다 |
| **명령과 env는 Job spec에 넣지 않는다** | `team-ml`의 다른 사람도 `kubectl get job -o yaml`로 볼 수 있다. 명령과 env(API 키 포함 가능)는 PVC 파일로만 전달한다. 셸 이스케이프 문제도 함께 사라진다 | spec에 cmd/env 넣기 |
| **권한 경계 = pod 경계** | 이 환경에서 강제할 수 있는 유일한 경계다 (7장) | Claude Code 권한 규칙, 샌드박스, 좁은 SA: 7.3 참고 |
| **자원은 `team-ml` pod 목록으로 추정한다** | 노드를 조회할 수 없다. 워크스페이스에 프로젝트가 하나뿐이라 `team-ml` pod 사용량 합계로 노드별 여유를 추정할 수 있다 (5.13) | MLXP UI/API: 에이전트가 접근할 수 없다 · Pending 사유만 보기: 노드별 정보가 없다 |
| **gqd가 노드를 직접 고른다 (best-fit)** | 기본 스케줄러는 작업을 퍼뜨리는 경향이 있어 단편화를 악화시킨다. 맞는 노드가 없으면 Pending pod를 만들지 않고 대기시킨다 (5.14) | k8s 스케줄러에 맡기기 |
| **분할은 에이전트가 정하고 gq는 수단만 준다** | 명령을 자동으로 쪼갤 수는 없다. 배열 작업과 가변 GPU 수, 그리고 자원 조회를 제공한다 | 여러 노드에 걸친 분산 실행: v1 범위 밖 |

## 4. 아키텍처

```
[토큰 없음: 에이전트 영역]                         [토큰 있음: 작고 고정된 코드]
alice-cpu (automount 끔)                       gqd-alice (Deployment, 상시)
 사람 / Claude Code 등 에이전트                    · 코드·설정은 ConfigMap (PVC 아님)
 gq CLI ── 요청 ──▶ ~/gq/spool/  ◀──── 감시 ────── · 요청 검증 → 노드 선택 → Job 생성/삭제
        ◀─ status ─ ~/gq/jobs/<id>/status.json ◀── · 실행 시간·대기 시간 상한 집행
        ◀─ 자원 ─── ~/gq/capacity.json ◀────────── · team-ml pod 목록으로 노드별 여유 계산
   │                                                    │ Job 생성 (spec엔 id만)
   └─ --local: nogpu2에서 직접 실행 (supervisor)     Job pod (토큰 없음, PVC 전체, GPU)
                                                     ~/gq/jobs/<id>/run.sh 실행 → log, exit
```

### 4.1 구성요소

| 구성요소 | 위치 | 의존성 | 권한 |
|---|---|---|---|
| `gq` (CLI) | `~/gq/bin/gq` (PVC) | Python 표준 라이브러리만 | 없음 (파일만 읽고 씀) |
| `gqd` (데몬) | ConfigMap `gqd-<instance>` → `/opt/gqd/gqd.py` | 표준 라이브러리 + `kubernetes` | `default-editor` 토큰 |
| `gqd` 설정 | ConfigMap `gqd-<instance>` → `/opt/gqd/config.json` | | |
| 스킬 | `~/.claude/skills/gq/SKILL.md` (PVC) | | |
| `gqctl` (설치·운영) | 사용자의 PC (kubectl이 되는 곳) | 표준 라이브러리 + `kubectl` | 사용자 본인의 kubeconfig |

## 5. 상세 설계

### 5.1 ID와 디렉터리

- ID: `YYMMDD-HHMMSS-xxxx` (UTC, `xxxx`는 무작위 hex 4자리). 예: `261108-061530-a1b2`. 배열 작업의 task는 `-<k>`를 붙인다(`261108-061530-a1b2-3`). 정규식은 `^\d{6}-\d{6}-[0-9a-f]{4}(-\d{1,3})?$`.
  - 사람이 읽기 쉽고 시간순으로 정렬된다. Job 이름은 `gq-<instance>-<id>-a<시도 번호>`이고(최대 45자), DNS-1123 규칙과 63자 제한을 만족한다. 시도 번호는 재배치(5.14)할 때 올라간다. 지우는 중인 이전 Job과 이름이 겹치지 않게 하기 위해서다.
  - gqd는 정규식에 맞지 않는 ID를 거부한다. 경로 주입은 이것으로 막는다.

```
~/gq/
  bin/gq
  gqd.heartbeat              # gqd가 매 루프마다 갱신 ({"ts":..., "version":...})
  capacity.json              # gqd가 매 루프마다 갱신 (5.13)
  spool/
    <id>.submit              # 제출 요청 (빈 파일, 내용은 jobs/<id>/request.json)
    <id>.kill                # 중지 요청
  jobs/<id>/
    request.json             # gq가 작성: 자원·시간·이름·owner·backend
    run.sh                   # gq가 작성: env 복원 + cd + exec cmd
    status.json              # 상태 (pod 작업은 gqd, 로컬 작업은 gq가 작성)
    log                      # stdout+stderr
    exit                     # 종료 코드 (정상 종료 시에만 생긴다)
```

### 5.2 제출 프로토콜

1. **gq:** ID를 만들고 `jobs/<id>/`에 `request.json`, `run.sh`를 쓴다. 그다음 `spool/.<id>.tmp`를 만들고 `spool/<id>.submit`으로 rename한다. 같은 파일시스템 안의 rename은 원자적이라 gqd가 반쯤 쓴 요청을 볼 일이 없다.
2. **gq:** `{"id": "...", "state": "submitted"}`를 출력하고 끝난다.
3. **gqd:** `spool/*.submit`을 ID 순서(=시간순)로 처리한다.
   - 검증에 실패하면 `rejected`를 기록하고 spool 파일을 지운다.
   - 상한을 넘거나 맞는 노드가 없으면(5.14) spool 파일을 **그대로 두고** `queued`(대기 순번과 사유 포함)를 기록한다. 대기열의 실체가 spool 파일이라 gqd가 재시작되어도 대기열이 유지된다.
   - 통과하면 노드를 고르고 Job `gq-<instance>-<id>-a<n>`을 만든다. `AlreadyExists`가 나면 이미 처리한 요청으로 본다(크래시 후 재처리 대비). 그다음 `pending`을 기록하고 spool 파일을 지운다.
4. **kill:** gq가 `spool/<id>.kill`을 만들면, gqd가 상태에 따라 처리한다.
   - `submitted`/`queued`이면 submit 파일을 지운다.
   - Job이 있으면 `gq=1` 라벨을 확인한 뒤 삭제한다(`propagationPolicy: Background`).
   - 어느 경우든 `killed`를 기록한다.

`request.json` 형식:

```json
{"id": "261108-061530-a1b2-3", "group": "261108-061530-a1b2", "task_index": 3, "task_count": 8,
 "name": "sq-eval", "owner": "claude:3f9a", "backend": "pod",
 "gpus": [1, 2], "cpu": null, "mem": null, "time": 14400, "wait_timeout": 21600,
 "cwd": "/home/irteam/SliderQuant", "submitted": "2026-11-08T06:15:30Z"}
```

- `gpus`는 `[최소, 최대]`이다. 고정 수이면 두 값이 같다.
- `cpu`/`mem`은 **limit**이다. `null`이면 gqd가 실제 GPU 수에 비례해 기본값을 정한다(`node_capacity` / GPU 수). request는 요청과 상관없이 작은 고정값이다(5.9).
- 배열 작업이 아니면 `group`·`task_index`·`task_count`는 생략한다. 배열 작업은 task마다 `jobs/<id>-<k>/`와 spool 파일을 따로 만든다.

명령과 env는 `request.json`에 넣지 않고 `run.sh`에만 둔다. gqd는 `run.sh`를 읽지 않는다.

### 5.3 상태 머신

```
submitted ─┬─▶ rejected
           ├─▶ queued ─┐  ◀──── 배치 실패 (placement_grace 초과) ────┐
           └───────────┴─▶ pending ─────────────────────────────────┘
          (queued·pending에서 wait_timeout 초과 → expired)    │
                                                               └─▶ running ─┬─▶ succeeded (exit 0)
                                                                            ├─▶ failed    (exit ≠ 0, OOMKilled 등)
                                                                            ├─▶ timeout   (time 초과)
                                                                            └─▶ lost      (exit 없이 pod 소멸: 축출·노드 장애)
 (submitted·queued·pending·running) ──kill──▶ killed
```

- 종료 상태는 `rejected`, `expired`, `succeeded`, `failed`, `timeout`, `lost`, `killed`이다.
- 판정 근거는 우선순위 순서로 다음과 같다.
  1. gqd가 직접 한 행동 (kill, timeout, expired)
  2. `exit` 파일
  3. pod `containerStatuses[main].state.terminated` (`reason`, `exitCode`). OOMKilled는 wrapper가 exit를 쓰기 전에 죽으므로 여기서 잡는다.
  4. 아무 근거 없이 Job이나 pod가 사라졌으면 `lost`
- `status.json` 형식:

```json
{"id": "...", "group": null, "name": "...", "owner": "...", "backend": "pod", "state": "queued",
 "reason": "NoFit", "message": "4 GPUs needed on one node; max free on a node is 2 (total free 4)",
 "queue_position": 2, "gpus": null, "gpus_requested": [4, 4], "time": 14400,
 "submitted": "...", "started": null, "finished": null, "exit_code": null,
 "node": null, "updated": "..."}
```

- `queued`의 `reason`은 `NoFit`(맞는 노드 없음), `Cap`(gq 상한), `Replace`(재배치 중), `ApiError`(Job 생성 실패, 다음 루프에 재시도) 중 하나다. backfill 때문에 "앞 작업 때문에 대기"하는 경우는 없다.
- `rejected`의 `reason`은 `Invalid`(검증 실패)나 `QueueFull`(`max_queue` 초과)이다.
- `gpus`는 실제로 배정된 수다. 배치 전에는 `null`이다.

### 5.4 시간 제한

- **함정:** `activeDeadlineSeconds`는 Job이 생성된 시점부터 센다. 그래서 Pending으로 기다리는 시간도 `--time`에서 깎인다.
- **실행 시간:** gqd가 main 컨테이너의 `startedAt`부터 직접 재고, 넘으면 Job을 삭제한 뒤 `timeout`으로 기록한다.
- **대기 시간:** `wait_timeout`(기본 6h, 최대 24h)은 제출부터 시작까지, 즉 `queued`와 `pending`을 합친 시간이다. 넘으면 Job이 있으면 삭제하고 `expired`로 기록한다.
- **백스톱:** `activeDeadlineSeconds = time + placement_grace + deadline_slack`(기본 30분, 이미지 pull 여유)으로 둔다. Job은 맞는 노드가 있을 때만 만들어지므로 Pending은 짧다. gqd가 죽어 있어도 작업이 무한히 돌지 않는다.

### 5.5 실행 환경 전달

에이전트가 기대하는 것은 "내 셸에서 돌리던 그대로" 실행되는 것이다. 이미지와 경로가 같으므로 env만 넘기면 된다.

- gq가 호출 시점의 환경변수를 `run.sh`에 `export K=<shlex.quote(V)>` 형태로 기록한다.
  - **제외(prefix):** `KUBERNETES_`, `NVIDIA_`, `CUDA_VISIBLE_DEVICES`, `HOSTNAME`, `PWD`, `OLDPWD`, `SHLVL`, `TERM`, `TMUX`, `SSH_`, `JPY_`, `JUPYTER_`, `NB_PREFIX`, `KF_`, `_`, 그리고 `*_SOCK`, `*_SOCKET`.
  - `--env K=V`로 덮어쓰거나 추가할 수 있다. `--clean-env`를 주면 아무것도 넘기지 않는다(로그인 셸 기본 환경).
- `run.sh` 형태:

```bash
#!/bin/bash
export PATH='/home/irteam/.conda/envs/sliderquant/bin:...'
export CONDA_PREFIX='/home/irteam/.conda/envs/sliderquant'
# ... (캡처한 env)
export OMP_NUM_THREADS=<cpu / max(gpus,1)>   # 명시하지 않았을 때만
cd '/home/irteam/SliderQuant'
exec python eval.py --model llama3-8b
```

- Job 컨테이너의 command는 고정이다(5.9). `bash -lc`로 로그인 셸을 띄우고 `run.sh`를 실행하므로, `.profile`의 기본 환경 위에 캡처한 env가 덮인다.
- 멀티 GPU 작업은 사용자가 직접 `--gpus 4 -- torchrun --nproc_per_node 4 train.py`처럼 쓴다. 노드는 하나만 쓴다.

### 5.6 gqd 루프 (2초 주기)

```
1. gqd.heartbeat 갱신
2. k8s 조회: 네임스페이스의 Pod 전체와 label gq=1인 Job을 1회씩 list (metrics는 30초마다)
3. 자원 계산: 노드별 여유 → capacity.json 기록 (5.13)
4. kill 요청 처리
5. 시간 집행: running이 time 초과 → 삭제 → timeout / queued·pending이 wait_timeout 초과 → expired
   / pending이 placement_grace 초과 → Job 삭제 → queued로 되돌림 (5.14)
6. 상태 판정: (Job, Pod, exit 파일)로 각 작업의 state 계산 → 바뀐 것만 status.json 기록
7. 제출 처리: spool/*.submit을 ID 순으로 배치 시도 (상한·노드 맞춤, backfill 허용). 배치한 것은 Job 생성,
   나머지는 queued로 순번·사유 갱신. 같은 루프에서 배치한 만큼 자원 추정치에서 바로 뺀다
8. (1시간마다) 보존 기간이 지난 종료 작업 디렉터리 삭제
```

- **순수 함수와 I/O를 분리한다.** `validate(request, config)`, `capacity(pods, config)`, `place(requests, capacity, config)`, `derive_state(job, pod, exit_file, now)`는 순수 함수로 두고 테스트한다(10장).
- **파일 쓰기:** 임시 파일에 쓴 뒤 rename한다. symlink는 따라가지 않는다(`O_NOFOLLOW`, `lstat`으로 확인). 에이전트가 `status.json`을 symlink로 바꿔 두는 공격을 막기 위해서다.
- **Job 소멸 후 판정:** `ttlSecondsAfterFinished: 3600`이라 gqd가 1시간 넘게 죽어 있으면 Job이 먼저 사라질 수 있다. 그때는 `exit` 파일이 있으면 그것으로 판정하고, 없으면 `lost`로 둔다.

### 5.7 로컬 백엔드 (`--local`)

nogpu2(CPU 64코어)에서 바로 실행하는 CPU 작업이다. gqd를 거치지 않는다.

- gq가 자기 자신을 `gq _supervise <id> <time>`으로 분리 실행한다(새 세션). supervisor는 `run.sh`를 별도 프로세스 그룹으로 띄우고, `--time`이 지나면 그룹 전체에 SIGTERM을 보내고 10초 뒤 SIGKILL을 보낸다. 끝나면 `exit` 파일과 최종 `status.json`(`succeeded`/`failed`/`timeout`)을 쓴다. `timeout` 명령에 의존하지 않는다.
- `status.json`에는 `{backend: "local", host, pid, pid_start, supervisor, supervisor_start, ...}`가 들어간다.
- 데몬이 없으므로, 누군가 상태를 읽을 때 죽은 작업을 판정한다.
  - `exit` 파일이 있으면 supervisor가 기록한 최종 상태를 쓴다.
  - 없으면 작업 프로세스나 supervisor 중 하나라도 살아 있는지 본다. `/proc/<pid>/stat`의 starttime까지 비교해 PID 재사용을 걸러낸다.
  - 둘 다 죽었으면 `lost`다(예: nogpu2 재시작).
- kill: 같은 host일 때만 `kill -- -<pgid>`로 프로세스 그룹 전체를 죽인다.
- nogpu2 CPU에 대한 상한은 두지 않는다. 필요해지면 `taskset`이나 동시 실행 수 제한을 추가한다.

### 5.8 CLI 명세

```
gq submit   [옵션] -- <cmd...>     요청을 넣고 id를 출력한 뒤 바로 반환
gq run      [옵션] -- <cmd...>     submit + logs -f + wait
gq status   <id> [--json]          id는 task 또는 그룹 (그룹이면 task별 요약)
gq wait     <id> [--timeout 30m]   그룹이면 모든 task가 끝날 때까지
gq logs     <id> [-f] [--tail N]   그룹이면 task 접두어를 붙여 합쳐서 출력
gq ps       [--json] [--all] [--mine]
gq kill     <id>                   그룹이면 모든 task
gq capacity [--json] [--fit N]     노드별 남은 자원 (5.13)

옵션:
  --gpus N | LO-HI    기본 0 (CPU 작업). 범위를 주면 지금 한 노드에서 줄 수 있는 만큼 받는다 (5.14)
  --array N           같은 명령을 N개 task로 (5.14)
  --time 4h           필수. 상한 48h
  --cpu C --mem M     limit. 기본: 실제 GPU 1장당 노드 용량/8 (CPU 15, 245Gi). CPU 작업은 8 / 64Gi.
                      request는 항상 작은 고정값(1코어, 4Gi; limit보다 크지 않게)
  --wait-timeout 6h   제출부터 시작까지. 상한 24h
  --name X            사람이 읽을 짧은 이름 (Job annotation에 들어가므로 비밀을 넣지 않는다)
  --local             nogpu2에서 직접 실행 (--gpus와 함께 쓸 수 없다)
  --env K=V, --clean-env
  --cwd DIR           기본은 현재 디렉터리. /home/irteam 아래여야 한다
```

- **owner:** `$GQ_OWNER`가 있으면 그 값을, 없으면 `$USER:<부모 프로세스 pid>`를 쓴다. 스킬에서 에이전트 세션마다 `GQ_OWNER`를 설정하도록 안내한다. `--mine`은 이 값으로 거른다.
- **heartbeat 경고:** `gqd.heartbeat`가 30초 넘게 갱신되지 않았으면, 모든 명령이 stderr로 "gqd 응답 없음" 경고를 낸다. `submitted`에 머무는 원인을 바로 알 수 있게 하기 위해서다.
- **`gq ps` 헤더:** 사용 중인 GPU/상한, 대기열 길이, PVC 남은 용량(`statvfs`)을 함께 보여준다.
- **exit code:**

| code | 의미 |
|---|---|
| 0 | 명령 성공 / `run`·`wait`에서는 작업이 exit 0으로 끝남 |
| 1–123 | `run`·`wait`: 작업의 exit code를 그대로 |
| 2 | 사용법 오류 |
| 124 | `wait --timeout`이 끝나도록 작업이 끝나지 않음 (작업은 계속 돈다) |
| 125 | 작업이 정상 종료되지 못함 (`rejected`/`expired`/`timeout`/`lost`/`killed`). 사유는 stderr에 JSON으로 출력 |

- 그룹에 대한 `wait`: 모든 task가 exit 0이면 0, 하나라도 실패하면 가장 먼저 실패한 task의 코드를 돌려준다. task별 결과는 `gq status <그룹> --json`으로 본다.
- `gq capacity --fit N`: 지금 GPU N장이 비어 있는 노드가 있으면 0, 없으면 1을 돌려준다.

### 5.9 Job 템플릿 (gqd가 생성)

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: gq-<instance>-<id>-a<attempt>
  labels: {gq: "1", gq-instance: "<instance>", gq-id: "<id>", gq-group: "<group>"}   # gq-group은 배열일 때만
  annotations: {gq/name: "<name>", gq/owner: "<owner>", gq/node: "<node>", gq/gpus: "<n>",
                gq/cpu: "<cores>", gq/mem: "<bytes>", gq/attempt: "<n>"}   # 아직 bind 안 된 Job의 자원 예약에 쓴다
spec:
  backoffLimit: 0
  activeDeadlineSeconds: <time + placement_grace + deadline_slack>   # 백스톱. 실제 집행은 gqd (5.4)
  ttlSecondsAfterFinished: 3600
  template:
    metadata:
      labels: {gq: "1", gq-id: "<id>", h200-pod-default: "true", private-h200-acme-0-pod-default: "true"}
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      enableServiceLinks: false
      affinity:                                       # gqd가 고른 노드에 고정 (5.14)
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchFields: [{key: metadata.name, operator: In, values: ["<node>"]}]   # 라벨이 아닌 노드 이름 자체
      containers:
      - name: main
        image: <config.images[0]>
        env:
        - {name: GQ_ID, value: "<id>"}
        - {name: GQ_GPUS, value: "<실제 배정된 GPU 수>"}
        - {name: GQ_CPU, value: "<CPU 코어 수>"}      # run.sh가 OMP_NUM_THREADS 기본값 계산에 쓴다
        - {name: GQ_TASK_INDEX, value: "<k>"}         # 배열 작업일 때만
        - {name: GQ_TASK_COUNT, value: "<N>"}         # 배열 작업일 때만
        command: ["bash", "-lc", "d=<root>/jobs/$GQ_ID; bash $d/run.sh >$d/log 2>&1; rc=$?; echo $rc >$d/exit; exit $rc"]
        resources:
          requests: {cpu: "<min(1, cpu)>", memory: "<min(4Gi, mem)>"}   # config requests. 노트북처럼 작게
          limits: {nvidia.com/gpu: "<gpus>", cpu: "<cpu>", memory: "<mem>"}
        volumeMounts:                                  # 설정의 volumes 전체를 노트북과 같은 경로에
        - {name: v0, mountPath: /home/irteam}          # (+ subPath / readOnly가 있으면 그대로)
        - {name: dshm, mountPath: /dev/shm}
      volumes:
      - {name: v0, persistentVolumeClaim: {claimName: alice-ws}}
      - {name: dshm, emptyDir: {medium: Memory}}
```

Job spec에 들어가는 것은 ID, 이름, owner, 자원, 노드, 배열 index뿐이다. 명령과 사용자 env는 들어가지 않는다.

**request는 작게, limit은 크게.** 이 클러스터의 노트북과 같은 방식이다(request 0.5코어/1Gi, limit은 크게).
- 스케줄러는 request만 보므로, 배치는 사실상 GPU로만 갈린다. 일부 노트북이 CPU request를 크게 잡아서 노드의 request 여유가 적어도(예: node-c 22.4코어) GPU 자리만 있으면 들어간다.
- 노드가 한가하면 limit까지 CPU와 메모리를 더 쓸 수 있다(실제 사용량은 대개 낮다. 예: node-c는 약 8코어).
- 대가: CPU가 붐비면 request 비율만큼만 몫을 보장받는다. 노드 메모리가 부족하면 request를 넘겨 쓰는 pod부터 축출될 수 있다. 같은 노드의 노트북들도 같은 조건이다.

### 5.10 설정 (`config.json`, ConfigMap)

`gqctl init`이 노트북에서 읽어 만든다. 전체 예시는 `examples/config.mlxp.json`에 있다. 사용자별 키는 다음과 같다.

```json
{
  "instance": "alice",
  "namespace": "team-ml",
  "image": "example/mlxp-notebook:cu128",
  "service_account": "default-editor",
  "volumes": [{"pvc": "alice-ws", "mountPath": "/home/irteam"}],
  "root": "/home/irteam/gq",
  "pod_labels": {"h200-pod-default": "true", "private-h200-acme-0-pod-default": "true"},
  "zone": "private-h200-acme-0",
  "nodes": ["h200-node-a", "h200-node-d", "h200-node-c", "h200-node-b"],
  "node_capacity": {"gpu": 8, "cpu": 125, "mem": "1964Gi"},
  "placement_grace": 60,
  "max_gpus_total": 4,
  "max_running": 8,
  "max_queue": 32,
  "max_array": 64,
  "max_time": 172800,
  "max_wait_timeout": 86400,
  "max_gpus_per_job": 8,
  "job_ttl_after_finished": 3600,
  "retention_days": 30,
  "loop_interval": 2
}
```

`volumes`는 노트북의 PVC 마운트 전체(subPath·readOnly 포함)이고, Job pod도 똑같이 마운트한다. `root`는 그중 쓰기 가능한 볼륨 위에 있어야 한다. 선택 키 `gqd_image`(이미지에 `kubernetes` 패키지가 없을 때)도 있다. 실제 파일에는 다음 키도 있다: `cpu_job_default`(CPU 작업 limit 기본 8코어/64Gi), `requests`(모든 작업의 request, 1코어/4Gi), `deadline_slack`(1800), `metrics_interval`(30), `gc_interval`(3600).

설정은 ConfigMap에 둔다. PVC에 두면 에이전트가 상한을 고칠 수 있기 때문이다.

- `nodes`와 `node_capacity`는 **보정값**이다. SA로는 노드와 Zone을 읽을 수 없어서 config에 직접 적는다.
  - 노드 목록이 바뀌면 Mac에서 `kubectl get zones.management.mlx.navercorp.com private-h200-acme-0 -o jsonpath='{.spec.nodes}'`로 확인하고 갱신한다.
  - `cpu: 125`, `mem: 1964Gi`는 MLXP 노트북 설정의 최대값이다. 이 값으로 꽉 채워 만들어도 항상 생성되었다는 사용자 경험에 따라 노드 한도로 쓴다.
- 작업 하나의 CPU/메모리 상한은 `node_capacity`가 겸한다.
- `max_gpus_total`은 배열 task를 모두 합한 값에 적용된다. 넘는 task는 `queued`(`Cap`)로 기다린다.

### 5.11 보존과 정리

- **k8s Job:** 끝나고 1시간 뒤 ttl로 사라진다. 상태는 그 전에 `status.json`에 확정된다.
- **`jobs/<id>/`:** 종료 후 `retention_days`(30일)가 지나면 gqd가 삭제한다. 삭제 대상은 ID 정규식에 맞는 디렉터리뿐이고, symlink는 따라가지 않는다.
- 로그를 오래 남겨야 하는 작업이라면, 사용자 코드가 결과를 자기 프로젝트 디렉터리에 쓰도록 한다.

### 5.12 장애 시나리오

| 상황 | 영향 | 대응 |
|---|---|---|
| gqd 다운 | 새 요청이 `submitted`에 머문다. 실행 중인 작업은 계속 돈다 | Deployment가 재시작한다. gq가 heartbeat 경고를 낸다. 시간 상한은 `activeDeadlineSeconds` 백스톱이 맡는다 |
| gqd 두 개가 동시에 실행 | 중복 처리 | Deployment `strategy: Recreate`, replicas 1. Job 이름이 고정이라 `AlreadyExists`로 멱등성이 보장된다 |
| nogpu2 재시작 | 로컬 작업은 `lost`. pod 작업은 영향 없다 | 로컬 작업이 오래 걸리면 pod 백엔드를 쓴다 |
| GPU 부족 / 단편화 | `queued`(`NoFit`)가 길어진다 | Job을 만들지 않고 기다린다. `wait_timeout`이 지나면 `expired`. 에이전트는 `gq capacity`를 보고 쪼갤지 정한다 |
| 자원 추정이 틀림 (사람이 노트북을 켬, 보이지 않는 시스템 pod) | Job이 Pending | `placement_grace`(60초) 후 Job을 삭제하고 `queued`로 되돌려 다시 배치한다 |
| OOM / 축출 / 노드 장애 | `failed`(OOMKilled) / `lost` | 사유를 기록한다. 재시도는 에이전트가 판단한다 (자동 재시도 없음) |
| PVC 가득 참 | log/exit 쓰기 실패 | `gq ps` 헤더에 남은 용량 표시 |
| 공유 파일시스템 지연 | 상태 반영이 수 초 늦어질 수 있다 | 허용 범위다 |

### 5.13 자원 파악 (`capacity.json`)

| 정보 | 출처 | 비고 |
|---|---|---|
| 노드 목록 | config `nodes` | SA는 Zone 객체를 읽을 수 없다 |
| 노드 용량 | config `node_capacity` | 노드를 조회할 수 없어서 보정값으로 둔다 |
| 사용량 | `team-ml` pod **전체** 목록 (gq 작업만이 아니다) | `nodeName`이 config 노드이고 `Succeeded`/`Failed`가 아닌 pod의 requests 합계. GPU는 limits를 쓴다 |
| 실제 CPU 사용량 | `metrics.k8s.io` (pod 단위를 노드별로 합산) | 참고용이다. 스케줄러는 requests만 본다. 노트북마다 request는 0.5부터 64까지 제각각이고, 실제 사용량과는 관계가 없다 |
| GPU 대기 수요 | `team-ml`에서 `nodeName`이 없는 GPU pod | 다른 사람이 기다리고 있는 GPU 수 |

- 노드별 여유 = `node_capacity` − 사용량. gqd가 매 루프마다 `~/gq/capacity.json`에 기록한다.

```json
{"updated": "...", "estimated": true,
 "nodes": [{"name": "h200-node-a", "gpu_free": 2, "gpu_used": 6, "cpu_req_free": 112.5,
            "mem_req_free": "1700Gi", "cpu_actual": 12.3}, ...],
 "total_gpu_free": 4, "max_gpu_free_on_node": 2,
 "gpu_waiting": 0, "gq_gpus_in_use": 0, "gq_gpus_cap": 4}
```

- `gq capacity` 출력 (2026-11-08 실제 사용량 기준, GPU 열만):

```
NODE             GPU free   CPU req free   MEM req free   CPU actual
h200-node-a   2/8        …              …              …
h200-node-b   1/8        …              …              …
h200-node-c   1/8        …              …              …
h200-node-d   0/8        …              …              …
total free 4 · max on one node 2 · team-ml GPU waiting 0 · gq 0/4
```

- **정확도 한계:**
  - 다른 네임스페이스나 시스템 pod는 보이지 않는다. 예를 들어 MLXP의 GPU 검증 작업 같은 것이 GPU를 쓰면 여유를 실제보다 많게 잡는다.
  - 사람이 노트북을 켜는 것과 2초 단위로 경쟁한다.
  - 그래서 이 값은 추정치이고, 최종 판단은 스케줄러가 한다. 추정이 틀려서 배치에 실패하면 5.14의 재배치가 흡수한다.

### 5.14 배치와 분할

**원칙:** Job pod 하나는 노드 하나에만 올라간다. GPU N장짜리 작업은 한 노드에 N장이 비어 있어야 한다.

**(a) gqd가 노드를 고른다 (best-fit 패킹)**

- k8s 기본 스케줄러는 덜 쓴 노드로 작업을 퍼뜨리는 경향이 있다. 그러면 1장짜리 작업이 node-a에 남은 2장 자리를 깨뜨려 단편화가 심해진다.
- 노드 선택 규칙:
  - **GPU 작업:** `gpu_free ≥ N`인 노드 중 `gpu_free`가 가장 작은 노드를 고른다. 동률이면 CPU 여유가 많은 쪽이다. 단, 그 노드의 CPU/메모리 **request** 여유가 작업의 request(기본 1코어/4Gi)를 감당해야 한다.
  - **가변 GPU 작업(`--gpus LO-HI`):** HI부터 LO까지 줄여 가며 처음 맞는 수를 고르고, 그 수 안에서 best-fit으로 노드를 고른다.
  - **CPU 작업:** CPU/메모리가 맞는 노드 중 `gpu_free`가 가장 작은 노드를 고른다. GPU가 남은 노드의 CPU는 GPU 작업을 위해 남겨 둔다.
- 고른 노드는 required nodeAffinity의 `matchFields`(`metadata.name`)로 고정한다. hostname 라벨이 노드 이름과 다를 수 있어서 이름 자체로 지정한다. `nodeName`을 쓰지 않는 이유는 스케줄러가 자원 검사를 하게 두기 위해서다. 실제 클러스터에서 dry-run으로 허용되는 것을 확인했다.
- 맞는 노드가 없으면 Job을 만들지 않고 `queued`(`NoFit`)로 둔다. 사유에 필요한 수, 한 노드 최대 여유, 전체 여유를 적는다. Pending pod를 쌓아 두지 않는다.
- 경쟁 때문에 Pending(`Unschedulable`)이 `placement_grace`(60초)를 넘으면 Job을 삭제하고, 같은 루프에서 새 시도 번호로 다시 배치한다. 이번 루프의 자원 계산에는 삭제한 Job의 예약이 아직 남아 있으므로, 실패한 노드는 자연히 피하게 된다.
- **대기열:** FIFO에 backfill을 허용한다. 뒤에 있는 작업이라도 자리가 맞으면 먼저 시작한다.
  - `ponytail:` 자리 예약이 없어서 큰 작업이 계속 밀릴 수 있다. 실제로 그런 일이 생기면 "앞 작업이 30분 넘게 기다리면, 그 작업이 들어가기에 가장 가까운 노드에는 backfill을 금지한다"는 규칙을 추가한다.

**(b) 분할 수단 (어떻게 쪼갤지는 에이전트가 정한다)**

1. **배열 작업 `--array N`:** 같은 명령을 N개 task로 나눈다.
   - 각 task는 독립된 Job이고 ID는 `<id>-<k>`이다. env로 `GQ_TASK_INDEX`(0부터)와 `GQ_TASK_COUNT`를 받는다.
   - 자리가 나는 대로 각자 배치된다. 그룹 ID `<id>`로 `status`/`wait`/`kill`/`logs`를 한꺼번에 다룬다.
   - 예: 남은 GPU가 2+1+1장일 때 4장짜리 평가 하나 대신 1장짜리 shard 4개로 돌린다.
     ```bash
     gq submit --array 4 --gpus 1 --time 2h -- bash -c 'python eval.py --shard $GQ_TASK_INDEX --num-shards $GQ_TASK_COUNT'
     ```
     (`$GQ_*`는 제출하는 셸이 아니라 task 안에서 확장되어야 하므로 `bash -c '...'`로 감싼다.)
2. **가변 GPU 수 `--gpus LO-HI`:** 지금 한 노드에서 줄 수 있는 만큼 받는다(최대 HI, 최소 LO). 실제 수는 `GQ_GPUS`로 전달된다.
   ```bash
   gq submit --gpus 1-4 --time 8h -- bash -c 'torchrun --nproc_per_node $GQ_GPUS train.py'
   ```
   데이터 병렬 학습처럼 GPU 수에 맞춰 돌 수 있는 작업에 쓴다.
3. **판단 정보 `gq capacity [--json] [--fit N]`:** 쪼갤지 기다릴지 정할 때 본다. 판단 규칙은 6장에 있다.

**(c) v1에서 하지 않는 것:** 한 프로그램을 여러 노드에 걸쳐 돌리는 것(예: 4장을 2+1+1로 묶는 멀티노드 분산). 노드 간 NCCL 통신, istio sidecar 우회, 패브릭(`FabricClusterClaim`) 설정이 필요하다. v2 이후에 PyTorchJob이나 JobSet으로 검토한다.

## 6. 에이전트 사용 규칙 (`~/.claude/skills/gq/SKILL.md`)

홈이 공유 PVC라서 모든 노트북의 에이전트가 같은 스킬을 본다. 들어갈 내용:

- 세션을 시작할 때 `export GQ_OWNER=claude:<세션 식별자>`를 설정한다.
- 1분 이내의 짧은 GPU 확인은 `gq run`, 긴 작업은 `gq submit` 후 `gq wait --timeout`. sleep 루프로 폴링하지 않는다.
- `--time`은 반드시 넣고, 예상 시간의 1.5배 정도로 잡는다.
- exit code 125이면 stderr의 사유를 읽는다. `expired`(GPU 부족)이면 GPU 수를 줄이거나 나중에 다시 시도한다. 같은 요청을 연달아 넣지 않는다.
- **GPU 작업을 넣기 전에 `gq capacity`를 본다.** 쪼갤지는 다음 순서로 정한다.
  1. 독립적으로 나눌 수 있는 작업(평가 shard, 시드, 하이퍼파라미터, 모델별 실행)은 `--array`에 `--gpus 1`(또는 2)로 쪼갠다.
  2. 한 프로그램이 N장을 꼭 써야 하면 `gq capacity --fit N`을 확인한다.
  3. 맞는 노드가 없는데 GPU 수에 맞춰 돌 수 있는 작업이면 `--gpus LO-HI`를 쓴다.
  4. 그것도 안 되면 제출하고 기다린다. 이때 `--wait-timeout`을 정해 둔다.
- CPU 작업은 nogpu2에 여유가 있으면 `--local`로, 크면 pod로 보낸다.
- `queued` 사유가 `NoFit`이면 기다리는 중이다. `Cap`이면 gq 상한에 걸린 것이므로 자기 작업이 끝나기를 기다린다.
- `gq ps --mine`으로 자기 작업만 보고, 자기 작업만 kill한다.
- `--name`에 비밀을 넣지 않는다.
- kubectl은 쓰지 않는다 (토큰이 없어서 어차피 실패한다).

Claude Code 설정 (편의와 사고 방지용이며 보안 경계가 아니다): `Bash(gq *)` 허용, `Bash(kubectl *)` deny.
경계가 pod 수준에 있으므로 bypass나 auto 모드로 자율 실행해도 된다.

## 7. 보안 모델

### 7.1 무엇을 막으려는가

1. **사고:** 에이전트가 실수로 `kubectl delete`를 실행하거나 정리를 잘못하는 경우.
2. **프롬프트 인젝션:** 논문, 웹페이지, 로그 내용에 끌려간 에이전트가 의도적으로 우회하려는 경우.
3. **공유 PVC 데이터 손상:** 토큰과 무관한 별도 문제 (7.4).

### 7.2 지켜야 할 규칙

1. `alice-ws`를 마운트하는 pod 중 k8s 토큰을 가진 것은 **gqd 하나뿐**이다.
   - 모든 `alice-*` 노트북과 Job pod는 `automountServiceAccountToken: false`로 설정한다.
   - 이 규칙만 지키면 에이전트가 `~/.bashrc` 등 PVC 파일에 무엇을 심어 두든, 그 코드는 토큰이 없는 pod에서만 실행된다.
2. gqd는 **PVC에 있는 코드를 실행하거나 import하지 않는다.** `request.json`을 파싱하고 상태 파일을 쓰기만 한다. gqd의 코드와 설정은 ConfigMap에 있고, 배포는 Mac에서만 한다.
3. 사람의 kubectl 작업은 로컬 머신(Mac)의 kubeconfig로 한다.

토큰이 없는 pod에서는 sudo로 root가 되어도 얻는 게 없다 (CapEff 0, unprivileged pod).

**gqd 검증 규칙** (실제 보안 경계):

- `request.json`을 엄격하게 파싱한다. 크기 제한 64KB, 모르는 필드가 있으면 거부.
- ID 정규식, `cwd`가 `/home/irteam/` 아래인지, 숫자 범위와 상한(5.10)을 검사한다.
- Job spec은 고정 템플릿(5.9)으로만 만든다. 요청으로 바꿀 수 있는 값은 자원, 시간, 이름뿐이다.
- 파일을 다룰 때 symlink를 따라가지 않는다.
- kill과 삭제는 `gq=1` 라벨이 붙은 Job만 대상으로 한다.

### 7.3 쓰지 않은 방법과 이유 (모두 확인함)

| 방법 | 결과 |
|---|---|
| Claude Code 권한 규칙 | Bash deny는 `sh -c`나 python을 거치면 적용되지 않는다. Read deny는 subprocess의 `open()`을 막지 못한다. 사고 방지용일 뿐이다 |
| Claude Code 샌드박스 (bubblewrap) | pod의 AppArmor(`cri-containerd.apparmor.d`)가 mount namespace 생성을 막는다 (`Failed to make / slave: Permission denied`). **동작하지 않는다** |
| 권한을 좁힌 SA | SA는 만들 수 있지만 Role/RoleBinding을 만들 권한이 없다 |
| NetworkPolicy로 API 서버 차단 | 생성 권한이 없다 |
| 토큰 파일 숨기기 | 읽기 전용 tmpfs, 0644. chmod가 안 되고 다른 uid로도 읽힌다 |
| dotfile 읽기 전용 마운트 | 7.2의 규칙 1을 지키면 권한 측면에서 필요 없다. 데이터 손상도 막지 못한다 |

### 7.4 남는 위험

- **데이터 손상:** 에이전트는 PVC 전체를 쓸 수 있다(`rm -rf` 등). 프로젝트는 git push를 자주 한다. 스토리지 스냅샷 지원 여부는 확인하지 못했다.
- **PVC 안의 자격 증명:** `~/.ssh` 키, HF·wandb 토큰 등은 에이전트가 읽을 수 있다. 범위가 좁은 토큰을 쓴다.
- **GPU 낭비:** 인젝션된 에이전트는 상한(5.10) 안에서 GPU를 소모할 수 있다.
- **KFP 토큰:** 노트북 pod에 남는다면 간접 경로가 된다 (12장).

## 8. 저장소 구조와 배포

```
mlxp-gq/
  DESIGN.md, README.md
  gq                 # CLI (단일 파일, 표준 라이브러리)
  gqd.py             # 데몬 (단일 파일)
  gqctl              # 설치·운영 도구 (사용자 PC에서 실행)
  test_gqd.py        # 테스트
  skill/SKILL.md     # 6장
  examples/config.mlxp.json
```

매니페스트는 파일로 두지 않고 `gqctl`이 설정에서 렌더링한다(`gqctl render`로 볼 수 있다).

**gqd Deployment 요점** (`gqctl up`이 만든다):

```yaml
kind: Deployment
metadata: {name: gqd-<instance>, labels: {app: gqd-<instance>, gq-instance: <instance>}}
spec:
  replicas: 1
  strategy: {type: Recreate}                     # 한 인스턴스에 gqd는 하나만
  template:
    metadata:
      labels: {app: gqd-<instance>, gq-instance: <instance>, <pod_labels...>}
    spec:
      serviceAccountName: <service_account>       # 토큰이 있는 유일한 pod
      containers:
      - name: gqd
        image: <gqd_image 또는 image>
        command: ["python3", "-I", "/opt/gqd/gqd.py", "--config", "/opt/gqd/config.json", "--root", "/gq"]
        resources: {requests: {cpu: 100m, memory: 256Mi}, limits: {cpu: "1", memory: 1Gi}}
        volumeMounts:
        - {name: root, mountPath: /gq, subPath: <root의 볼륨 내 경로>}   # PVC 중 <root>만 보인다
        - {name: code, mountPath: /opt/gqd, readOnly: true}
      volumes:
      - {name: root, persistentVolumeClaim: {claimName: <root가 있는 PVC>}}
      - {name: code, configMap: {name: gqd-<instance>}}
```

**배포와 설치:** 모두 사용자 PC에서 `gqctl`로 한다(README.md 설치 절).
- `gqctl up`: ConfigMap(gqd.py + 설정)을 apply하고, Deployment를 apply한 뒤 rollout restart한다.
- `gqctl install`: 실행 중인 노트북으로 `kubectl cp`해서 `gq`와 스킬을 PVC에 둔다. 에이전트가 이 파일들을 고쳐도 권한 문제는 없다.
- `gqctl install`은 `<root>/env.sh`도 쓰고, 노트북의 셸 rc에 그것을 읽는 한 줄을 표식과 함께 넣는다(중복 없음, symlink인 rc는 건드리지 않음). Claude Code 설정에는 `gq` 허용과 `kubectl` 금지를 합친다(처음 한 번 백업).
- `gqctl lockdown`은 기본적으로 설정의 공유 볼륨을 마운트하는 노트북을 **전부** 고른다. 7.2의 규칙("그 볼륨을 마운트하는 pod 중 토큰을 가진 것은 gqd뿐")을 그대로 옮긴 것이다. 실행 중이던 노트북은 재시작을 기다린 뒤 토큰이 사라졌는지 확인한다.
- `gqctl setup`은 init(설정이 없을 때) → probe → install → up → end-to-end 확인을 한 번에 한다. 마지막 확인은 노트북 안에서 실제로 `gq run`(CPU, 그리고 `--gpu`면 GPU 1장)을 돌려 gqd까지 전 경로를 검증한다. 노트북을 재시작시키는 lockdown은 `--lockdown`을 줄 때만 한다.
- `gqctl unlock`은 lockdown을 되돌린다. 값을 `true`로 쓰지 않고 필드를 지워서(merge patch `null`), lockdown 전의 플랫폼 기본값으로 돌아가게 한다. 기본 대상은 공유 볼륨을 마운트하는 노트북 중 값이 `false`인 것이다.

## 9. 사람이 쓰는 법 (요약)

```bash
gq run --gpus 1 --time 10m -- python -c "import torch; print(torch.cuda.get_device_name())"
```

```bash
gq submit --gpus 2 --time 6h --name sq-eval -- python eval.py --model llama3-8b
```

```bash
gq capacity
```

```bash
gq submit --array 4 --gpus 1 --time 2h -- bash -c 'python eval.py --shard $GQ_TASK_INDEX --num-shards $GQ_TASK_COUNT'
```

```bash
gq ps
```

GPU 노트북을 직접 켜서 쓰는 기존 방식은 그대로 쓸 수 있다. gq와 서로 간섭하지 않는다.

## 10. 테스트

- **`test_gqd.py`** (assert 기반, 외부 프레임워크 없음):
  - `validate`: 상한 초과, 잘못된 ID·cwd, 모르는 필드, 큰 파일을 거부하는지.
  - `derive_state`: (Job, Pod, exit) 조합으로 succeeded/failed/OOMKilled/lost/timeout/expired를 맞게 판정하는지.
  - `capacity`: Succeeded/Failed pod와 config에 없는 노드의 pod를 빼고 합산하는지.
  - `place`: 다음을 확인한다.
    - 1+2+1+0 여유에서 4장 요청이 `NoFit`인지.
    - 1장 요청이 1장 남은 노드로 가는지(best-fit).
    - `1-4` 범위 요청이 2장을 받는지.
    - backfill이 되는지.
    - 한 루프에서 여러 작업을 배치할 때 자원을 중복으로 쓰지 않는지.
    - `max_gpus_total`이 배열 task 전체에 적용되는지.
- **개발 중 실행:** 토큰을 끄기 전에는 nogpu2에서 `gqd.py --root ~/gq`를 직접 돌려 볼 수 있다.
- **배포 후 체크리스트:** 11장의 미확인 항목과 12장의 구현 순서 5번.

## 11. 확인 상태

| 항목 | 상태 |
|---|---|
| zone 라벨 붙은 Job/Pod의 웹훅 통과 | ✅ server dry-run |
| PodDefault 라벨로 zone·H200·priority 주입 | ✅ dry-run |
| istio native sidecar | ✅ dry-run (실제로 Job이 완료되는지는 미확인) |
| 일반 Pod에서 `automountServiceAccountToken: false` 유지 | ✅ dry-run |
| `bash -lc`에서 conda base가 잡힘, env는 PVC에 있음 | ✅ nogpu2에서 확인 |
| 실제 Job 실행, 완료, 시작 시간 | ❌ README 1단계 probe로 확인 |
| gqd Deployment, ConfigMap, gqd pod가 웹훅을 통과하는지 | ✅ server dry-run (gqd pod는 `default-editor` 토큰과 private zone을 받는다) |
| gqd `--dry-run --once`를 실제 클러스터에 실행 | ✅ 실제 pod로 자원을 계산했고, best-fit 배치와 GPU 범위(1-8 → 2), CPU 작업 배치까지 API 서버가 Job을 수락했다 (dry-run) |
| 노드 CPU **requests** 여유 | ⚠️ 일부 노트북이 request를 크게 잡는다 (예: node-c는 requests 합이 102.6이라 125 기준 22.4 남음. 실제 사용량은 약 8.4코어). 그래서 gq Job은 request를 작게, limit을 크게 잡는다(5.9). 스케줄러가 보는 request만 검사한다 |
| `/home` → `/home1` symlink | ✅ 이미지에서 `/home`이 `/home1`을 가리킨다. gq가 realpath로 비교한 뒤 `/home/irteam/...` 경로로 바꿔 넘긴다 |
| SA로 `team-ml` pod 목록과 pod metrics 조회 | ✅ nogpu2에서 확인 |
| SA로 Zone 조회 | ✅ 불가 확인 → 노드 목록은 config로 |
| nodeAffinity로 노드 고정 | ✅ dry-run (웹훅이 zone nodeSelector와 함께 유지) |
| `team-ml` pod 합산이 실제 노드 사용량과 맞는지 | ❌ MLXP UI의 노드별 GPU 사용량과 비교 필요 |
| 노드 CPU/메모리 한도 | ✅ 125코어 / 1964Gi (MLXP 노트북 최대값. 꽉 채워 만들어도 생성된다는 사용자 확인) |
| Notebook pod에서 토큰이 실제로 사라지는지 | ❌ 재시작 후 확인 필요 |
| KFP 토큰(`/var/run/secrets/kubeflow/pipelines`)이 남는지 | ❌ 웹훅이 넣고 있어 남을 가능성이 있다 |

## 12. 열린 질문

1. **MLXP 정책:** 노트북이 아닌 Job과 상시 Deployment(gqd)를 써도 되는가? MLXP UI에 보이는가, 사용량 집계에 문제가 없는가? (관리자에게 문의)
2. **KFP 토큰:** Notebook pod에 자동으로 들어가는 KFP 토큰을 끌 수 있는가? 끌 수 없다면 다음 중 하나를 고른다.
   - 위험을 받아들인다. 실수로는 일어날 수 없고 의도적으로 pipeline을 짜야 하는 경로다.
   - 에이전트를 Notebook이 아닌 별도 pod로 옮긴다. MLXP UI에서 관리할 수 없게 된다.
3. **자원 정보:** 이 4대에서 `team-ml`가 아닌 pod(시스템, GPU 검증 작업 등)가 GPU를 쓰는 일이 있는지 관리자에게 묻는다.
   
   SA에 Zone 읽기 권한(또는 노드 `get` 권한)을 받을 수 있으면, config의 보정값 대신 자동으로 읽는다.
4. **멀티노드 분산:** 한 작업을 여러 노드에 걸쳐 돌리는 기능이 필요한가? 필요하다면 노드 간 NCCL이 istio 환경에서 동작하는지, 패브릭 설정이 필요한지부터 확인해야 한다.
5. **warm pod:** 짧은 GPU 테스트를 반복할 때 Job 시작 시간이 문제가 되면(probe로 측정), `sleep infinity` pod에 exec하고 일정 시간 놀면 삭제하는 백엔드를 추가한다. 이때 exec는 gqd가 대신 한다.

## 13. 구현 순서 (v1)

1. **probe (Mac에서 수동):** CPU 1코어 Job으로 실행, 완료, 시작 시간을 측정한다. 이어서 GPU 1장으로 `nvidia-smi`를 실행한다. gqd Deployment를 server dry-run으로 검사한다.
2. **gqd.py, gq, test_gqd.py:** 순수 함수(`validate`, `capacity`, `place`, `derive_state`)를 먼저 만들고 테스트한 뒤 I/O를 붙인다. 토큰이 아직 있는 nogpu2에서 직접 실행해 확인한다. 이때 `gq capacity`가 보여주는 값을 MLXP UI의 노드별 GPU 사용량과 비교한다.
3. **gqd 배포:** Mac에서 ConfigMap과 Deployment를 배포한다(8장).
4. **노트북 토큰 끄기:** 모든 `alice-*` Notebook에 `automountServiceAccountToken: false`를 넣고 재시작한 뒤 `/var/run/secrets`를 확인한다.
5. **스킬과 Claude Code 설정, 그리고 체크리스트:**
   - 에이전트 셸에서 `kubectl get pods`가 실패하는지.
   - `gq run --gpus 1`이 성공하는지.
   - 상한을 넘는 요청이 `queued`나 `rejected`로 가는지.
   - `--time`이 지나면 `timeout`이 되는지.
   - 맞는 노드가 없으면 `queued`(`NoFit`)로 기다리다가 `wait_timeout`이 지나면 `expired`가 되는지.
   - 1장짜리 작업이 best-fit 노드로 가는지.
   - `--array`와 `--gpus LO-HI`가 동작하는지.
   - gqd를 재시작해도 대기열과 상태가 유지되는지.

> 위 구현 순서는 처음 설계할 때의 계획이다. 지금 설치 절차는 README.md의 `gqctl` 단계(init → probe → install → up → lockdown)를 따른다.

## 14. 여러 사용자로 일반화

처음에는 한 사람(`alice`)용으로 이름과 값을 고정했다. 이제는 **사람마다 인스턴스 하나**를 띄운다.

| 항목 | 결정 |
|---|---|
| 단위 | 인스턴스 = gqd 하나 + 설정 하나 + gq root 하나. 이름(`instance`, 1–16자 `[a-z0-9-]`)은 네임스페이스 안에서 겹치면 안 된다 |
| 리소스 이름 | Deployment·ConfigMap `gqd-<instance>`, Job `gq-<instance>-<id>-a<n>`, 라벨 `gq-instance=<instance>` |
| 설정 | `gqctl init --notebook <내 노트북>`이 Notebook 객체에서 읽는다: 이미지, PVC 마운트 전체, service account, `*-pod-default` 라벨(MLXP zone), PodDefault의 zone annotation, 그리고 사용자 계정으로 읽은 Zone의 노드 목록. 노트북이 정지 상태여도 된다 |
| 볼륨 | 노트북의 PVC 마운트 중 **다른 노드에서 동시에 마운트할 수 있는 것**만 Job pod에 같은 경로로 마운트한다. 즉 ReadWriteMany이고, 읽기 전용 마운트라면 ReadOnlyMany도 된다. 홈 PVC 외에 data 볼륨(`/home/irteam/data-vol1`)을 쓰는 사람이 많아서 하나만이 아니라 전부 본다 |
| 제외 볼륨 | ReadWriteOnce와 `local-path`(노드 로컬 디스크)는 `excluded_volumes`에 기록하고 마운트하지 않는다. local-path PV는 특정 노드에 묶여 있어서, gqd가 다른 노드로 고정한 Job이 Pending과 재배치를 반복하다 `expired`로 끝나기 때문이다. 공유 파일시스템의 RWO 볼륨은 그 CSI 드라이버가 여러 노드의 동시 마운트를 실제로 막는지 확인하지 못해서 안전하게 제외한다 |
| 가려짐 | 제외 볼륨이 포함 볼륨 안쪽에 마운트되어 있으면(예: RWX 홈 안의 local-path `scratch`), Job에서 그 경로는 홈 볼륨의 빈 디렉터리가 된다. 그래서 작업 디렉터리를 판정할 때는 포함·제외를 합쳐 **가장 긴 마운트**를 기준으로 하고, 그것이 제외 볼륨이면 gqd와 gq 모두 거부한다. gq는 heartbeat의 `excluded` 목록으로 미리 알려준다 |
| root | 홈 경로(`/home/<user>`)에 RWX PVC가 있으면 `<home>/gq`. 없으면 가장 짧은 쓰기 가능 RWX 마운트 위에 두고 `GQ_ROOT`를 설정하라고 안내한다. 쓰기 가능한 RWX가 하나도 없으면 `gqctl init`이 거부한다 |
| gq ↔ gqd | gq는 `$GQ_ROOT`(기본 `~/gq`)만 안다. 허용 경로(마운트 목록)는 gqd가 heartbeat에 올리고 gq가 읽는다 |
| 자원 추정 | 네임스페이스의 모든 pod를 센다. 다른 인스턴스의 Job도, 아직 노드에 올라가지 않은 다른 인스턴스 Job의 예약(annotation)도 포함한다. 그래서 같은 네임스페이스의 인스턴스들이 서로의 자리를 겹쳐 잡지 않는다 |
| 삭제 범위 | gqd는 `gq-<자기 instance>-...` 이름의 Job만 지운다. Kube 계층에서 정규식으로 강제한다 |
| 상한 | 인스턴스마다 따로다. 팀 전체의 공정성은 강제하지 않는다(합의로 정한다) |
| 매니페스트 | 정적 YAML을 두지 않고 `gqctl`이 설정에서 렌더링한다(`render`, `up`, `probe`) |
| gqd 이미지 | 기본은 노트북 이미지다. python3와 `kubernetes` 패키지가 없으면 `gqd_image`를 지정한다. `gqctl probe`가 확인한다 |

확인한 것:
- `gqctl init`을 세 가지 레이아웃의 실제 노트북에 실행해 봤다(읽기 전용): 홈 PVC 하나(alice), 홈 PVC + data 볼륨, 홈 PVC 없이 workspace + data 볼륨.
- `alice`의 설정은 손으로 쓴 이전 설정과 같게 나왔다.
- 네임스페이스에는 RWX(공유 파일시스템), RWO(같은 파일시스템), RWO `local-path` PVC가 섞여 있다. 실제 결과는 다음과 같았다.
  - `bob-gpu`: 홈 RWO + data RWX. 홈을 제외하고 root를 data 위에 둔다.
  - `carol-cpu`: local-path만 있다. init이 거부한다.
  - `alice`: RWX 하나. 그대로다.
- 렌더링한 Deployment, probe Job, ConfigMap이 server dry-run을 통과했다.
- 인스턴스 이름이 붙은 Job(`gq-alice-…-a1`)으로 gqd `--dry-run --once`를 돌려 API 서버의 수락을 확인했다.

남은 한계:
- 자원 추정은 내 네임스페이스의 pod만 본다. 같은 노드를 다른 네임스페이스도 쓰면 여유를 실제보다 많게 잡고, 재배치로만 흡수된다.
- MLXP가 아닌 Kubeflow에서는 zone과 노드를 자동으로 찾지 못한다. `pod_labels`와 `nodes`를 직접 채워야 한다.
- 각 gqd는 네임스페이스 편집 권한(`default-editor`)을 가진다. 다른 사람의 리소스를 건드리지 않는 것은 gqd 코드가 보장하는 것이지 RBAC가 보장하는 것이 아니다. Role을 만들 권한이 없어서 더 좁힐 수 없다.

### 14.1 일반 Kubeflow 지원

| 항목 | 결정 |
|---|---|
| Job pod 라벨 | 노트북 라벨 중 **PodDefault selector와 일치하는 것**만 복사한다. Kubeflow가 노트북 pod에 넣는 PodDefault(환경변수, 볼륨 등)를 Job pod도 똑같이 받게 하기 위해서다. MLXP의 zone 라벨도 같은 원리라 특별 처리가 필요 없다. PodDefault를 읽을 수 없으면 MLXP의 `*-pod-default` 이름 규칙으로 대신한다 |
| 플랫폼 감지 | 일치한 PodDefault에 MLXP zone annotation이 있으면 `platform: mlxp`. MLXP 전용 값(annotation 키, Zone 리소스, H200 노드 크기)은 gqctl의 상수 세 개에 모아 두었다 |
| 노드 목록 | `--nodes`가 있으면 그것을 쓰고, 없으면 MLXP Zone을 읽고, 그것도 없으면 `--node-selector`로 노드를 조회한다(권한이 있을 때). 셋 다 안 되면 비운다 |
| 노드 크기 | `--node-capacity`가 있으면 그것을 쓰고, 없으면 조회한 노드의 allocatable(GPU 노드 중 가장 작은 것)을 쓴다. 그것도 없으면 MLXP H200 기본값을 쓰거나, 기본값임을 경고한다 |
| scheduler 배치 모드 | `nodes`가 비면 gqd는 노드별 여유 계산과 고정(affinity)을 하지 않는다. gq 상한(`max_gpus_total`, `max_running`)만 지키고 노드는 스케줄러가 고른다. GPU 범위(`LO-HI`)는 상한 안에서 최대를 받는다. Pending이 길어도 재배치하지 않고 `wait_timeout`까지 기다린다(고정한 노드가 없으므로). `gq capacity`는 노드별 정보가 없다고 알려주고, `--fit`은 판단할 수 없어 1을 돌려준다 |
| `node_selector` | 선택 설정. Job pod의 nodeSelector로 들어간다. GPU 노드 풀 라벨 등에 쓴다 |
