# kubeflow-gq

**gq**는 GPU 없이 상시 켜 둔 Kubeflow 노트북에서 GPU/CPU 작업을 던지고 관리하는 도구다. 사람과 에이전트(Claude Code 등)가 함께 쓴다. 일반 Kubeflow에서 동작하며, [MLXP](#mlxp에서-쓸-때)에서는 zone과 노드를 자동으로 찾는 기본 설정을 쓴다.

- **작업 실행:** 작업 하나는 k8s Job 하나로 돈다. 끝나면 GPU가 바로 반납된다. GPU 노트북을 켜고 끌 필요가 없다.
- **배치:** 노드 목록을 알면 노드별 남은 GPU를 보고, 단편화를 고려해 노드를 고른다. 4장짜리 작업은 한 노드에 4장이 비어 있어야 들어간다. 노드 목록을 모르면 스케줄러에게 맡긴다.
- **에이전트:** Kubernetes 권한 없이 `gq`만으로 작업을 시작하고 관리한다.
- **여러 사용자:** 사람마다 자기 **인스턴스**(자기 gqd, 자기 설정, 자기 상한)를 띄운다. 같은 네임스페이스에서 여러 명이 함께 써도 서로 간섭하지 않는다.

```
내 상시 노트북 (토큰 없음)                          gqd-<instance> (내 토큰을 가진 유일한 pod)
  사람 / 에이전트                                    코드·설정은 ConfigMap
  gq ──── 요청 파일 ────▶ <root>/spool ◀── 감시 ──── 검증 → 노드 선택 → Job 생성
     ◀─── 상태·로그 ───── <root>/jobs/<id>/ ◀─ 기록 ─ 실행/대기 시간 집행
     ◀─── 남은 자원 ───── <root>/capacity.json ◀──── 네임스페이스 pod로 노드별 여유 계산
                                                        │
                    Job pod (GPU 노드, 토큰 없음, 내 노트북과 같은 이미지·볼륨·env)
```

`<root>`는 공유 볼륨(PVC) 위의 gq 디렉터리다. 보통 `~/gq`다. 설계 배경과 결정 이유는 [DESIGN.md](DESIGN.md)에 있다.

## 목차

1. [파일 구성](#파일-구성)
2. [요구 사항](#요구-사항)
3. [설치](#설치) (`gqctl setup` 한 번, 또는 단계별)
4. [사용법](#사용법)
5. [MLXP에서 쓸 때](#mlxp에서-쓸-때)
6. [여러 사람이 쓸 때](#여러-사람이-쓸-때)
7. [운영](#운영)
8. [문제 해결](#문제-해결)
9. [개발](#개발)

## 파일 구성

| 파일 | 역할 | 어디서 실행되나 |
|---|---|---|
| `gqctl` | 설치·운영 도구: 설정 생성, probe, 설치, 배포, 잠금, 제거 | **내 노트북 PC** (kubectl이 되는 곳) |
| `gq` | CLI. 파일만 읽고 쓰며 표준 라이브러리만 쓴다 | 내 노트북 (`<root>/bin/gq`) |
| `gqd.py` | 데몬. 요청 검증, 자원 계산, 배치, Job 생성, 시간 집행 | `gqd-<instance>` pod (ConfigMap) |
| `skill/SKILL.md` | 에이전트용 사용 규칙 | 내 노트북 (`~/.claude/skills/gq/`) |
| `examples/config.mlxp.json` | MLXP에서 `gqctl init`이 만드는 설정 예시 (값은 가상) | 참고용 |
| `examples/config.kubeflow.json` | 일반 Kubeflow 설정 예시 (scheduler 배치 모드, 값은 가상) | 참고용 |
| `test_gqd.py` | 테스트 (의존성 없음) | 어디서든 |

## 요구 사항

- **노트북 PC:**
  - `kubectl`이 클러스터의 내 네임스페이스에 연결되어 있어야 한다. 그 네임스페이스에서 Deployment, ConfigMap, Job을 만들고 Notebook을 patch할 수 있어야 한다(Kubeflow의 contributor 권한이면 된다).
  - Python 3.8 이상. `gqctl`은 표준 라이브러리만 쓴다.
- **상시 노트북:** **ReadWriteMany(RWX)** PVC가 하나 이상 마운트된 노트북. 홈 디렉터리일 필요는 없다.
  - Job pod는 다른 노드에서 같은 볼륨을 동시에 마운트한다. 그래서 RWX(읽기 전용 마운트라면 ReadOnlyMany)만 쓸 수 있다.
  - ReadWriteOnce나 `local-path`(노드 로컬 디스크) 볼륨은 Job에 마운트하지 않는다. 그 위의 파일은 Job에서 보이지 않고, gq도 그 위에서는 제출을 거부한다(`--local`은 된다).
  - 접근 모드는 `kubectl get pvc`의 `ACCESS MODES`(RWX/RWO)로 확인한다.
  - 작업에 쓰는 코드, 데이터, conda env는 RWX 볼륨 위에 있어야 Job에서 그대로 보인다.
- **이미지:**
  - Job은 그 노트북의 이미지로 돈다.
  - gqd는 같은 이미지에 python3와 `kubernetes` 패키지가 있어야 한다. `kfp`가 깔린 Kubeflow Jupyter 이미지에는 보통 들어 있다. `gqctl probe`가 확인해 준다. 없으면 설정의 `gqd_image`에 다른 이미지를 지정한다.
- **클러스터:** Kubeflow Notebook(`notebooks.kubeflow.org`)과 PodDefault를 쓰는 멀티유저 Kubeflow. 노드 목록을 어떻게 얻느냐에 따라 배치 방식이 달라진다.

  | 노드 목록 | 배치 | 얻는 방법 |
  |---|---|---|
  | 있음 | **노드 배치 모드:** 노드별 여유를 보고 best-fit으로 고정한다. `gq capacity`가 노드별 여유를 보여준다 | MLXP는 자동, 그 외에는 `--nodes`나 `--node-selector` |
  | 없음 | **scheduler 배치 모드:** gq 상한만 지키고 노드는 Kubernetes 스케줄러가 고른다. 단편화를 피하지 못하고 노드별 여유를 보여주지 않지만, 작업은 그대로 돈다 | 기본값 |

## 설치

모든 `gqctl` 명령은 **노트북 PC에서, 이 저장소 루트에서** 실행한다. 아래 예시 값은 `alice`의 경우다.

### 빠른 설치: `gqctl setup`

상시 노트북을 **켜 둔 상태에서** 한 번 실행하면 아래 1–5, 7단계를 차례로 진행한다.

```bash
./gqctl setup --notebook alice-cpu --gpu
```

1. **설정:** `gq.config.json`을 만든다. 이미 있으면 그대로 쓰고, `--reinit`을 주면 다시 만든다. 일반 Kubeflow라면 `--nodes`, `--node-selector`, `--node-capacity`를 같이 줄 수 있다.
2. **probe:** 확인용 Job을 한 번 돌린다(`--skip-probe`로 건너뛴다).
3. **설치:** gq, 에이전트 스킬, `<root>/env.sh`를 넣는다. 셸 rc(`~/.bashrc`, 있으면 `~/.zshrc`)에는 env.sh를 읽는 한 줄을 추가하고, Claude Code 설정에는 `gq` 허용과 `kubectl` 금지를 합친다.
4. **gqd 배포:** gqd를 배포하고 heartbeat가 올라오는지 기다린다.
5. **end-to-end 확인:** 노트북 안에서 **실제 gq 작업**을 돌린다. CPU 작업은 항상, `--gpu`를 주면 GPU 1장 작업까지 돌린다.

각 단계는 같은 일을 반복해도 안전하다. 코드를 갱신한 뒤 다시 실행해도 된다.

마지막으로 토큰을 끈다(6단계). 실행 중인 노트북이 재시작되므로 따로 실행한다. `setup`에 `--lockdown`을 붙여 한 번에 해도 된다.

```bash
./gqctl lockdown
```

단계별로 하고 싶거나 문제가 생긴 단계만 다시 하려면 아래를 따른다.

### 1. 설정 만들기: `gqctl init`

상시로 켜 둘 노트북을 지정하면, 그 노트북 설정을 읽어 `gq.config.json`을 만든다. 노트북은 정지 상태여도 된다.

```bash
./gqctl init --notebook alice-cpu
```

```
wrote gq.config.json for instance 'alice' (namespace team-ml, platform mlxp)
  image    example/mlxp-notebook:cu128
  root     /home/irteam/gq
  volumes  alice-ws@/home/irteam
  excluded -
  labels   h200-pod-default=true, private-h200-acme-0-pod-default=true
  zone     private-h200-acme-0
  nodes    h200-node-a, h200-node-d, h200-node-c, h200-node-b
  capacity {"gpu": 8, "cpu": 125, "mem": "1964Gi"} per node
  ! node_capacity set to {"gpu": 8, "cpu": 125, "mem": "1964Gi"}, the MLXP H200 node size; change it if your zone's nodes differ
```

일반 Kubeflow에서는 노드 정보를 옵션으로 준다. 셋 다 선택이다.

```bash
./gqctl init --notebook alice-cpu --node-selector node-pool=gpu --nodes gpu-1,gpu-2 --node-capacity 8,96,1000Gi
```

- `--node-selector KEY=VALUE`: Job pod의 nodeSelector가 된다. 노드를 조회할 권한이 있으면 이 라벨로 노드 목록과 크기도 읽는다.
- `--nodes`: gq가 배치할 노드를 직접 지정한다.
- `--node-capacity GPU,CPU,MEM`: 노드 한 대의 크기를 지정한다. 노드를 조회할 수 있으면 allocatable에서 자동으로 읽는다.

노드 목록을 얻지 못하면 scheduler 배치 모드로 설정된다.

홈이 RWO이고 data 볼륨만 RWX인 노트북이라면 다음처럼 나온다. 이 경우 작업은 `/home/irteam/data-vol1` 아래에서 내야 한다.

```
  root     /home/irteam/data-vol1/gq
  volumes  bob-data@/home/irteam/data-vol1
  excluded bob-ws@/home/irteam
  ! bob-ws (ReadWriteOnce, shared-rwx) is left out: Job pods on other nodes cannot mount it, ...
```

RWX 볼륨이 하나도 없으면 `init`이 설정을 만들지 않고 이유를 알려준다.

생성된 파일을 열어 확인한다.

| 키 | 의미 | 확인할 점 |
|---|---|---|
| `instance` | 리소스 이름에 들어가는 짧은 이름 (기본값: 노트북 이름 앞부분) | **네임스페이스 안에서 겹치면 안 된다.** 겹치면 `--instance`로 바꾼다 |
| `image` | Job 이미지 (노트북 이미지) | |
| `volumes` | 노트북의 PVC 마운트 중 **RWX인 것**. Job pod도 같은 경로로 마운트한다 | 작업에 필요한 볼륨이 모두 들어갔는지 |
| `excluded_volumes` | 노트북에는 있지만 Job에 마운트할 수 없는 볼륨(RWO, `local-path`) | 여기 있는 경로에서는 pod 작업을 낼 수 없다. 필요하면 RWX 볼륨으로 옮긴다 |
| `root` | gq 디렉터리 (RWX 볼륨 위) | 홈에 RWX PVC가 없으면 다른 RWX 볼륨 위로 잡힌다. 이때는 `GQ_ROOT`를 설정해야 한다 (3단계) |
| `pod_labels` | 노트북의 PodDefault를 고르는 라벨. Job pod에도 붙여서 같은 PodDefault(환경변수, 볼륨, MLXP zone 등)가 들어가게 한다 | |
| `node_selector` | Job pod의 nodeSelector (선택) | |
| `platform`, `zone` | `mlxp` 또는 `kubeflow`, MLXP zone 이름 | 정보용 |
| `nodes` | gq가 배치할 노드. 비어 있으면 scheduler 배치 모드 | |
| `node_capacity` | 노드 한 대의 GPU/CPU/메모리 크기. 노드별 여유 계산과 GPU당 기본 limit에 쓴다 | 내 노드와 맞는지. MLXP라면 노트북 생성 화면의 최대값이다 |
| `max_gpus_total`, `max_running`, ... | 이 인스턴스의 상한 | 팀에서 합의한 값으로 |

### 2. 확인용 Job 한 번 돌리기: `gqctl probe`

이 환경에서 plain Job이 실행되고, 끝까지 완료되고, PVC에 쓸 수 있는지 확인한다. CPU 1코어만 쓰며 10분 뒤 자동으로 삭제된다.

```bash
./gqctl probe
```

```bash
./gqctl probe --gpu
```

`--gpu`를 주면 GPU 1장으로 `nvidia-smi`까지 실행한다. 확인하는 항목은 다음과 같다.
- Job이 완료되는지
- PVC에 쓸 수 있는지
- Job pod에 토큰이 없는지
- 이미지에 `kubernetes` 패키지가 있는지 (gqd용)

| 결과 | 조치 |
|---|---|
| Job이 완료되지 않음 | 출력에 나온 `kubectl describe` 명령으로 이벤트를 본다. Pending이면 자원이나 zone 라벨 문제다. 로그는 끝났는데 완료되지 않으면 istio sidecar 문제(DESIGN.md 2장)이고, gq를 쓸 수 없다 |
| `NO_K8S_CLIENT` | 설정에 `"gqd_image"`를 넣는다. python3와 `kubernetes` 패키지가 있는 이미지여야 한다 |

### 3. CLI와 스킬 설치: `gqctl install`

**실행 중인** 노트북을 하나 지정하면, 그 노트북을 통해 공유 볼륨에 `gq`와 에이전트 스킬을 복사한다. `<root>`도 여기서 만든다. gqd보다 먼저 만들어야 하는 이유는, 없으면 kubelet이 root 소유로 만들어 버리기 때문이다.

```bash
./gqctl install --notebook alice-cpu
```

함께 하는 일:
- **환경 파일:** `<root>/env.sh`(`GQ_ROOT`, `PATH`)를 쓴다.
- **셸 rc:** `~/.bashrc`(있으면 `~/.zshrc`도)에 `. <root>/env.sh` 한 줄을 표식 주석과 함께 넣는다. 여러 번 실행해도 한 번만 들어간다.
  - rc 파일이 symlink면(다른 곳에서 관리하는 경우) 건드리지 않고 넣을 줄만 알려준다.
  - `--no-shell-rc`로 끄고, `--shell-rc FILE`로 넣을 파일을 바꾼다.
- **Claude Code 설정:** `~/.claude/settings.json`에 `allow: Bash(gq *)`, `deny: Bash(kubectl *)`를 합친다. 처음 한 번 `settings.json.before-gq`로 백업하고, `--no-claude-settings`로 끈다.

끝나면 노트북에서 새 터미널을 열거나 `source ~/.bashrc`를 실행한다. 볼륨이 공유되므로, 같은 볼륨을 마운트하는 내 다른 노트북에서도 같은 `gq`를 쓸 수 있다.

> 스킬은 노트북 사용자의 `$HOME/.claude/skills/gq`에 설치된다. 에이전트가 노트북에서 읽기만 하면 되므로 RWX일 필요는 없다. 다만 홈이 PVC가 아니면 노트북을 재시작할 때 사라진다. 다른 위치가 필요하면 `--skills-dir`로 지정한다.

### 4. gqd 배포: `gqctl up`

```bash
./gqctl up
```

ConfigMap `gqd-<instance>`(gqd 코드 + 설정)와 Deployment `gqd-<instance>`를 만든다. 롤아웃이 끝나면 최근 로그를 보여준다. `gqd 0.1.0 started (instance=alice, ...)`가 나오고 에러가 반복되지 않으면 된다.

### 5. 동작 확인 (노트북 터미널)

```bash
gq capacity
```

노드별 GPU 여유가 보여야 한다. MLXP UI의 노드별 사용량과 비교한다.

```bash
gq run --gpus 1 --time 10m -- nvidia-smi
```

```bash
gq run --local --time 1m -- echo hello
```

### 6. 노트북 토큰 끄기: `gqctl lockdown`

Kubeflow 노트북 pod에는 보통 네임스페이스 편집 권한을 가진 토큰(`default-editor`)이 있다. 그래서 노트북 안의 에이전트가 python이나 curl로 **같은 네임스페이스에 있는 다른 사람의 노트북과 PVC까지 지울 수 있다.** 내 공유 볼륨을 마운트하는 pod 중 토큰을 가진 것이 gqd 하나뿐이어야 경계가 성립한다(DESIGN.md 7.2). 그래서 **그 볼륨을 마운트하는 내 노트북 전부**의 토큰을 끈다.

> ⚠️ 실행 중인 노트북은 **재시작된다.** tmux 세션, 실행 중인 프로세스, `--local` gq 작업이 모두 사라진다. 정지된 노트북은 다음에 켤 때 적용된다.

```bash
./gqctl lockdown
```

- **대상:** 기본값은 설정의 공유 볼륨(`volumes`)을 마운트하는 노트북 전부다. 직접 고르려면 `--notebook NAME`(여러 번 가능)이나 `--prefix PREFIX`를 쓴다.
- **확인:** 실행 중이던 노트북은 재시작될 때까지 기다렸다가, service account 토큰이 사라졌는지 확인한다. KFP 토큰이 남아 있는지도 알려준다. `--no-wait`로 건너뛸 수 있다.
- **정지된 노트북:** 다음에 켤 때 적용된다.

- **MLXP의 KFP 토큰:** MLXP 노트북에는 KFP 토큰(`/var/run/secrets/kubeflow/pipelines`)이 웹훅으로 따로 들어갈 수 있다. 남아 있다면 DESIGN.md 12장의 열린 질문을 본다.
- **설정 초기화:** MLXP UI에서 노트북 설정을 수정하면 이 값이 초기화될 수 있다. 수정한 뒤에는 위 확인을 다시 한다.
- **kubectl 위치:** 이후 kubectl 작업과 `gqctl` 실행은 노트북 PC에서만 한다.

### 7. 에이전트 설정 (Claude Code, 노트북 안)

3단계(`install`)에서 스킬 설치와 아래 권한 설정까지 자동으로 된다. `--no-claude-settings`를 줬거나 다른 위치를 쓴다면 `~/.claude/settings.json`에 다음을 **합친다.**

```json
{
  "permissions": {
    "allow": ["Bash(gq *)"],
    "deny": ["Bash(kubectl *)"]
  }
}
```

이 설정은 편의와 사고 방지용이다. 보안 경계는 6단계가 담당한다.

## 사용법

### 기본

```bash
gq capacity                                              # 노드별 남은 자원, gq 상한
gq run --gpus 1 --time 10m -- python check.py            # 실행하고, 로그를 보고, 끝날 때까지 대기
gq submit --gpus 2 --time 6h --name my-eval -- python eval.py --model llama3-8b   # 바로 반환
gq wait <id> --timeout 30m                               # 끝날 때까지 대기 (exit code = 작업 결과)
gq logs <id> -f                                          # 로그 따라가기
gq status <id>                                           # 상태 (--json 가능)
gq ps                                                    # 최근 24시간 작업 (--all, --mine, --json)
gq kill <id>                                             # 중지 (배열 그룹 id면 전체)
```

- **작업 환경:** 작업은 **제출한 디렉터리에서, 제출한 셸의 환경변수 그대로** 실행된다. conda env를 activate한 상태에서 제출하면 Job 안에서도 그 env다. 제외되는 변수는 `KUBERNETES_*`, `NVIDIA_*`, `CUDA_VISIBLE_DEVICES` 등이다. `--clean-env`를 주면 아무것도 넘기지 않고, `--env K=V`로 추가하거나 덮어쓴다.
- **경로:** 작업 디렉터리는 노트북이 마운트한 볼륨 위여야 한다. `/home → /home1` 같은 symlink는 gq가 알아서 처리한다.
- **비공개 범위:** 명령과 환경변수는 Job spec에 들어가지 않는다. 네임스페이스의 다른 사람이 볼 수 있는 것은 `--name`, owner, 자원뿐이다.

### 옵션

| 옵션 | 기본값 | 설명 |
|---|---|---|
| `--time` | (필수) | 실행 시간 상한. `90s`, `30m`, `4h`, `1h30m`, `2d` 형식. 넘으면 `timeout` |
| `--gpus` | `0` | `N` 또는 `LO-HI`. 범위를 주면 지금 한 노드에서 줄 수 있는 만큼 받는다 |
| `--array N` | | 같은 명령을 N개 task로 돌린다. 각 task는 독립적으로 배치된다 |
| `--cpu`, `--mem` | GPU 1장당 노드 한도/GPU 수, CPU 작업은 8코어/64Gi | **limit**이다. request는 항상 작게(1코어/4Gi) 잡는다 |
| `--wait-timeout` | `6h` | 제출부터 시작까지 기다리는 최대 시간. 넘으면 `expired` |
| `--name` | | 짧은 라벨 (`[A-Za-z0-9._-]`, 63자 이하). 네임스페이스에 보이므로 비밀을 넣지 않는다 |
| `--local` | | pod 대신 지금 이 노트북에서 실행한다 (CPU 작업 전용, gqd 불필요) |
| `--env K=V` / `--clean-env` | | 환경변수 추가·덮어쓰기 / 환경 전달 끄기 |
| `--cwd DIR` | 현재 디렉터리 | 작업 디렉터리 |

### 단편화를 고려해 나누기

**한 작업은 한 노드에만 올라간다.** 아래는 H200 8장 × 4대 zone의 예시다.

```
$ gq capacity
NODE               GPU free  CPU req free  MEM req free  CPU actual
h200-node-a     2/8       97.3          1781.1Gi      5.9
h200-node-b     1/8       72.1          586.5Gi       10
h200-node-c     1/8       22.4          391.2Gi       8.2
h200-node-d     0/8       56.3          1438.1Gi      4.9
total free 4 · max on one node 2 · namespace GPU waiting 0 · gq 0/4 · estimated (updated 1s ago)
```

위 상황에서 `--gpus 4`는 들어갈 노드가 없어 `queued`(`NoFit`)로 기다린다. 쪼갤 수 있는 작업이면 다음처럼 바꾼다.

```bash
# 독립적인 조각 (평가 shard, 시드, 모델별 실행): 1장짜리 task 4개가 2+1+1에 나눠 들어간다
gq submit --array 4 --gpus 1 --time 2h -- bash -c 'python eval.py --shard $GQ_TASK_INDEX --num-shards $GQ_TASK_COUNT'
```

```bash
# GPU 수에 맞춰 돌 수 있는 프로그램: 지금 가능한 최대(여기서는 2장)를 받는다
gq submit --gpus 1-4 --time 8h -- bash -c 'torchrun --nproc_per_node $GQ_GPUS train.py'
```

```bash
# 지금 4장이 들어가는 노드가 있는지 (exit 0/1)
gq capacity --fit 4
```

`$GQ_*`는 제출하는 셸이 아니라 task 안에서 확장되어야 하므로 `bash -c '...'`(작은따옴표)로 감싼다.

배치 규칙:
- **GPU 작업:** 들어갈 수 있는 노드 중 가장 꽉 찬 노드에 넣는다(best-fit). 큰 빈자리를 깨뜨리지 않기 위해서다.
- **CPU 작업:** GPU가 가장 적게 남은 노드에 넣는다.
- **대기열:** FIFO에 backfill을 허용한다. 큰 작업이 기다리는 동안 뒤의 작은 작업이 먼저 시작할 수 있다.
- **CPU/메모리:** 노트북처럼 **request는 작게(1코어/4Gi), limit은 크게** 잡는다.
  - 스케줄러는 request만 보므로 배치는 GPU 자리로 갈린다. `CPU req free`가 작은 노드(다른 노트북들이 request를 크게 잡은 노드)에도 GPU 자리만 있으면 들어간다.
  - 노드가 한가하면 limit까지 CPU를 더 쓸 수 있다. 붐비면 몫이 줄고, 노드 메모리가 부족하면 먼저 축출될 수 있다.
  - request 값은 설정의 `requests`에서 바꾼다.

### Job 안에서 쓸 수 있는 환경변수

| 변수 | 값 |
|---|---|
| `GQ_ID` | 작업 id |
| `GQ_GPUS` | 실제로 배정된 GPU 수 |
| `GQ_CPU` | CPU limit (코어) |
| `GQ_TASK_INDEX`, `GQ_TASK_COUNT` | 배열 작업일 때만 (0부터 시작) |
| `OMP_NUM_THREADS` | 설정하지 않았다면 `GQ_CPU / GQ_GPUS` |

### 상태

```
submitted → queued → pending → running → succeeded / failed
비정상 종료: timeout · expired · lost · killed · rejected
```

| 상태 | 의미 |
|---|---|
| `submitted` | 요청 파일만 있다. gqd가 아직 읽지 않았다 |
| `queued` | 기다리는 중. 사유는 `NoFit`(들어갈 노드 없음), `Cap`(내 gq 상한), `Replace`(재배치 중), `ApiError`(재시도 예정) |
| `pending` | Job이 만들어졌고 pod가 시작되는 중이다 |
| `running` | 실행 중 |
| `succeeded` / `failed` | exit 0 / 그 외. `failed`의 reason이 `OOMKilled`면 메모리 부족이다 |
| `timeout` | `--time`을 넘겨서 중지됐다 |
| `expired` | `--wait-timeout` 안에 시작하지 못했다 |
| `lost` | exit code 없이 pod가 사라졌다 (축출, 노드 장애, 노트북 재시작 시 `--local` 작업) |
| `killed` | `gq kill`로 중지했다 |
| `rejected` | 요청이 유효하지 않거나(`Invalid`) 대기열이 가득 찼다(`QueueFull`) |

### exit code (`gq run`, `gq wait`)

| code | 의미 |
|---|---|
| 0 | 성공 (그룹이면 모든 task가 성공) |
| 작업의 exit code | 작업이 실패함 (그룹이면 가장 먼저 실패한 task의 코드) |
| 124 | `wait --timeout`이 먼저 끝났다. **작업은 계속 돈다** |
| 125 | 정상 종료되지 못함 (`timeout`, `expired`, `lost`, `killed`, `rejected`). 사유가 stderr에 JSON으로 나온다 |
| 2 | 사용법 오류 |

`gq submit`, `gq wait`, `gq kill`, `gq capacity --fit`은 stdout에 JSON 한 줄을 출력한다. 스크립트나 에이전트가 파싱할 수 있다.

### 상한 (`gq.config.json`)

| 키 | 기본값 | 의미 |
|---|---|---|
| `max_gpus_total` | 4 | 이 인스턴스의 작업 전체가 동시에 쓰는 GPU 합 |
| `max_running` | 8 | 동시 실행 작업 수 |
| `max_queue` | 32 | 대기열 길이. 넘으면 `rejected`(`QueueFull`) |
| `max_time` / `max_wait_timeout` | 48h / 24h | `--time` / `--wait-timeout`의 최대값 |
| `max_array` | 64 | 배열 task 수 |
| `requests` | 1코어 / 4Gi | 모든 Job의 request |
| `retention_days` | 30 | `<root>/jobs/<id>/`를 보관하는 기간 |

설정을 바꾼 뒤에는 `./gqctl up`으로 반영한다.

## MLXP에서 쓸 때

MLXP(여러 사용자가 쓰는 Kubeflow 기반 GPU 플랫폼)에서는 `gqctl init`이 다음을 자동으로 채운다.

| 항목 | 출처 |
|---|---|
| zone 라벨 | 노트북의 `<zone>-pod-default` 라벨. PodDefault가 그 라벨로 zone annotation(`mlx.navercorp.com/zone`)을 넣는다 |
| 노드 목록 | Zone 객체(`zones.management.mlx.navercorp.com`)의 `spec.nodes`. 내 계정으로 읽는다 |
| 노드 크기 | H200 노드 기본값(GPU 8, CPU 125, 메모리 1964Gi). 노트북 생성 화면의 최대값과 같다. zone의 노드가 다르면 고친다 |

알아 둘 점:
- **노드 목록 갱신:** gqd의 서비스 계정은 Zone이나 Node를 읽을 수 없다. 그래서 노드 목록은 설정 파일에 고정된다. zone의 노드가 바뀌면 `nodes`를 고치고 `./gqctl up`을 실행한다.
- **CPU request가 큰 노트북:** 일부 노트북은 CPU request를 크게 잡는다. 그런 노드에서도 gq Job은 request가 작아서(1코어/4Gi) GPU 자리만 있으면 들어간다.
- **경로:** 이미지에서 `/home`은 `/home1`을 가리키는 symlink다. gq가 알아서 처리한다.
- **볼륨:** RWX, RWO, `local-path` PVC가 섞여 있을 수 있다. RWX만 Job에 마운트된다([요구 사항](#요구-사항)).
- **KFP 토큰:** 노트북 pod에는 KFP 토큰이 웹훅으로 들어갈 수 있고, `gqctl lockdown`으로는 지워지지 않는다(DESIGN.md 12장).

## 여러 사람이 쓸 때

각자 이 저장소를 받아 **자기 노트북으로** `gqctl init`부터 진행하면 된다. 인스턴스끼리는 다음처럼 나뉜다.

| | 인스턴스마다 따로 | 공유 |
|---|---|---|
| gqd | `gqd-<instance>` Deployment와 ConfigMap | |
| 파일 | 자기 공유 볼륨의 `<root>` | |
| Job | 이름 `gq-<instance>-<id>-a<n>`, 라벨 `gq-instance=<instance>` | |
| 상한 | `max_gpus_total` 등 | |
| 자원 추정 | | 네임스페이스의 모든 pod를 센다. 다른 사람의 gq 작업과 아직 노드에 올라가지 않은 Job의 예약까지 포함한다 |

- **삭제 범위:** gqd는 자기 인스턴스 이름이 붙은 Job만 지운다(코드에서 강제). 다른 사람의 작업을 건드리지 않는다.
- **상한은 인스턴스마다 따로다.** 팀 전체의 공정성까지 강제하지는 않는다. `max_gpus_total`은 팀에서 합의한 값으로 정한다.
- **자원 추정의 범위:** 내 네임스페이스의 pod만 볼 수 있다. 같은 노드를 다른 네임스페이스도 쓴다면 여유가 실제보다 많게 보인다. 그럴 때 Job이 Pending에 걸려도 60초 뒤 재배치로 흡수된다.

## 운영

| 할 일 | 명령 (노트북 PC) |
|---|---|
| 설정·코드 변경 반영 | `./gqctl up` |
| CLI·스킬 갱신 | `./gqctl install --notebook <running-nb>` |
| 상태 보기 (pod, Job, 최근 로그) | `./gqctl status` |
| gqd 로그 | `./gqctl logs -f` |
| 만들어질 매니페스트 보기 | `./gqctl render` |
| gqd 제거 (실행 중인 작업까지) | `./gqctl down` (`--jobs`) |

- **gqd는 상태를 들고 있지 않다.** 대기열은 spool 파일이고 실행 상태는 k8s Job이므로, `up`으로 재시작해도 대기 중인 작업과 실행 중인 작업이 유지된다.
- **노드 목록 갱신:** zone의 노드가 바뀌면 `gq.config.json`의 `nodes`를 고친다. MLXP라면 다음 명령으로 확인할 수 있다.

```bash
kubectl get zones.management.mlx.navercorp.com <zone> -o jsonpath='{.spec.nodes}'
```

## 문제 해결

### `gqd heartbeat is missing` / 작업이 `submitted`에서 멈춤

gqd가 돌고 있지 않다는 뜻이다. `./gqctl status`로 확인한다.

- **pod가 Pending:** zone 노드의 CPU가 부족하다.
- **`permission denied`:** `<root>`가 root 소유로 만들어진 것이다. `gqctl install`을 `up`보다 먼저 했는지 확인한다.
- **`No module named kubernetes`:** 설정에 `gqd_image`를 지정한다.
- **`dry_run=True`:** 테스트 모드로 떠 있다.
- **`GQ_ROOT`가 안 맞음:** 노트북의 `GQ_ROOT`가 설정의 `root`와 같아야 한다. gq는 `$GQ_ROOT/gqd.heartbeat`를 본다.

### `queued` (`NoFit`)가 오래감

들어갈 노드가 없다는 뜻이다. `gq capacity`로 확인하고 [단편화를 고려해 나누기](#단편화를-고려해-나누기)처럼 쪼개거나, `--wait-timeout`을 넉넉히 두고 기다린다.

### `queued` (`Cap`)

내 인스턴스의 상한(`max_gpus_total`, `max_running`)에 걸린 것이다. 내 다른 작업이 끝나면 시작된다.

### 자원 추정이 실제와 다름

`gq capacity`는 **네임스페이스 pod requests의 합**으로 계산한 추정치다. 다른 네임스페이스나 시스템 pod가 쓰는 자원은 보이지 않고, 노트북이 켜지는 것과도 2초 단위로 경쟁한다. 추정이 틀려서 Job이 Pending에 머물면, 60초 뒤 gqd가 지우고 다시 배치한다(`attempt`가 올라감). 계속 다르게 나오면 `node_capacity`와 `nodes`를 확인한다.

### `failed` (`OOMKilled`)

메모리 limit을 넘었다는 뜻이다. `--mem`을 올린다.

### `lost`

exit code 없이 pod가 사라졌다는 뜻이다. 노드 장애나 축출, 또는 `--local` 작업이 돌던 중 노트북이 재시작된 경우다. 자동으로 재시도하지 않으므로 다시 제출한다.

### `... is on a volume Job pods cannot mount`

그 디렉터리가 RWO나 `local-path` 볼륨 위에 있다는 뜻이다(설정의 `excluded_volumes`). Job pod는 다른 노드에서 그 볼륨을 마운트할 수 없다. 작업을 RWX 볼륨으로 옮기거나, 노트북 자원으로 충분하면 `--local`로 실행한다.

### `--cwd must be on a volume Job pods mount`

pod 작업은 노트북이 마운트한 볼륨 위에서만 실행할 수 있다. 프로젝트 디렉터리로 이동해서 제출한다. gqd가 처음 뜨기 전에는 gq가 마운트 목록을 몰라서 `~`만 허용한다.

### 작업 안에서 conda env나 패키지가 다름

환경은 **제출 시점의 셸**에서 가져온다. env를 activate한 상태에서 제출했는지 확인한다. `<root>/jobs/<id>/run.sh`에 실제로 넘어간 환경변수와 명령이 있다.

## 개발

### 테스트

의존성 없이 실행한다. 순수 로직, `gqctl`의 설정 탐지와 렌더링, 가짜 Kubernetes로 gq와 gqd 두 인스턴스를 함께 돌리는 end-to-end 테스트, `--local` 실제 실행을 포함한다.

```bash
python3 test_gqd.py
```

### 실제 클러스터 대상 dry-run

gqd를 한 번만 실행하되 Job 생성은 server-side dry-run으로 보낸다. 실제 pod로 자원을 계산하고 웹훅 통과까지 확인되지만, 아무것도 만들어지지 않는다. `kubernetes` 패키지와 클러스터 접근 권한이 필요하므로, 토큰을 끄기 전의 노트북이나 `pip install kubernetes`를 한 PC에서 실행한다. 다른 루트를 쓰면 운영 중인 gqd와 섞이지 않는다.

```bash
cd ~ && GQ_ROOT=/tmp/gqtest gq submit --gpus 1 --time 10m -- nvidia-smi
```

```bash
python3 -I gqd.py --config gq.config.json --root /tmp/gqtest --dry-run --once
```

```bash
GQ_ROOT=/tmp/gqtest gq ps
```

### 보안 모델 요약

- 토큰은 gqd pod에만 있다. gqd는 PVC의 코드를 실행하지 않고 `request.json`만 파싱한다.
- Job spec은 고정 템플릿으로만 만든다. 요청으로 바꿀 수 있는 값은 자원, 시간, 이름, 노드뿐이다. 볼륨과 이미지는 설정(ConfigMap)에서 온다.
- Job pod에는 토큰이 없다(`automountServiceAccountToken: false`).
- 자세한 내용과 남은 위험은 DESIGN.md 7장에 있다.
