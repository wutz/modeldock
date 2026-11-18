# 训推协同与多租户安全边界

本文档定义 ModelDock 如何与上游训练任务（`TrainPilot`）、强化学习（RL）工作流以及下游推理服务（`InferPilot`）协同，并确立多租户隔离边界。

---

## 1. 与 `TrainPilot`（SFT / 预训练）的标准协同流水线

在 `TrainPilot` 中，每个 `TrainTask` 均注入平台保留环境变量 `TASK_ID`、`RUN_ID`、`RESTART_COUNT`，并将 Checkpoint 落盘在共享 PVC 的固定目录结构下。ModelDock 提供两种无缝衔接方式：

### 1.1 声明式自动归档（任务结束后导出）

用户在提交训练任务的同时（或训练完成后在控制台一键点击“注册为模型”），在同 Namespace 下创建 `ModelExport`：

```text
TrainTask (Succeeded)
   │  共享 PVC 中已产出 /checkpoints/<TASK_ID>/<RUN_ID>/final/
   ▼
创建 ModelExport (引用共享 PVC + trainTaskRef)
   │  modeldock-exporter 校验 safetensors、提取架构参数、推送至 Harbor
   ▼
自动生成 ModelArtifact (stage: Staging, 携带完整 TASK_ID/RUN_ID 血缘)
   │  触发自动化评测任务 (Eval Job) 写回 evaluations[] 分数
   ▼
人工或策略门禁晋级 stage: Production
   │  触发 ModelRepository.autoWarmupStages 自动就绪 ModelCache
   ▼
InferPilot 监听到新 Production 版本 → 创建 LoRABinding 或按权重灰度滚动更新 InferenceService
```

### 1.2 训练中里程碑异步归档（不阻塞 GPU 训练进程）

对于运行数天的长周期预训练或大规模微调任务，训练脚本在落盘完 `global_step_5000` 目录并写入 `.checkpoint_done` 标记后，可通过集群内 ServiceAccount 调用 K8s API 创建一个 `ModelExport` CR：
- `modeldock-exporter` 调度在廉价的 **纯 CPU 节点** 上运行，以只读方式流式上传该 Step 目录至 Harbor；
- GPU 训练节点无需等待网络上传，零停顿继续跑 `step 5001`，实现**计算与归档彻底解耦**。

---

## 2. 与强化学习（RLHF / GRPO / Agentic RL）的协同边界

强化学习系统同时存在“制品版本管理”和“在线采样（Rollout）高频权重同步”两类截然不同的流量，ModelDock 明确采用**快慢分流架构**：

| 协同环节 | 频率与延迟要求 | 是否经过 ModelDock | 承载机制 |
| --- | --- | --- | --- |
| **RL 初始基座加载**（Actor / Critic / Reference / Rollout 池冷启动） | 任务启动时 1 次，要求分钟级内多节点同时挂载 | **是（慢路径）** | 引用同一份 `ModelArtifact` 的 `ModelCache` 只读卷，保证训练侧 Reference 模型与推理侧初始 Rollout 模型 Digest 100% 一致 |
| **RL 在线 Step 权重刷新**（GRPO/PPO 每步策略更新同步给推理池） | 每数十秒～数分钟 1 次，要求 **秒级完成** | **否（快路径旁路）** | 由 `TrainPilot` Actor 节点与 `InferPilot` Rollout 池通过 **NCCL / RDMA / NIXL** 直接覆写显存权重并刷新 KV Cache（由 `InferPilot` `RolloutChannel` 协调） |
| **RL 里程碑保存与最终策略交付** | 每数百步或收敛结束时，保留最优 Reward 版本上线 | **是（慢路径）** | 从 Actor 保存的 PVC Checkpoint 触发 `ModelExport` 入库 Harbor，晋级为 `Production` 后交付线上生产流量 |

> **设计红线**：严禁将 RL 每个训练 Step 的临时权重推送至 Harbor 或生成 `ModelArtifact`。这不仅会因落盘与 HTTP 传输把单步延迟拉长 10 倍以上，还会在 Harbor 与 etcd 中堆积数万个无意义的瞬态版本。

---

## 3. 跨集群训推同步（训练集群 → 在线推理集群）

当企业将重算力训练集群（机房 A）与面向公网用户的在线推理集群（机房 B / 公有云可用区）独立部署时，ModelDock 利用 Harbor 原生能力实现零自定义数据通道同步：

```text
[ 训练集群 (Cluster-Train) ]                        [ 在线推理集群 (Cluster-Infer) ]
  ModelExport 推送 OCI 模型                           Harbor Webhook 通知推入完成
           │                                                     │
           ▼                                                     ▼
   Harbor (Train-Registry)  ═══(Harbor Replication)═══▶  Harbor (Infer-Registry)
    仅同步打上 prod-* 或                                         │
    verified-* 标签的制品                                        ▼
                                                    ModelDock 自动同步生成同名同 Digest
                                                    ModelArtifact + 自动触发 ModelCache 预热
```

1. **按晋级阶段过滤带宽**：当用户在训练集群将某个 `ModelArtifact` 的 `spec.stage` 晋级为 `Verified` 或 `Production` 时，ModelDock 自动为 Harbor 内对应 Manifest 打上 `stage-verified` / `stage-production` 标签。
2. **Harbor 跨站复制**：Harbor Replication 规则仅匹配携带 `stage-production` 标签的制品同步至推理集群 Harbor，淘汰掉 95% 的实验性中间 Checkpoint 跨专线流量。
3. **推理侧免重新解析**：由于完整 `ServingProfile` 与血缘已固化在 OCI ModelPack Config JSON 内，推理集群的 ModelDock 收到 Harbor Webhook 后直接读取 Config Blob 即可生成完全一致的 `ModelArtifact` CR。

---

## 4. 多租户权限与凭证安全架构

### 4.1 Harbor Robot Account 最小权限映射

租户用户和训练/推理 Pod **永远不接触 Harbor 管理员账号**：
- `ModelRepositoryReconciler` 使用集群级管理凭证在 Harbor `models-<tenant>` 项目下创建两个独立的 Project-scoped Robot Account：
  - **Pull Robot**（仅限 `repository:pull` 权限）：写入当前 Namespace 的 `mrepo-<name>-pull` Secret，仅供 `ModelCache` 预热器和推理/评测拉取使用。
  - **Push Robot**（具备 `repository:pull` + `repository:push`，**无 `delete` 权限**）：写入 `mrepo-<name>-push` Secret，仅供 `modeldock-exporter` Job 挂载推送，防止被入侵的构建任务恶意删库。
- 支持定期自动轮换（默认 30 天）与注解触发即时轮换。

### 4.2 跨租户引用准入校验（Anti-Confused-Deputy）

当租户 A（Namespace `forge-tenant-a`）创建：
- 引用基座 `baseModelRef` 的 `ModelArtifact` / `ModelExport`；或
- 在 `InferPilot` 中创建引用某个 `ModelArtifact` 的 `InferenceService` / `LoRABinding`

准入控制器强制校验目标 `ModelArtifact` 所属 `ModelRepository` 的可见性契约：
1. 若目标位于同 Namespace：放行；
2. 若目标 `visibility == Public`（如 `modeldock-public` 下的平台官方基座）：放行；
3. 若目标 `visibility == WorkspaceShared` 且 `forge-tenant-a` 在目标 `sharedNamespaces[]` 白名单中：放行；
4. 其余一切跨 Namespace 引用一律在 API 准入阶段直接 `Deny`，杜绝越权挂载其他租户的微调权重或私有 LoRA。
