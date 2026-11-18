# Harbor OCI 模型封装与极速分发机制深潜

本文档详细说明 ModelDock 如何在 Harbor 之上按 OCI v1.1 与 CNCF ModelPack 规范组织大模型制品，以及如何消除“导出双倍落盘”与“推理扩容拉取风暴”。

---

## 1. OCI ModelPack 清单结构设计

ModelDock 推送至 Harbor 的每一个模型版本都是一个符合 **OCI Image Manifest v1.1** 的独立 Artifact，Harbor 界面可直接识别其 Artifact Type、大小、签名与漏洞/合规标签。

```json
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "artifactType": "application/vnd.cncf.model.manifest.v1+json",
  "config": {
    "mediaType": "application/vnd.cncf.model.config.v1+json",
    "digest": "sha256:8c1e...",
    "size": 4192
  },
  "layers": [
    {
      "mediaType": "application/vnd.cncf.model.weight.config.v1.tar+zstd",
      "digest": "sha256:1a2b...",
      "size": 3845120,
      "annotations": {
        "org.cncf.model.filepath": "configs-and-tokenizer.tar.zst",
        "modeldock.wutz.dev/contents": "config.json,generation_config.json,tokenizer.json,tokenizer_config.json,model.safetensors.index.json"
      }
    },
    {
      "mediaType": "application/vnd.cncf.model.weight.v1.raw",
      "digest": "sha256:5d9f...",
      "size": 4982162064,
      "annotations": {
        "org.opencontainers.image.title": "model-00001-of-00014.safetensors",
        "modeldock.wutz.dev/safetensors.header-size": "28416"
      }
    }
  ],
  "annotations": {
    "modeldock.wutz.dev/artifact-type": "FullWeight",
    "modeldock.wutz.dev/architecture": "Qwen3ForCausalLM",
    "modeldock.wutz.dev/precision": "BF16",
    "modeldock.wutz.dev/train-task-id": "task-98a7c1"
  }
}
```

### 1.1 为什么不把权重打包进 `.tar.gz`？

1. **跳过无效 CPU 压缩**：BF16/FP8 浮点指数与尾数近似随机分布，`gzip` 耗时数十分钟仅缩减 `<5%` 体积。采用 `v1.raw` 免压缩层，`modeldock-exporter` 可以跑满万兆/25G 网卡线速直传对象存储。
2. **文件级跨版本自然去重**：在冻结底层 Transformer 层、仅微调顶层或 Embedding 层的场景下，未变动的 `model-00001.safetensors` 拥有完全相同的 SHA256 Digest，Harbor Blob 上传接口（`HEAD /v2/<name>/blobs/<digest>`）秒级命中跳过上传。
3. **支持 Range 随机读取 Header**：通过在 Layer Annotation 记录 `safetensors.header-size`，校验器或控制台无需下载 5 GB 分片，仅发一次 `Range: bytes=0-28423` HTTP 请求即可在线检视张量形状与 dtype。

---

## 2. `modeldock-exporter` 零本地落盘流式导出流水线

导出 Job 以非特权容器运行（`privileged: false`、`allowPrivilegeEscalation: false`、`runAsNonRoot: true`），只读挂载训练任务的共享 PVC：

```text
训练共享 PVC (ReadOnly)
   │
   ├── 1. 预检扫描阶段 (Pre-flight Validation)
   │     ├─ 拒绝符号链接逃逸出 subPath 根目录（防路径穿越读取宿主机或其他租户目录）
   │     ├─ 校验 config.json 或 adapter_config.json 完整性，提取 ServingProfile
   │     ├─ 校验 model.safetensors.index.json 中声明的所有分片文件均存在且非空
   │     └─ 过滤优化器状态 (optimizer.pt / zero_stage3 / checkpoint_state)
   │
   ├── 2. 小文件配置层流式构建
   │     └─ 内存管道 tar 归档 + zstd 压缩 config/tokenizer 文件（通常 < 20 MiB），上传为首层
   │
   └── 3. 大权重分片并发直传 (Bounded Worker Pool, 默认并发度 4)
         ├─ Worker 顺序读取 safetensors 计算 SHA256（若存储系统提供可信扩展属性缓存可加速）
         ├─ 向 Harbor 发起 HEAD /v2/<repo>/blobs/<sha256>：已存在则 0 字节秒级复用
         └─ 不存在则重新 seek(0) 分块流式 PUT 至 Harbor，全程无需在容器本地写临时副本
```

---

## 3. `modeldock-puller` 与 `ModelCache` 极速挂载交付

当 `InferPilot` 需要将某个 70B 模型扩容至 8 个副本时，若由 8 个推理 Pod 的 `initContainer` 各自拉取 140 GB，总网络流量高达 `1.12 TB`。ModelDock 通过 `ModelCache` 提供两级缓存交付模型：

### 3.1 模式一：共享文件系统 CAS 缓存池（`SharedFilesystem`，主推）

适用于部署了高性能分布式文件系统（如 `AxonFS`、JuiceFS、CephFS、VAST NFS）的集群：

1. **目录拓扑**：
   ```text
   /modeldock-cas/
   ├── staging/
   │   └── tmp-<uuid>/              # 拉取写入中的临时目录（同文件系统，支持原子 rename）
   └── sha256/
       └── 4b227777d4dd.../         # 以 OCI Manifest Digest 命名的只读不可变目录
           ├── config.json
           ├── tokenizer.json
           ├── model-00001-of-00014.safetensors
           └── .modeldock-ready     # 全部文件 fsync 校验通过后最后写入的完成信标
   ```
2. **并发幂等**：即便多个 `ModelCache` 或控制循环同时请求预热同一 Digest，通过 K8s Lease / 原子目录 `rename` 保证只执行一次拉取。
3. **跨 Namespace 安全暴露**：租户推理 Pod **绝不挂载整个 `/modeldock-cas` 根目录**。ModelDock 为目标 Namespace 按需绑定只读 PVC 并由 `InferPilot` 注入精确到 `subPath: sha256/<digest>` 的只读挂载（`readOnly: true`），租户容器只能看到自身获权引用的那一个模型目录。

### 3.2 模式二：GPU 节点本地 NVMe 缓存池（`NodeLocalNVMe`）

适用于无全局高速共享存储、或追求极致单机 PCIe NVMe 冷启动吞吐（`> 6 GB/s`）的裸金属 GPU 节点：

1. `ModelCacheReconciler` 按 `nodeSelector` 匹配目标 GPU 节点，驱动节点上的轻量级 `modeldock-node-agent`（或按需预热 Job）。
2. 拉取流量强制经过集群内 **Dragonfly P2P Peer**：首个 GPU 节点从 Harbor 对象存储回源，其余 15 台 GPU 节点直接通过机架内高速网络互传 OCI 分片，避免打垮 Harbor 上联带宽。
3. 若两个不同微调版本共享部分未修改的 `safetensors` 分片，节点本地底层按 `/var/lib/modeldock/blobs/sha256/<blob-digest>` 存储单份物理文件，再通过**只读硬链接（Hardlink）**拼装出 `/var/lib/modeldock/manifests/sha256/<manifest-digest>/` 视图目录，节省本地 NVMe 容量。

---

## 4. 动态 LoRA 适配器的极速投递路径

对于体积通常在 `50 MiB ~ 1 GiB` 的 `LoRAAdapter`：
- 当共享基座推理池已经挂载了公共/租户 LoRA 缓存卷时，`ModelCache` 预热完成后新 LoRA 目录在毫秒级内对运行中的 `vLLM` Pod 可见；
- `InferPilot` 的 `LoRABinding` 控制器随即调用 `vLLM` `/v1/load_lora_adapter` 本地路径加载接口，**全程无需重启基座 Pod，从训练导出到线上可调用可在 15 秒内闭环**。
