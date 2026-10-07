# 海底观测网设备故障管理

这是一个只使用Python标准库和SQLite的模块化原型项目，默认端口为`8337`。领域对象包括站点、资产、链路、遥测、故障事件、恢复动作、出海任务、数据缺口、离线记录和对账冲突。`app.py`只负责参数解析、依赖组装和服务生命周期，业务状态机与约束集中在`src/rules.py`。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动和信号处理。
- `src/domain.py`：角色、领域异常、身份解析和实体数据结构。
- `src/rules.py`：状态机、角色权限、领域计算、来源优先级、冲突和跨对象校验。
- `src/repository.py`：SQLite建表、查询、事务、乐观锁、审计、幂等键和合并批次账本。
- `src/reconciliation.py`：离线批次对账合并（观测时间裁决、冲突另存、断点续跑、越权退回）。
- `src/service.py`：用例编排、离线记录合并入口、版本控制和审计写入。
- `src/http_api.py`：HTTP路由、JSON解析和统一错误响应。
- `src/audit.py`：实体操作审计时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则、失败场景和离线对账测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8337
```

服务启动时自动建表并执行升级迁移。`--host`可修改监听地址，`--db`可指定其他SQLite文件。旧版本写入、没有`source_id`的遥测等实体会在启动时按原值守人（`created_by`）补齐来源字段。初始化服务不需要单独命令，首次启动即可访问：

```bash
curl http://127.0.0.1:8337/health
```

## 主要接口

- `GET /health`：健康检查。
- `GET /api/<kind>`：按对象类型查询，可用`?status=`过滤。
- `POST /api/<kind>`：创建对象；请求体为JSON。
- `GET /api/entities/<id>`：读取对象当前版本。
- `POST /api/entities/<id>/actions`：提交`{"action":"动作名","data":{...},"expected_version":数字}`。
- `POST /api/offline-records`：整批上传离线记录，见下文。
- `GET /api/offline-batches/<batch_id>`：查询批次断点与逐条结果。
- `GET /api/audit`：读取审计记录。

身份通过`X-User-Id`和`X-Role`请求头传入，角色和动作权限由规则引擎校验。

## 离线对账合并

值守员断网期间把遥测修订、故障事件、事件动作和数据缺口记在本地，回岸后通过`POST /api/offline-records`整批上传：

```json
{
  "batch_id": "mission-2026-10-01",
  "records": [
    {
      "source_id": "bouy-7",
      "record_id": "rev-0001",
      "record_type": "telemetry_revision",
      "observed_at": "2026-10-01T08:30:00Z",
      "payload": {"asset_id": "...", "metric": "pressure", "value": 12.4, "revision": 2, "incident_id": "..."}
    },
    {"source_id": "bouy-7", "record_id": "inc-1", "record_type": "incident", "payload": {"asset_id": "...", "kind": "loss", "severity": "high", "summary": "..."}},
    {"source_id": "bouy-7", "record_id": "act-1", "record_type": "incident_action", "payload": {"incident_id": "...", "action": "resolve", "data": {"summary": "..."}}},
    {"source_id": "bouy-7", "record_id": "gap-1", "record_type": "gap", "payload": {"incident_id": "...", "start_at": "...", "end_at": "..."}}
  ]
}
```

对账规则：

- **按观测时间定先后**：同一资产、同一指标的遥测修订，以`observed_at`较新者为准，与上传顺序无关；较早观测与岸上现值不一致时不覆盖。
- **时间相同按来源优先级**：`observed_at`相同时，比较来源优先级（默认`shore < station < field`，可通过`RuleEngine(source_priority=...)`扩展，数值小者胜）。无法判定或低优先级来源与高优先级现值冲突时，不做静默覆盖。
- **冲突另存待处理**：无法自动裁决的记录另存为`reconciliation_conflict`（状态`pending`，含双方数据与原因），可经`resolve`动作处理。事件或其资产存在待处理冲突时，`resolve`和`close`都会被挡住（在线接口与离线批次均如此）；离线重复报障同样生成冲突。
- **断点续跑、不重复入库**：每条记录在独立事务内处理并写入批次账本（`merge_batches`/`merge_items`）。批次中途失败会停在最后一个成功断点并标记`failed`，用同一个`batch_id`重发即可从未处理记录继续；已处理记录直接返回账本结果。同一`(source_id, record_id)`在其他批次再出现时记为`duplicated`，绝不二次入库。
- **并发提交看最新结果**：写事务使用`BEGIN IMMEDIATE`串行化，两名值守员同时提交同一条记录时，先到者入库，后到者基于最新数据得到`duplicated`/`applied`/`conflicted`结果。
- **越权记录退回并说明原因**：记录逐条鉴权（如`operator`不能离线`resolve/close`事件，`viewer`不能提交批次），无权记录状态为`rejected`并在`reason`中说明原因，不产生业务实体，批次继续处理其余记录。

批次响应包含`state`（`running`/`failed`/`completed`）和每条记录的`status`（`applied`/`duplicated`/`conflicted`/`rejected`）、`reason`与落点`entity_id`。

## 核心流程

建立站点、资产和链路后记录遥测与故障事件，创建恢复动作并跟踪重启、备用链路、出海任务和数据缺口，最后关闭事件。遥测`revise`动作只接受更高修订号，用于处理迟到数据；在线写入自动按提交人补`source_id`。

## 规则重点

- 同一资产和故障类型不能同时有多个活动事件。
- 恢复动作按`dedupe_key`防止重复执行。
- 事件解决前恢复动作、数据缺口和受影响资产必须达到可关闭状态，且不存在待处理对账冲突。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

项目使用请求头模拟身份、SQLite单机持久化和简化状态机，适合原型演示和流程验证，不替代行业正式系统、设备控制系统或现场安全规程。
