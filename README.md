# process-supervisor-engine
JavaScript 进程监管库，处理重启策略、退避、依赖顺序和孤儿进程清理

## 设计

纯状态机，不创建真实子进程。所有进程操作通过注入的 `ops` 接口完成，
所有时间由调用方通过 `tick(now)` 推进，因此测试完全确定、无定时器。

```js
const { Supervisor } = require('./src/supervisor');

const sup = new Supervisor({
  processes: {
    db:  { stopTimeoutMs: 1000 },
    api: { dependsOn: ['db'], healthCheck: { intervalMs: 100 } },
    web: {
      dependsOn: ['api'],
      restart: {
        policy: 'on-failure',          // always | on-failure | never
        maxRetries: 3, windowMs: 10000,
        backoff: { baseMs: 100, factor: 2, maxMs: 5000 },
      },
    },
  },
  ops: {
    spawn(name, spec) { /* 拉起进程 */ },
    stop(name, spec)  { /* 优雅停止 */ },
    kill(name, spec)  { /* 超时强杀 */ },
    checkHealth(name) { return true; },
  },
});

sup.start();                 // 依赖健康前，下游保持 pending
sup.tick(100);               // 推进时间：健康检查 / 退避重启 / 停止超时
sup.notifyExit('web', 1);    // 上报退出事件
sup.stopAll();               // 按依赖逆序关闭，父进程等子进程退出
```

## 状态机

`stopped → pending → starting → running`，异常路径进入
`backoff / stopping / failed / orphaned`。

- `pending`：依赖未全部健康，禁止 spawn 下游。
- `starting`：已 spawn，等待健康检查通过（无 `healthCheck` 配置则 spawn 即 `running`）。
- `backoff`：崩溃后等待退避到期再重启。
- `orphaned`：优雅停止超过 `stopTimeoutMs`，已升级 `kill`，进程可能仍占端口；
  迟到的退出事件会将其回收为 `stopped`。

## 重启分类

退出按 `(exitCode, signal)` 分类：

| 类别 | 判定 | 语义 |
| --- | --- | --- |
| `clean` | code 0 且无信号 | 正常退出。`always` 立即重启；其余策略进入 `stopped`，不重启 |
| `crash` | 非零退出码或信号终止 | 崩溃。`never` 直接 `failed`；其余按指数退避重启 |
| `intentional` | 主动停止（`stopping`/`orphaned`）期间的退出 | 任何退出码都视为预期内，进入 `stopped`，绝不重启 |

崩溃退避：`delay = min(baseMs * factor^(n-1), maxMs)`，`n` 为 `windowMs`
滑动窗口内的崩溃次数；窗口滑出后重新计数。窗口内崩溃次数超过
`maxRetries` 判定为 crash loop，进入 `failed` 并停止自动重启
（`restart(name)` 可手动恢复并重置计数）。

## 最终退出语义

- 每个进程记录 `lastExit = { code, signal, at, kind }`，`kind` 即上表分类。
- `stopped` 与 `failed` 是终态：监管器不再对其施加任何自动动作。
- `stopAll()` 按依赖逆序退出：下游未退出（或超时成孤儿）前，上游不会收到 `stop`。
- 停止超时的进程进入 `orphaned` 并收到 `kill`，可通过 `orphans()` 查询；
  其迟到退出到达后回收为 `stopped`，保证编排器关闭后不留未跟踪的占端口进程。

## 测试

```sh
node --test
```

覆盖：正常退出、崩溃循环退避、并发启动与依赖健康门禁、停止超时、
依赖逆序关闭与孤儿回收。
