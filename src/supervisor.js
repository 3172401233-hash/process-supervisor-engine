'use strict';

const STATE = Object.freeze({
  PENDING: 'pending',     // 等待依赖健康
  STARTING: 'starting',   // 已 spawn，等待健康检查通过
  RUNNING: 'running',     // 健康
  BACKOFF: 'backoff',     // 崩溃后等待退避重启
  STOPPING: 'stopping',   // 已发送优雅停止，等待退出
  STOPPED: 'stopped',     // 正常/主动停止（终态）
  FAILED: 'failed',       // 崩溃重试耗尽或策略不允许重启（终态）
  ORPHANED: 'orphaned',   // 停止超时，进程可能仍存活（已升级 kill）
});

const EXIT_KIND = Object.freeze({
  CLEAN: 'clean',           // code 0 且无信号
  CRASH: 'crash',           // 非零退出码或信号终止
  INTENTIONAL: 'intentional', // 主动停止期间的退出
});

const TERMINAL = new Set([STATE.STOPPED, STATE.FAILED, STATE.ORPHANED]);

function classifyExit(code, signal) {
  return code === 0 && !signal ? EXIT_KIND.CLEAN : EXIT_KIND.CRASH;
}

class Supervisor {
  /**
   * @param {object} opts
   * @param {Object<string, object>} opts.processes 进程树：name -> spec
   *   spec: {
   *     dependsOn?: string[],
   *     stopTimeoutMs?: number,
   *     healthCheck?: { intervalMs?: number },   // 缺省表示 spawn 即健康
   *     restart?: {
   *       policy?: 'always'|'on-failure'|'never',
   *       maxRetries?: number, windowMs?: number,
   *       backoff?: { baseMs?: number, factor?: number, maxMs?: number },
   *     },
   *   }
   * @param {object} opts.ops 注入的进程操作接口：
   *   spawn(name, spec)、stop(name, spec)、kill(name, spec)、checkHealth(name, spec) -> bool
   * @param {number} [opts.healthIntervalMs] 健康检查默认间隔
   */
  constructor({ processes, ops, healthIntervalMs = 200 } = {}) {
    if (!ops) throw new Error('ops interface is required');
    this.ops = ops;
    this.now = 0;
    this.records = new Map();

    for (const [name, spec] of Object.entries(processes || {})) {
      const restart = spec.restart || {};
      const backoff = restart.backoff || {};
      this.records.set(name, {
        name,
        spec,
        dependsOn: [...(spec.dependsOn || [])],
        state: STATE.STOPPED,
        desired: 'stopped',
        restartPolicy: restart.policy || 'on-failure',
        maxRetries: restart.maxRetries ?? 3,
        windowMs: restart.windowMs ?? 30000,
        backoff: {
          baseMs: backoff.baseMs ?? 100,
          factor: backoff.factor ?? 2,
          maxMs: backoff.maxMs ?? 5000,
        },
        stopTimeoutMs: spec.stopTimeoutMs ?? 5000,
        healthIntervalMs: spec.healthCheck
          ? (spec.healthCheck.intervalMs ?? healthIntervalMs)
          : null,
        crashTimes: [],
        nextRestartAt: null,
        stopDeadline: null,
        nextHealthCheckAt: null,
        restartAfterStop: false,
        lastExit: null,
        orphanedSince: null,
        spawnCount: 0,
      });
    }

    for (const rec of this.records.values()) {
      for (const dep of rec.dependsOn) {
        if (!this.records.has(dep)) {
          throw new Error(`${rec.name} depends on unknown process "${dep}"`);
        }
      }
    }
    this._assertAcyclic();
  }

  state(name) {
    return this._rec(name).state;
  }

  snapshot() {
    const out = {};
    for (const [name, rec] of this.records) out[name] = rec.state;
    return out;
  }

  lastExit(name) {
    return this._rec(name).lastExit;
  }

  orphans() {
    return [...this.records.values()]
      .filter((rec) => rec.state === STATE.ORPHANED)
      .map((rec) => rec.name);
  }

  /** 启动全部进程（缺省）或指定进程及其依赖。依赖健康前下游保持 pending。 */
  start(name) {
    if (name === undefined) {
      for (const rec of this.records.values()) rec.desired = 'running';
    } else {
      const rec = this._rec(name);
      rec.desired = 'running';
      for (const dep of this._transitiveDeps(rec)) {
        this.records.get(dep).desired = 'running';
      }
    }
    this._progress();
  }

  /** 主动停止单个进程；若有下游仍在运行，先等下游退出。 */
  stop(name) {
    this._rec(name).desired = 'stopped';
    this._progress();
  }

  /** 关闭编排器：全部进程按依赖逆序退出，父进程等子进程退出后才停止。 */
  stopAll() {
    for (const rec of this.records.values()) rec.desired = 'stopped';
    this._progress();
  }

  /** 手动重启：运行中则先停再拉起；终态则直接拉起并重置崩溃计数。 */
  restart(name) {
    const rec = this._rec(name);
    rec.desired = 'running';
    rec.crashTimes = [];
    if (rec.state === STATE.RUNNING || rec.state === STATE.STARTING) {
      rec.restartAfterStop = true;
      rec.desired = 'stopped';
    }
    this._progress();
  }

  /** 调用方推进时间；触发健康检查、退避重启与停止超时。 */
  tick(now) {
    if (now < this.now) throw new Error('time cannot move backwards');
    this.now = now;
    this._progress();
  }

  /** 进程退出事件（由注入接口的模拟方上报）。 */
  notifyExit(name, code, { signal = null } = {}) {
    const rec = this._rec(name);
    rec.lastExit = { code, signal, at: this.now, kind: null };

    switch (rec.state) {
      case STATE.ORPHANED:
        // 迟到的退出：孤儿被回收
        rec.state = STATE.STOPPED;
        rec.orphanedSince = null;
        rec.lastExit.kind = EXIT_KIND.INTENTIONAL;
        break;
      case STATE.STOPPING:
        rec.state = STATE.STOPPED;
        rec.stopDeadline = null;
        rec.lastExit.kind = EXIT_KIND.INTENTIONAL;
        if (rec.restartAfterStop) {
          rec.restartAfterStop = false;
          rec.desired = 'running';
        }
        break;
      case STATE.STARTING:
      case STATE.RUNNING:
        this._handleUnexpectedExit(rec, code, signal);
        break;
      default:
        return; // 过期/重复退出事件，忽略
    }
    this._progress();
  }

  // ---- 内部 ----

  _rec(name) {
    const rec = this.records.get(name);
    if (!rec) throw new Error(`unknown process "${name}"`);
    return rec;
  }

  _assertAcyclic() {
    const visiting = new Set();
    const done = new Set();
    const visit = (name, stack) => {
      if (done.has(name)) return;
      if (visiting.has(name)) {
        throw new Error(`dependency cycle: ${[...stack, name].join(' -> ')}`);
      }
      visiting.add(name);
      for (const dep of this.records.get(name).dependsOn) visit(dep, [...stack, name]);
      visiting.delete(name);
      done.add(name);
    };
    for (const name of this.records.keys()) visit(name, []);
  }

  _transitiveDeps(rec) {
    const out = new Set();
    const walk = (r) => {
      for (const dep of r.dependsOn) {
        if (!out.has(dep)) {
          out.add(dep);
          walk(this.records.get(dep));
        }
      }
    };
    walk(rec);
    return out;
  }

  _dependentsOf(name) {
    return [...this.records.values()].filter((r) => r.dependsOn.includes(name));
  }

  _depsRunning(rec) {
    return rec.dependsOn.every((dep) => this.records.get(dep).state === STATE.RUNNING);
  }

  _dependentsSettled(rec) {
    return this._dependentsOf(rec.name).every((d) => TERMINAL.has(d.state));
  }

  _spawn(rec) {
    rec.spawnCount += 1;
    rec.stopDeadline = null;
    rec.nextRestartAt = null;
    rec.orphanedSince = null;
    this.ops.spawn(rec.name, rec.spec);
    if (rec.healthIntervalMs === null) {
      rec.state = STATE.RUNNING;
    } else {
      rec.state = STATE.STARTING;
      rec.nextHealthCheckAt = this.now + rec.healthIntervalMs;
    }
  }

  _initiateStop(rec) {
    rec.state = STATE.STOPPING;
    rec.stopDeadline = this.now + rec.stopTimeoutMs;
    this.ops.stop(rec.name, rec.spec);
  }

  _handleUnexpectedExit(rec, code, signal) {
    const kind = classifyExit(code, signal);
    rec.lastExit.kind = kind;

    if (kind === EXIT_KIND.CLEAN && rec.restartPolicy !== 'always') {
      rec.state = STATE.STOPPED;
      rec.desired = 'stopped';
      return;
    }
    if (kind === EXIT_KIND.CRASH && rec.restartPolicy === 'never') {
      rec.state = STATE.FAILED;
      rec.desired = 'stopped';
      return;
    }

    if (kind === EXIT_KIND.CRASH) {
      rec.crashTimes.push(this.now);
      rec.crashTimes = rec.crashTimes.filter((t) => this.now - t <= rec.windowMs);
      if (rec.crashTimes.length > rec.maxRetries) {
        rec.state = STATE.FAILED;
        rec.desired = 'stopped';
        return;
      }
      const attempt = rec.crashTimes.length;
      const delay = Math.min(
        rec.backoff.baseMs * rec.backoff.factor ** (attempt - 1),
        rec.backoff.maxMs,
      );
      rec.nextRestartAt = this.now + delay;
    } else {
      // always + 正常退出：立即重启，不计入崩溃窗口
      rec.nextRestartAt = this.now;
    }
    rec.state = STATE.BACKOFF;
  }

  _progress() {
    let changed = true;
    let guard = 0;
    while (changed && guard++ < 1000) {
      changed = false;
      for (const rec of this.records.values()) {
        // 健康检查
        if (rec.state === STATE.STARTING && rec.nextHealthCheckAt <= this.now) {
          let healthy = false;
          try {
            healthy = !!this.ops.checkHealth(rec.name, rec.spec);
          } catch {
            healthy = false;
          }
          if (healthy) {
            rec.state = STATE.RUNNING;
            rec.nextHealthCheckAt = null;
            changed = true;
          } else {
            rec.nextHealthCheckAt = this.now + rec.healthIntervalMs;
          }
        }

        // 退避到期重启（依赖仍健康才拉起，否则回到 pending）
        if (rec.state === STATE.BACKOFF && rec.desired === 'running'
            && rec.nextRestartAt <= this.now) {
          if (this._depsRunning(rec)) {
            this._spawn(rec);
          } else {
            rec.state = STATE.PENDING;
            rec.nextRestartAt = null;
          }
          changed = true;
        }

        // 停止超时：升级为 kill 并标记孤儿
        if (rec.state === STATE.STOPPING && rec.stopDeadline <= this.now) {
          rec.state = STATE.ORPHANED;
          rec.orphanedSince = this.now;
          rec.stopDeadline = null;
          if (this.ops.kill) this.ops.kill(rec.name, rec.spec);
          changed = true;
        }

        // 期望运行
        if (rec.desired === 'running'
            && (rec.state === STATE.STOPPED || rec.state === STATE.FAILED
                || rec.state === STATE.PENDING)) {
          if (this._depsRunning(rec)) {
            if (rec.state === STATE.FAILED) rec.crashTimes = [];
            this._spawn(rec);
          } else if (rec.state !== STATE.PENDING) {
            rec.state = STATE.PENDING;
          }
          changed = true;
        }

        // 期望停止：先取消未运行的，再按依赖逆序优雅停止
        if (rec.desired === 'stopped') {
          if (rec.state === STATE.PENDING || rec.state === STATE.BACKOFF) {
            rec.state = STATE.STOPPED;
            rec.nextRestartAt = null;
            changed = true;
          } else if ((rec.state === STATE.STARTING || rec.state === STATE.RUNNING)
                     && this._dependentsSettled(rec)) {
            this._initiateStop(rec);
            changed = true;
          }
        }
      }
    }
  }
}

module.exports = { Supervisor, STATE, EXIT_KIND, classifyExit };
