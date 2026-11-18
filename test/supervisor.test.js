'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { Supervisor, STATE } = require('../src/supervisor');

/** 注入的模拟进程接口：记录全部操作，健康状态由测试控制。 */
class FakeOps {
  constructor() {
    this.calls = [];
    this.health = new Map(); // name -> bool，缺省 true
  }
  spawn(name) { this.calls.push({ op: 'spawn', name }); }
  stop(name) { this.calls.push({ op: 'stop', name }); }
  kill(name) { this.calls.push({ op: 'kill', name }); }
  checkHealth(name) { return this.health.get(name) !== false; }
  opsFor(name, op) { return this.calls.filter((c) => c.name === name && (!op || c.op === op)); }
  spawnCount(name) { return this.opsFor(name, 'spawn').length; }
}

function makeSupervisor(processes, ops) {
  return new Supervisor({ processes, ops });
}

test('正常退出：code 0 不触发重启，进入 stopped 终态', () => {
  const ops = new FakeOps();
  const sup = makeSupervisor({
    worker: { restart: { policy: 'on-failure' } },
  }, ops);

  sup.start();
  assert.equal(sup.state('worker'), STATE.RUNNING);
  assert.equal(ops.spawnCount('worker'), 1);

  sup.notifyExit('worker', 0);
  assert.equal(sup.state('worker'), STATE.STOPPED);
  assert.equal(sup.lastExit('worker').kind, 'clean');

  sup.tick(10_000);
  assert.equal(ops.spawnCount('worker'), 1, '正常退出不得重启');
});

test('崩溃循环：指数退避重启，超过 maxRetries 后进入 failed', () => {
  const ops = new FakeOps();
  const sup = makeSupervisor({
    svc: {
      restart: {
        policy: 'on-failure',
        maxRetries: 3,
        windowMs: 10_000,
        backoff: { baseMs: 100, factor: 2, maxMs: 1_000 },
      },
    },
  }, ops);

  sup.start();
  assert.equal(ops.spawnCount('svc'), 1);

  sup.notifyExit('svc', 1);
  assert.equal(sup.state('svc'), STATE.BACKOFF);
  sup.tick(99);
  assert.equal(ops.spawnCount('svc'), 1, '退避未到期不得重启');
  sup.tick(100); // 第一次退避 100ms
  assert.equal(ops.spawnCount('svc'), 2);

  sup.notifyExit('svc', 1);
  sup.tick(299);
  assert.equal(ops.spawnCount('svc'), 2);
  sup.tick(300); // 第二次退避 200ms
  assert.equal(ops.spawnCount('svc'), 3);

  sup.notifyExit('svc', 1);
  sup.tick(700); // 第三次退避 400ms
  assert.equal(ops.spawnCount('svc'), 4);

  sup.notifyExit('svc', 1); // 窗口内第 4 次崩溃 > maxRetries=3
  assert.equal(sup.state('svc'), STATE.FAILED);
  sup.tick(60_000);
  assert.equal(ops.spawnCount('svc'), 4, 'failed 后不得再自动重启');
  assert.equal(sup.lastExit('svc').kind, 'crash');
});

test('崩溃窗口滑出后重新计数，退避上限生效', () => {
  const ops = new FakeOps();
  const sup = makeSupervisor({
    svc: {
      restart: {
        policy: 'on-failure',
        maxRetries: 2,
        windowMs: 1_000,
        backoff: { baseMs: 100, factor: 10, maxMs: 250 },
      },
    },
  }, ops);

  sup.start();
  sup.notifyExit('svc', 1);
  sup.tick(100); // 退避 100ms 后重启
  assert.equal(ops.spawnCount('svc'), 2);

  sup.tick(5_000); // 窗口滑出
  sup.notifyExit('svc', 1);
  sup.tick(5_100); // 重新从 100ms 计
  assert.equal(ops.spawnCount('svc'), 3);

  sup.notifyExit('svc', 1);
  sup.tick(5_100 + 250); // 第二次退避被 maxMs 截断为 250ms
  assert.equal(ops.spawnCount('svc'), 4);
});

test('并发启动：无依赖进程同拍启动，下游等依赖健康后才启动', () => {
  const ops = new FakeOps();
  ops.health.set('a', false);
  ops.health.set('b', false);
  const sup = makeSupervisor({
    a: { healthCheck: { intervalMs: 100 } },
    b: { healthCheck: { intervalMs: 100 } },
    c: { dependsOn: ['a', 'b'] },
  }, ops);

  sup.start();
  assert.equal(ops.spawnCount('a'), 1);
  assert.equal(ops.spawnCount('b'), 1, 'a 与 b 应并发启动，互不等待');
  assert.equal(sup.state('c'), STATE.PENDING);
  assert.equal(ops.spawnCount('c'), 0, '依赖未健康，下游不得 spawn');

  ops.health.set('a', true);
  sup.tick(100);
  assert.equal(sup.state('a'), STATE.RUNNING);
  assert.equal(sup.state('b'), STATE.STARTING);
  assert.equal(sup.state('c'), STATE.PENDING, 'b 未健康，c 仍等待');

  ops.health.set('b', true);
  sup.tick(200);
  assert.equal(sup.state('b'), STATE.RUNNING);
  assert.equal(ops.spawnCount('c'), 1, '依赖全部健康后下游才启动');
  assert.equal(sup.state('c'), STATE.RUNNING);
});

test('停止超时：升级为 kill 并标记孤儿，迟到退出后回收', () => {
  const ops = new FakeOps();
  const sup = makeSupervisor({
    web: { stopTimeoutMs: 500 },
  }, ops);

  sup.start();
  sup.stop('web');
  assert.equal(sup.state('web'), STATE.STOPPING);
  assert.equal(ops.opsFor('web', 'stop').length, 1);

  sup.tick(499);
  assert.equal(sup.state('web'), STATE.STOPPING, '超时前保持 stopping');
  assert.equal(ops.opsFor('web', 'kill').length, 0);

  sup.tick(500);
  assert.equal(sup.state('web'), STATE.ORPHANED);
  assert.equal(ops.opsFor('web', 'kill').length, 1, '超时后升级 kill');
  assert.deepEqual(sup.orphans(), ['web']);

  sup.notifyExit('web', 137, { signal: 'SIGKILL' });
  assert.equal(sup.state('web'), STATE.STOPPED);
  assert.deepEqual(sup.orphans(), [], '迟到退出后孤儿被回收');
});

test('关闭编排器：按依赖逆序退出，子进程超时成孤儿后父进程才停止', () => {
  const ops = new FakeOps();
  const sup = makeSupervisor({
    db: { stopTimeoutMs: 1_000 },
    api: { dependsOn: ['db'], stopTimeoutMs: 1_000 },
    web: { dependsOn: ['api'], stopTimeoutMs: 1_000 },
  }, ops);

  sup.start();
  assert.deepEqual(sup.snapshot(), {
    db: STATE.RUNNING, api: STATE.RUNNING, web: STATE.RUNNING,
  });

  sup.stopAll();
  // 只有最下游 web 收到 stop；api/db 必须等子进程退出
  assert.equal(sup.state('web'), STATE.STOPPING);
  assert.equal(sup.state('api'), STATE.RUNNING, 'web 未退出前不得停 api');
  assert.equal(sup.state('db'), STATE.RUNNING, 'api 未退出前不得停 db');
  assert.deepEqual(ops.calls.filter((c) => c.op === 'stop').map((c) => c.name), ['web']);

  sup.notifyExit('web', 0);
  assert.equal(sup.state('web'), STATE.STOPPED);
  assert.equal(sup.state('api'), STATE.STOPPING, 'web 退出后才停 api');
  assert.equal(sup.state('db'), STATE.RUNNING);

  // api 不响应优雅停止，超时后成为孤儿，db 的停止继续推进
  sup.tick(1_000);
  assert.equal(sup.state('api'), STATE.ORPHANED);
  assert.equal(ops.opsFor('api', 'kill').length, 1);
  assert.equal(sup.state('db'), STATE.STOPPING, '子进程成孤儿后父进程停止流程继续');
  assert.deepEqual(sup.orphans(), ['api']);

  sup.notifyExit('db', 0);
  assert.equal(sup.state('db'), STATE.STOPPED);

  // 验证 stop 顺序严格为依赖逆序：web -> api -> db
  assert.deepEqual(
    ops.calls.filter((c) => c.op === 'stop').map((c) => c.name),
    ['web', 'api', 'db'],
  );

  // 迟到的 api 退出，孤儿回收
  sup.notifyExit('api', 137, { signal: 'SIGKILL' });
  assert.equal(sup.state('api'), STATE.STOPPED);
  assert.deepEqual(sup.orphans(), []);
});

test('restart 分类：always 重启正常退出，never 不重启崩溃，主动停止期间退出不重启', () => {
  const ops = new FakeOps();
  const sup = makeSupervisor({
    a: { restart: { policy: 'always' } },
    b: { restart: { policy: 'never' } },
    c: { restart: { policy: 'always' } },
  }, ops);

  sup.start();

  sup.notifyExit('a', 0); // always：正常退出也重启
  sup.tick(0);
  assert.equal(ops.spawnCount('a'), 2);

  sup.notifyExit('b', 1); // never：崩溃直接 failed
  assert.equal(sup.state('b'), STATE.FAILED);
  sup.tick(10_000);
  assert.equal(ops.spawnCount('b'), 1);

  sup.stop('c'); // 主动停止期间的退出是 intentional，不重启
  sup.notifyExit('c', 1);
  assert.equal(sup.state('c'), STATE.STOPPED);
  assert.equal(sup.lastExit('c').kind, 'intentional');
  sup.tick(10_000);
  assert.equal(ops.spawnCount('c'), 1);
});

test('手动 restart：运行中先停再起，failed 可手动恢复', () => {
  const ops = new FakeOps();
  const sup = makeSupervisor({
    svc: { restart: { policy: 'on-failure', maxRetries: 1, windowMs: 60_000 } },
  }, ops);

  sup.start();
  sup.restart('svc');
  assert.equal(sup.state('svc'), STATE.STOPPING);
  sup.notifyExit('svc', 0);
  assert.equal(sup.state('svc'), STATE.RUNNING, '停止完成后立即拉起');
  assert.equal(ops.spawnCount('svc'), 2);

  // 打到 failed 后手动恢复
  sup.notifyExit('svc', 1);
  sup.tick(100);
  sup.notifyExit('svc', 1);
  assert.equal(sup.state('svc'), STATE.FAILED);
  sup.restart('svc');
  assert.equal(sup.state('svc'), STATE.RUNNING);
});

test('依赖环与未知依赖直接报错', () => {
  const ops = new FakeOps();
  assert.throws(
    () => makeSupervisor({ a: { dependsOn: ['b'] }, b: { dependsOn: ['a'] } }, ops),
    /cycle/,
  );
  assert.throws(
    () => makeSupervisor({ a: { dependsOn: ['ghost'] } }, ops),
    /unknown process/,
  );
});
