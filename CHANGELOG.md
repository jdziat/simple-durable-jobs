# Changelog

## [1.0.2](https://github.com/jdziat/simple-durable-jobs/compare/v1.0.1...v1.0.2) (2026-02-28)


### Bug Fixes

* track frontend/dist so go:embed works for consumers ([c13cf66](https://github.com/jdziat/simple-durable-jobs/commit/c13cf6636b8d8981b252df2458d27388a40b246c))
* track frontend/dist so go:embed works for consumers ([0dd1796](https://github.com/jdziat/simple-durable-jobs/commit/0dd17961ba86c38a8fe17e6f314ca2b0eb83479f))

## [1.0.1](https://github.com/jdziat/simple-durable-jobs/compare/v1.0.0...v1.0.1) (2026-02-28)


### Bug Fixes

* **ci:** create docs/static dir before copying demo ([#5](https://github.com/jdziat/simple-durable-jobs/issues/5)) ([ec29398](https://github.com/jdziat/simple-durable-jobs/commit/ec29398bcbc8784de59d99ff87751e2ffc361d16))
* upsert checkpoints instead of inserting duplicate rows ([a8e4d2a](https://github.com/jdziat/simple-durable-jobs/commit/a8e4d2a049b5e23671071e62cd8c9303493b4530))

## 1.0.0 (2026-02-19)


### Features

* add Checkpoint struct for durable calls ([b2a27d6](https://github.com/jdziat/simple-durable-jobs/commit/b2a27d665170b1265252ef2186bf112a000a933e))
* add event stream for observability ([3610ebc](https://github.com/jdziat/simple-durable-jobs/commit/3610ebc911a54007c24aecbe0feb691cfafe918b))
* add job options (queue, priority, retries, delay) ([26c2232](https://github.com/jdziat/simple-durable-jobs/commit/26c22325b764c933be32f394851d88c538d50bca))
* add Job struct with status constants ([d2b4151](https://github.com/jdziat/simple-durable-jobs/commit/d2b4151508938ac4ed31e1572ce316ebc64279a5))
* add NoRetry and RetryAfter error types ([c85a4d7](https://github.com/jdziat/simple-durable-jobs/commit/c85a4d746cbc12bc8fc2ee8543fa1649e4d65b36))
* add observability hooks (OnJobStart, OnJobComplete, OnJobFail, OnRetry) ([867e446](https://github.com/jdziat/simple-durable-jobs/commit/867e446ca6517a93a9836e44e4ed3444a2f7d65d))
* add Queue with Register and Enqueue ([5358799](https://github.com/jdziat/simple-durable-jobs/commit/535879968beaaf0d30830d029bf5b8c92298ab11))
* add Queue.Schedule and scheduler worker ([e6d4b10](https://github.com/jdziat/simple-durable-jobs/commit/e6d4b10194f6459eb0772ef207ccb320c4ff1a1e))
* add retry with backoff and connection pooling for resilience ([3e75ac0](https://github.com/jdziat/simple-durable-jobs/commit/3e75ac072cd4c66cbc0b7ef20ec79a35834d4c8a))
* add scheduling (Every, Daily, Weekly, Cron) ([12e5cf7](https://github.com/jdziat/simple-durable-jobs/commit/12e5cf7fe9078ca8a20362a07a3ecd43bd5f7e13))
* add SQLite detection with fallback locking strategy ([67e2beb](https://github.com/jdziat/simple-durable-jobs/commit/67e2beb1e22a2a7e4ffbdeb1a6c705214de3f70b))
* add standalone pause/resume functions ([eb50e0e](https://github.com/jdziat/simple-durable-jobs/commit/eb50e0e0daf94c5622a5183f48e29175cf71c232))
* add WithPollInterval worker option ([5ea5ab7](https://github.com/jdziat/simple-durable-jobs/commit/5ea5ab7aa586e4975303b3658440ea169b9fa37a))
* add Worker for job processing ([1150cbf](https://github.com/jdziat/simple-durable-jobs/commit/1150cbf106e53a4056818a75073f0610667e1d83))
* **core:** add fan-out and sub-job methods to Storage interface ([46aaa76](https://github.com/jdziat/simple-durable-jobs/commit/46aaa76e2eb5cd8d49c5ce516d3807ab40e098cf))
* **core:** add FanOut tracking struct ([4fd3805](https://github.com/jdziat/simple-durable-jobs/commit/4fd38059896d07e115fd7c9b5bdd33b225412f53))
* **core:** add FanOutCheckpoint for replay ([ff8f9aa](https://github.com/jdziat/simple-durable-jobs/commit/ff8f9aa0190aa5b48c3294327cde8eb9f125b3c1))
* **core:** add parent-child and fan-out fields to Job ([cefca6f](https://github.com/jdziat/simple-durable-jobs/commit/cefca6f7aa38ea767f6edd9dc0be9a18f07e827e))
* **core:** add pause-related errors ([0f6d679](https://github.com/jdziat/simple-durable-jobs/commit/0f6d6798b13116d8a80214503e4e66afbc0b1ab4))
* **core:** add pause/resume events ([fddee7a](https://github.com/jdziat/simple-durable-jobs/commit/fddee7a7126feb24c25b213d89404e765b4bb51c))
* **core:** add pause/resume methods to Storage interface ([a058739](https://github.com/jdziat/simple-durable-jobs/commit/a05873962e151953a6ad567cf02dba7bcdd1fa88))
* **core:** add QueueState model for queue pause tracking ([01ffaf6](https://github.com/jdziat/simple-durable-jobs/commit/01ffaf68ff60716d47771db5d50baa420741a8e7))
* **core:** add StatusPaused and PauseMode types ([e946eee](https://github.com/jdziat/simple-durable-jobs/commit/e946eeee14549946270eeb82b75fb1ee1464b76f))
* **core:** add waiting and cancelled job statuses ([0e77bd3](https://github.com/jdziat/simple-durable-jobs/commit/0e77bd3616b17b109efdec0a1d32ebf947c6cd2b))
* define Storage interface ([ccc637b](https://github.com/jdziat/simple-durable-jobs/commit/ccc637b8fa17c1541aefa2c1ffbcf7c668ebde01))
* export fan-out API from root package ([326730c](https://github.com/jdziat/simple-durable-jobs/commit/326730cfee2884b0e66d2e1527dcd9745340251c))
* **fanout:** add Sub() function for creating sub-jobs ([465a5e0](https://github.com/jdziat/simple-durable-jobs/commit/465a5e0b1105d1cf02fdb9b495b156b95fbadc2f))
* **fanout:** add types and options for fan-out API ([e94aaa9](https://github.com/jdziat/simple-durable-jobs/commit/e94aaa95c40c0325017edbbf5151fa7dc414a2db))
* **fanout:** implement core FanOut function and helpers ([61a2291](https://github.com/jdziat/simple-durable-jobs/commit/61a2291a31ab54cfd5af18559fc0e01b963e15e9))
* implement GORM storage backend ([b12f52e](https://github.com/jdziat/simple-durable-jobs/commit/b12f52ef66e51b334d64272aa9d17a8df236ffce))
* implement jobs.Call with checkpointing and replay ([c8eb8ea](https://github.com/jdziat/simple-durable-jobs/commit/c8eb8eaad943be74bcef4cc7faebe9b87d9f81dc))
* **queue:** add pause/resume convenience methods ([361df0f](https://github.com/jdziat/simple-durable-jobs/commit/361df0f97ae1ead78cf39ff16553c5fcca5a5da3))
* **queue:** add running job registry for cross-worker pause cancellation ([a899cf3](https://github.com/jdziat/simple-durable-jobs/commit/a899cf32877679b4ecddd25e2bb98873f3b3c030))
* **storage:** implement fan-out and sub-job storage methods ([7282351](https://github.com/jdziat/simple-durable-jobs/commit/7282351cc7a103fd4749ca6ce56a090cc8ba04f7))
* **storage:** implement job pause/unpause operations ([57ca593](https://github.com/jdziat/simple-durable-jobs/commit/57ca59312c84246c1f3ed72ca76543dcce2f54f2))
* **storage:** implement queue pause/unpause with dequeue filtering ([e1e1f55](https://github.com/jdziat/simple-durable-jobs/commit/e1e1f550229251ff10c828af020cba83111614c8))
* **ui:** add embedded web dashboard with stats and events ([6796862](https://github.com/jdziat/simple-durable-jobs/commit/6796862416beb673fd38ad53146b6971cb95a558))
* **worker:** add pause/resume state and methods ([3cadc70](https://github.com/jdziat/simple-durable-jobs/commit/3cadc70a159affba20e6637822810918463d17dc))
* **worker:** add running job tracking for aggressive cancel ([ada777b](https://github.com/jdziat/simple-durable-jobs/commit/ada777b719dd00a206a5553ca976cfbc3717d5e6))
* **worker:** handle SuspendError and sub-job completion ([ff4b520](https://github.com/jdziat/simple-durable-jobs/commit/ff4b5209870b9489dcf86985a4843850a932b943))
* **worker:** integrate pause check into worker loop ([1225e95](https://github.com/jdziat/simple-durable-jobs/commit/1225e95b9161c1a1e2164470d1a06d9afc401ce1))


### Bug Fixes

* add stale lock reaper to reclaim stuck running jobs ([427c804](https://github.com/jdziat/simple-durable-jobs/commit/427c80460510a72544217fd4a4a7e4ddc982e705))
* address 10x reviewer findings for pause/resume feature ([0d029d3](https://github.com/jdziat/simple-durable-jobs/commit/0d029d3192a7be45fa1558b166b69571a19eaba2))
* Concurrency() now scopes to current queue inside WorkerQueue() ([c9c23c0](https://github.com/jdziat/simple-durable-jobs/commit/c9c23c0d552bb5b47e796ad1d027af681602e109))
* critical concurrency and debugging issues ([2e6c3e9](https://github.com/jdziat/simple-durable-jobs/commit/2e6c3e9346f8f896a27a2c9e86d1e5a4aa0dc77a))
* **fanout:** track cancelled sub-jobs toward completion ([f1ec3ed](https://github.com/jdziat/simple-durable-jobs/commit/f1ec3edd05ab4cc501a5fbeacb2bdfffdb3f2e50))
* high-priority security and validation issues ([7745d9e](https://github.com/jdziat/simple-durable-jobs/commit/7745d9ecb4d09b5274990ebcd1ad10384661d278))
* **ui:** address 10x reviewer findings for embedded dashboard ([f54d1d7](https://github.com/jdziat/simple-durable-jobs/commit/f54d1d798d440e6b03d51c4f255467d196f12ec4))
