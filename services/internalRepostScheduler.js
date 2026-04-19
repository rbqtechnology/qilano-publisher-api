export default class InternalRepostScheduler {
  constructor({ getConfig, runner, log = () => {} }) {
    this.getConfig = getConfig;
    this.runner = runner;
    this.log = log;
    this.timer = null;
    this.started = false;
    this.tickInProgress = false;
    this.lastTickAt = null;
    this.lastRunResult = null;
    this.lastSuccessAt = null;
    this.lastFailureAt = null;
  }

  start() {
    if (this.started) {
      return;
    }

    const config = this.getConfig();
    if (!config.schedulerEnabled) {
      this.log("info", "scheduler_init_disabled", {
        action: "scheduler",
        processing_engine: "internal",
        interval_ms: config.intervalMs,
        max_runs_per_tick: config.maxRunsPerTick,
        concurrency: config.concurrency,
      });
      return;
    }

    this.started = true;
    this.log("info", "scheduler_init_enabled", {
      action: "scheduler",
      processing_engine: "internal",
      interval_ms: config.intervalMs,
      max_runs_per_tick: config.maxRunsPerTick,
      concurrency: config.concurrency,
      note:
        "Before enabling the internal scheduler in production, pause or disable the n8n schedule trigger so only one scheduler path is active at a time.",
    });

    this.timer = setInterval(() => {
      this.tick({ trigger: "interval" }).catch((error) => {
        this.log("error", "scheduler_tick_error", {
          action: "scheduler",
          processing_engine: "internal",
          message: error?.message || String(error),
        });
      });
    }, config.intervalMs);
  }

  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.started = false;
  }

  async tick({ trigger = "manual", allowManualDiagnostics = false, maxRunsOverride = null } = {}) {
    const config = this.getConfig();
    if (this.tickInProgress) {
      this.log("warn", "overlapping_tick_skipped", {
        action: "scheduler",
        processing_engine: "internal",
        trigger,
        interval_ms: config.intervalMs,
        max_runs_per_tick: config.maxRunsPerTick,
        concurrency: config.concurrency,
      });
      return {
        ok: false,
        status: "skipped",
        processing_engine: "internal",
        action: "scheduler",
        message: "Scheduler tick skipped because a previous tick is still running",
      };
    }

    this.tickInProgress = true;
    this.lastTickAt = new Date().toISOString();

    try {
      this.log("info", "scheduler_tick_start", {
        action: "scheduler",
        processing_engine: "internal",
        trigger,
        interval_ms: config.intervalMs,
        max_runs_per_tick: config.maxRunsPerTick,
        concurrency: config.concurrency,
        dry_run: config.dryRun,
        manual_diagnostic: allowManualDiagnostics,
      });

      const results = [];
      const maxRuns = Number.isInteger(maxRunsOverride)
        ? Math.max(1, maxRunsOverride)
        : Math.max(1, config.maxRunsPerTick);
      const concurrency = Math.max(1, config.concurrency);
      let executed = 0;
      let shouldStop = false;

      while (executed < maxRuns && !shouldStop) {
        const batchSize = Math.min(concurrency, maxRuns - executed);
        const batch = await Promise.all(
          Array.from({ length: batchSize }, () =>
            this.runner.runOnce({
              trigger,
              dryRun: config.dryRun,
              workerId: config.workerId,
              allowManualDiagnostics,
            })
          )
        );
        results.push(...batch);
        executed += batch.length;

        if (batch.some((result) => result?.status === "no_item" || result?.status === "disabled")) {
          shouldStop = true;
        }
      }

      const summary = {
        ok: results.every((result) => result?.ok !== false),
        status: results.length > 0 ? results[results.length - 1]?.status || "no_item" : "no_item",
        processing_engine: "internal",
        action: "scheduler",
        trigger,
        interval_ms: config.intervalMs,
        max_runs_per_tick: maxRuns,
        concurrency: config.concurrency,
        dry_run: config.dryRun,
        manual_diagnostic: allowManualDiagnostics,
        runs: results,
        message:
          results.length > 0
            ? `Scheduler tick completed with ${results.length} run(s)`
            : "Scheduler tick completed with no runs",
      };

      this.lastRunResult = summary;
      if (summary.ok) {
        this.lastSuccessAt = new Date().toISOString();
      } else {
        this.lastFailureAt = new Date().toISOString();
      }
      this.log("info", "scheduler_tick_end", {
        action: "scheduler",
        processing_engine: "internal",
        trigger,
        status: summary.status,
        runs: results.length,
      });

      return summary;
    } finally {
      this.tickInProgress = false;
    }
  }

  async runNow() {
    return this.tick({
      trigger: "run_now",
      allowManualDiagnostics: true,
      maxRunsOverride: 1,
    });
  }

  getStatus() {
    const config = this.getConfig();
    return {
      ok: true,
      scheduler_enabled: config.schedulerEnabled,
      full_flow_enabled: config.fullFlowEnabled,
      interval_ms: config.intervalMs,
      max_runs_per_tick: config.maxRunsPerTick,
      concurrency: config.concurrency,
      worker_id: config.workerId,
      dry_run: config.dryRun,
      lease_seconds: config.leaseSeconds,
      retry_seconds: config.retrySeconds,
      started: this.started,
      tick_in_progress: this.tickInProgress,
      is_running: Boolean(this.timer) || this.tickInProgress,
      last_tick_at: this.lastTickAt,
      last_run_result: this.lastRunResult,
      last_success_at: this.lastSuccessAt,
      last_failure_at: this.lastFailureAt,
    };
  }
}
