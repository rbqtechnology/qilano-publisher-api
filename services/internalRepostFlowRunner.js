export default class InternalRepostFlowRunner {
  constructor({ getConfig, executeCycle }) {
    this.getConfig = getConfig;
    this.executeCycle = executeCycle;
  }

  async runOnce(context = {}) {
    const config = this.getConfig();
    const allowManualDiagnostics = context.allowManualDiagnostics === true;
    if ((!config.schedulerEnabled || !config.fullFlowEnabled) && !allowManualDiagnostics) {
      return {
        ok: false,
        status: "disabled",
        processing_engine: "internal",
        action: "full_repost_cycle",
        trace_id: null,
        queue_item_id: null,
        steps: {
          claim: null,
          prepare: null,
          rewrite: null,
          build_payload: null,
          publish: null,
          status_update: null,
        },
        message: !config.schedulerEnabled
          ? "Internal scheduler is disabled"
          : "Internal full repost flow is disabled",
      };
    }

    return this.executeCycle({
      ...context,
      config:
        allowManualDiagnostics && (!config.schedulerEnabled || !config.fullFlowEnabled)
          ? {
              ...config,
              schedulerEnabled: true,
              fullFlowEnabled: true,
            }
          : config,
    });
  }
}
