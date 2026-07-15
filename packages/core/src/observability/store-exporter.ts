import type { TraceRecord } from './records.js'
import type { ExportResult, TraceExporter } from './sink.js'
import { TraceStoreError, type TraceStore } from './store.js'

/** OBS-2 exporter adapter. Re-exported batches are safe because stores dedupe recordId. */
export class TraceStoreExporter implements TraceExporter {
  constructor(private readonly store: TraceStore) {}

  async export(records: readonly TraceRecord[], signal: AbortSignal): Promise<ExportResult> {
    if (signal.aborted) return { status: 'retryable', exported: 0, code: 'STORE_EXPORT_ABORTED' }
    try {
      await this.store.append(records)
      return { status: 'success', exported: records.length }
    } catch (error) {
      if (error instanceof TraceStoreError) {
        return {
          status: 'failure',
          exported: 0,
          code: error.code === 'UNSUPPORTED_SCHEMA_VERSION'
            ? 'STORE_UNSUPPORTED_SCHEMA_VERSION'
            : 'STORE_VALIDATION_FAILED',
        }
      }
      return { status: 'retryable', exported: 0, code: 'STORE_APPEND_FAILED' }
    }
  }
}
