import { describe, expect, it } from 'vitest'
import { BatchingTraceSink } from '../src/observability/batching.js'
import { InMemoryTraceStore } from '../src/observability/in-memory-store.js'
import type { TraceRecord } from '../src/observability/records.js'
import { TraceStoreExporter } from '../src/observability/store-exporter.js'
import type { TraceStore } from '../src/observability/store.js'
import { OpenMultiAgent } from '../src/orchestrator/orchestrator.js'
import type { LLMAdapter } from '../src/types.js'
import { attemptRecords, runTraceStoreContractSuite } from './helpers/trace-store-contract.js'

runTraceStoreContractSuite('InMemoryTraceStore', (options) => new InMemoryTraceStore(options))

describe('TraceStoreExporter', () => {
  it('participates in batching retry and makes duplicate delivery idempotent', async () => {
    const target = new InMemoryTraceStore()
    let calls = 0
    const flaky: TraceStore = {
      ...target,
      append: async (records) => {
        calls++
        if (calls === 1) throw new Error('temporary store outage')
        return target.append(records)
      },
      getRun: (...args) => target.getRun(...args),
      queryRuns: (...args) => target.queryRuns(...args),
      deleteRun: (...args) => target.deleteRun(...args),
      delete: (...args) => target.delete(...args),
      applyRetention: (...args) => target.applyRetention(...args),
    }
    const exporter = new TraceStoreExporter(flaky)
    const sink = new BatchingTraceSink(exporter, {
      scheduledDelayMs: 60_000, exportTimeoutMs: 100, maxRetries: 1,
      retryDelayMs: 0, retryJitter: false, diagnostics: 'silent',
    })
    const records = attemptRecords({ runId: 'export-retry' })
    for (const record of records) sink.emit(record)
    await expect(sink.forceFlush({ timeoutMs: 500 })).resolves.toMatchObject({ status: 'ok', exported: 2 })
    expect(sink.getStats()).toMatchObject({ exported: 2, retried: 2, failed: 0 })
    await expect(exporter.export(records, new AbortController().signal)).resolves.toEqual({ status: 'success', exported: 2 })
    expect((await target.getRun('export-retry', { includeRecords: true }))?.records).toHaveLength(2)
    await sink.shutdown({ timeoutMs: 500 })
  })

  it('maps validation failures permanently and unexpected store failures as retryable without throwing', async () => {
    const store = new InMemoryTraceStore()
    const exporter = new TraceStoreExporter(store)
    const unsupported = { ...attemptRecords({ runId: 'bad-schema' })[0]!, schemaVersion: 99 } as unknown as TraceRecord
    await expect(exporter.export([unsupported], new AbortController().signal)).resolves.toEqual({
      status: 'failure', exported: 0, code: 'STORE_UNSUPPORTED_SCHEMA_VERSION',
    })
    const broken = new TraceStoreExporter({
      append: async () => { throw new Error('store unavailable') },
    } as TraceStore)
    await expect(broken.export(attemptRecords({ runId: 'broken' }), new AbortController().signal)).resolves.toEqual({
      status: 'retryable', exported: 0, code: 'STORE_APPEND_FAILED',
    })
  })

  it('keeps store/export failure out of the Agent result while exposing delivery failure', async () => {
    const brokenStore = {
      append: async () => { throw new Error('store unavailable') },
    } as TraceStore
    const sink = new BatchingTraceSink(new TraceStoreExporter(brokenStore), {
      scheduledDelayMs: 60_000, exportTimeoutMs: 100, maxRetries: 0,
      diagnostics: 'silent',
    })
    const adapter: LLMAdapter = {
      name: 'trace-store-failure-test',
      async chat() {
        return {
          id: 'ok', content: [{ type: 'text', text: 'business result' }], model: 'test',
          stop_reason: 'end_turn', usage: { input_tokens: 1, output_tokens: 1 },
        }
      },
      async *stream() {},
    }
    const oma = new OpenMultiAgent({ defaultModel: 'test', observability: { sinks: [sink] } })
    const result = await oma.runAgent({ name: 'worker', model: 'test', adapter }, 'prompt')
    expect(result).toMatchObject({ success: true, output: 'business result', status: { code: 'ok' } })
    await expect(sink.forceFlush({ timeoutMs: 500 })).resolves.toMatchObject({ status: 'error', failed: expect.any(Number) })
    expect(sink.getStats().failed).toBeGreaterThan(0)
    await sink.shutdown({ timeoutMs: 500 })
  })
})

describe('InMemoryTraceStore baseline scale', () => {
  it('accepts and materializes 1k and 10k records within a broad regression budget', async () => {
    for (const count of [1_000, 10_000]) {
      const store = new InMemoryTraceStore()
      const base = attemptRecords({ runId: `scale-${count}`, startOnly: true })[0]!
      const records = Array.from({ length: count }, (_, index) => ({
        ...base,
        recordId: `scale-${count}-${index}`,
        sequence: index + 1,
        timestampUnixMs: index + 1,
        spanId: (index + 1).toString(16).padStart(16, '0'),
      })) as TraceRecord[]
      const started = performance.now()
      await expect(store.append(records)).resolves.toMatchObject({ written: count })
      const run = await store.getRun(`scale-${count}`, { includeRecords: true })
      const elapsedMs = performance.now() - started
      expect(run?.records).toHaveLength(count)
      expect(run?.spans).toHaveLength(count)
      expect(elapsedMs).toBeLessThan(5_000)
    }
  }, 15_000)
})
