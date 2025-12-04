import { EventEmitter } from 'node:events';
import * as Stream from 'node:stream';
import { JsonTransport } from '@vercel/queue';
import {
  MessageId,
  type Queue,
  QueuePayloadSchema,
  type QueuePrefix,
  type ValidQueueName,
} from '@workflow/world';
import { createEmbeddedWorld } from '@workflow/world-local';
import {
  run,
  makeWorkerUtils,
  Logger,
  type Runner,
  type WorkerUtils,
  type Task,
  type WorkerEvents,
} from 'graphile-worker';

// Silent logger to suppress graphile-worker's default INFO logs
const silentLogger = new Logger(() => () => {});
import { monotonicFactory } from 'ulid';
import { MessageData } from './message.js';
import type { PostgresWorldConfig } from './config.js';

/**
 * The Postgres queue uses Graphile Worker for job processing.
 * Unlike pg-boss which uses polling (min 500ms), Graphile Worker uses
 * PostgreSQL's LISTEN/NOTIFY for near-instant job processing (<3ms latency).
 *
 * Two task types are created:
 * - `{prefix}_flows` for workflow jobs
 * - `{prefix}_steps` for step jobs
 *
 * When a message is queued, it is sent to Graphile Worker with the appropriate task identifier.
 * When a job is processed, it is deserialized and then re-queued into the _embedded world_.
 */
export function createQueue(
  connectionString: string,
  config: PostgresWorldConfig
): Queue & { start(): Promise<void>; stop(): Promise<void> } {
  const port = process.env.PORT ? Number(process.env.PORT) : undefined;
  const embeddedWorld = createEmbeddedWorld({ dataDir: undefined, port });

  const transport = new JsonTransport();
  const generateMessageId = monotonicFactory();

  const prefix = config.jobPrefix || 'workflow';
  const TaskIdentifiers = {
    __wkf_workflow_: `${prefix}_flows`,
    __wkf_step_: `${prefix}_steps`,
  } as const satisfies Record<QueuePrefix, string>;

  const createQueueHandler = embeddedWorld.createQueueHandler;

  const getDeploymentId: Queue['getDeploymentId'] = async () => {
    return 'postgres';
  };

  let runner: Runner | null = null;
  let workerUtils: WorkerUtils | null = null;

  async function getWorkerUtils(): Promise<WorkerUtils> {
    if (!workerUtils) {
      workerUtils = await makeWorkerUtils({
        connectionString,
        logger: silentLogger,
      });
    }
    return workerUtils;
  }

  const queue: Queue['queue'] = async (queueName, message, opts) => {
    const utils = await getWorkerUtils();
    const [queuePrefix, queueId] = parseQueueName(queueName);
    const taskIdentifier = TaskIdentifiers[queuePrefix];
    const body = transport.serialize(message);
    const messageId = MessageId.parse(`msg_${generateMessageId()}`);

    await utils.addJob(
      taskIdentifier,
      MessageData.encode({
        id: queueId,
        data: body,
        attempt: 1,
        messageId,
        idempotencyKey: opts?.idempotencyKey,
        context: opts?.context,
      }),
      {
        jobKey: opts?.idempotencyKey ?? messageId,
        maxAttempts: 3,
      }
    );

    return { messageId };
  };

  function createTaskHandler(queuePrefix: QueuePrefix): Task {
    return async (payload, _helpers) => {
      const messageData = MessageData.parse(payload);
      const bodyStream = Stream.Readable.toWeb(
        Stream.Readable.from([messageData.data])
      );
      const body = await transport.deserialize(
        bodyStream as ReadableStream<Uint8Array>
      );
      const message = QueuePayloadSchema.parse(body);
      const queueName = `${queuePrefix}${messageData.id}` as const;
      await embeddedWorld.queue(queueName, message, {
        idempotencyKey: messageData.idempotencyKey,
        context: messageData.context,
      });
    };
  }

  const taskList: Record<string, Task> = {
    [TaskIdentifiers['__wkf_workflow_']]: createTaskHandler('__wkf_workflow_'),
    [TaskIdentifiers['__wkf_step_']]: createTaskHandler('__wkf_step_'),
  };

  // Create events emitter for debugging LISTEN/NOTIFY status
  const events: WorkerEvents = new EventEmitter() as WorkerEvents;
  const debug = config.debug ?? false;
  let notificationLogged = false;

  if (debug) {
    events.on('pool:listen:connecting', ({ attempts }) => {
      console.log(
        `[workflow-postgres] LISTEN connecting (attempt ${attempts})...`
      );
    });
    events.on('pool:listen:success', () => {
      console.log('[workflow-postgres] ✓ LISTEN/NOTIFY connected successfully');
    });
    events.on('pool:listen:error', ({ error }) => {
      console.error('[workflow-postgres] ✗ LISTEN/NOTIFY error:', error);
    });
    events.on('pool:listen:release', () => {
      console.log('[workflow-postgres] LISTEN connection released');
    });
    events.on('pool:listen:notification', () => {
      // Log sparingly - only first notification to confirm it's working
      if (!notificationLogged) {
        console.log(
          '[workflow-postgres] ✓ Received first NOTIFY - real-time notifications working'
        );
        notificationLogged = true;
      }
    });
  }

  return {
    createQueueHandler,
    getDeploymentId,
    queue,
    async start() {
      runner = await run({
        connectionString,
        concurrency: config.queueConcurrency || 10,
        taskList,
        events,
        logger: silentLogger,
        // Performance tuning for remote/serverless Postgres (Neon, Supabase, etc.)
        // where LISTEN/NOTIFY may have high latency or not work through poolers
        pollInterval: config.pollInterval ?? 1000,
        useNodeTime: config.useNodeTime ?? false,
      });
    },
    async stop() {
      if (runner) {
        await runner.stop();
        runner = null;
      }
      if (workerUtils) {
        await workerUtils.release();
        workerUtils = null;
      }
    },
  };
}

const parseQueueName = (name: ValidQueueName): [QueuePrefix, string] => {
  const prefixes: QueuePrefix[] = ['__wkf_step_', '__wkf_workflow_'];
  for (const prefix of prefixes) {
    if (name.startsWith(prefix)) {
      return [prefix, name.slice(prefix.length)];
    }
  }
  throw new Error(`Invalid queue name: ${name}`);
};
