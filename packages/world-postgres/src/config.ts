export interface PostgresWorldConfig {
  connectionString: string;
  jobPrefix?: string;
  queueConcurrency?: number;
  /**
   * How long to wait between polling for jobs in milliseconds.
   * Lower values = faster job pickup but more database load.
   * Default: 1000 (1 second)
   *
   * For serverless Postgres (Neon, Supabase) where LISTEN/NOTIFY
   * may not work well, consider lowering to 100-200ms.
   */
  pollInterval?: number;
  /**
   * Use Node.js time instead of PostgreSQL time.
   * Reduces a round-trip query per job. Recommended for remote databases.
   * Default: false
   */
  useNodeTime?: boolean;
  /**
   * Enable debug logging for queue operations
   * Shows LISTEN/NOTIFY connection status and notifications
   * Default: false
   */
  debug?: boolean;
}
