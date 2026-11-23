// Preserve the existing fixture guards without changing application behavior.
process.env.JEST_WORKER_ID = process.env.VITEST_POOL_ID ?? '1';
