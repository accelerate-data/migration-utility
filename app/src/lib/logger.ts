/**
 * Frontend logger.
 *
 * Wraps console.* with a runtime level filter so only messages at or above
 * the active level are emitted. Level is held in memory and set from the DB
 * via setFrontendLogLevel() — no localStorage dependency.
 *
 * Usage:
 *   import { logger } from '@/lib/logger';
 *   logger.info('workspace: applied');
 *   logger.error('workspace apply failed', err);
 */

export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export const LOG_LEVELS: LogLevel[] = ['debug', 'info', 'warn', 'error'];

const LEVEL_RANK: Record<LogLevel, number> = {
  debug: 0,
  info:  1,
  warn:  2,
  error: 3,
};

const DEFAULT_LEVEL: LogLevel = 'info';

let activeLevel: LogLevel = DEFAULT_LEVEL;

/** Set the active frontend log level (called once on mount from DB value). */
export function setFrontendLogLevel(level: LogLevel): void {
  activeLevel = level;
}

function shouldLog(level: LogLevel): boolean {
  return LEVEL_RANK[level] >= LEVEL_RANK[activeLevel];
}

export const logger = {
  debug: (msg: string, ...args: unknown[]): void => {
    if (shouldLog('debug')) console.debug(`[debug] ${msg}`, ...args);
  },
  info: (msg: string, ...args: unknown[]): void => {
    if (shouldLog('info')) console.info(`[info] ${msg}`, ...args);
  },
  warn: (msg: string, ...args: unknown[]): void => {
    if (shouldLog('warn')) console.warn(`[warn] ${msg}`, ...args);
  },
  error: (msg: string, ...args: unknown[]): void => {
    if (shouldLog('error')) console.error(`[error] ${msg}`, ...args);
  },
};
