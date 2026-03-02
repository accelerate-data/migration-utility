import { beforeEach, describe, expect, it, vi } from 'vitest';
import { setFrontendLogLevel, logger } from '@/lib/logger';

describe('logger', () => {
  beforeEach(() => {
    // Reset to default before each test (module-level state)
    setFrontendLogLevel('info');
  });

  it('defaults to info level', () => {
    const infoSpy = vi.spyOn(console, 'info').mockImplementation(() => {});
    const debugSpy = vi.spyOn(console, 'debug').mockImplementation(() => {});

    logger.info('visible');
    logger.debug('hidden');

    expect(infoSpy).toHaveBeenCalledTimes(1);
    expect(debugSpy).not.toHaveBeenCalled();

    infoSpy.mockRestore();
    debugSpy.mockRestore();
  });

  it('setFrontendLogLevel changes active level', () => {
    setFrontendLogLevel('warn');
    const debugSpy = vi.spyOn(console, 'debug').mockImplementation(() => {});
    const infoSpy = vi.spyOn(console, 'info').mockImplementation(() => {});
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});

    logger.debug('hidden debug');
    logger.info('hidden info');
    logger.warn('visible warn');

    expect(debugSpy).not.toHaveBeenCalled();
    expect(infoSpy).not.toHaveBeenCalled();
    expect(warnSpy).toHaveBeenCalledTimes(1);

    debugSpy.mockRestore();
    infoSpy.mockRestore();
    warnSpy.mockRestore();
  });

  it('filters debug/info when level is warn', () => {
    setFrontendLogLevel('warn');
    const debugSpy = vi.spyOn(console, 'debug').mockImplementation(() => {});
    const infoSpy = vi.spyOn(console, 'info').mockImplementation(() => {});
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    logger.debug('hidden debug');
    logger.info('hidden info');
    logger.warn('visible warn');
    logger.error('visible error');

    expect(debugSpy).not.toHaveBeenCalled();
    expect(infoSpy).not.toHaveBeenCalled();
    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(errorSpy).toHaveBeenCalledTimes(1);

    debugSpy.mockRestore();
    infoSpy.mockRestore();
    warnSpy.mockRestore();
    errorSpy.mockRestore();
  });
});
