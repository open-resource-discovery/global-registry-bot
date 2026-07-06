/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect, jest } from '@jest/globals';
import { readRepoFileText, isRepoContentFile } from '../src/handlers/request/infrastructure/repo-files.js';

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkContext(getContent: (args: unknown) => Promise<unknown>) {
  return { octokit: { repos: { getContent } } } as any;
}
const mkRepo = (): { owner: string; repo: string } => ({ owner: 'test-owner', repo: 'test-repo' });

describe('isRepoContentFile', () => {
  it('returns true for object with string content', () => {
    expect(isRepoContentFile({ content: 'abc' })).toBe(true);
  });
  it('returns false for array', () => {
    expect(isRepoContentFile([])).toBe(false);
  });
  it('returns false for null', () => {
    expect(isRepoContentFile(null)).toBe(false);
  });
  it('returns false when content is not string', () => {
    expect(isRepoContentFile({ content: 42 })).toBe(false);
  });
});

describe('readRepoFileText — toStringTrim edge cases', () => {
  it('L37 arm0: empty string filePath → path empty → returns null without fetching', async () => {
    const getContent = jest.fn();
    const result = await readRepoFileText(mkContext(getContent as any), mkRepo(), '');
    expect(result).toBeNull();
    expect(getContent).not.toHaveBeenCalled();
  });

  it('L21 arm0 + L37 arm0: null filePath → toStringTrim returns "" → returns null', async () => {
    const getContent = jest.fn();
    const result = await readRepoFileText(mkContext(getContent as any), mkRepo(), null as any);
    expect(result).toBeNull();
    expect(getContent).not.toHaveBeenCalled();
  });

  it('L22 arm1 + L23 arm0: number filePath → toStringTrim uses number branch → path "42"', async () => {
    const content = Buffer.from('{"key":"value"}').toString('base64');
    const getContent = jest.fn().mockResolvedValue({ data: { content, encoding: 'base64' } });
    const result = await readRepoFileText(mkContext(getContent as any), mkRepo(), 42 as any);
    expect(result).toBe('{"key":"value"}');
    expect(getContent).toHaveBeenCalledWith({ owner: 'test-owner', repo: 'test-repo', path: '42' });
  });

  it('L22 arm1 + L23 binary-expr arm1: object filePath → toStringTrim returns "" → returns null', async () => {
    const getContent = jest.fn();
    const result = await readRepoFileText(mkContext(getContent as any), mkRepo(), {} as any);
    expect(result).toBeNull();
    expect(getContent).not.toHaveBeenCalled();
  });

  it('L50 arm1: getContent returns file without encoding → uses "base64" fallback', async () => {
    const content = Buffer.from('hello world').toString('base64');
    const getContent = jest.fn().mockResolvedValue({ data: { content } });
    const result = await readRepoFileText(mkContext(getContent as any), mkRepo(), 'path/to/file.txt');
    expect(result).toBe('hello world');
  });

  it('L51 arm1: content is empty string → content || "" fires → returns ""', async () => {
    const getContent = jest.fn().mockResolvedValue({ data: { content: '', encoding: 'utf8' } });
    const result = await readRepoFileText(mkContext(getContent as any), mkRepo(), 'path/to/file.txt');
    expect(result).toBe('');
  });

  it('getContent returns array → !isRepoContentFile → returns null', async () => {
    const getContent = jest.fn().mockResolvedValue({ data: [{ type: 'dir' }] });
    const result = await readRepoFileText(mkContext(getContent as any), mkRepo(), 'some/dir');
    expect(result).toBeNull();
  });

  it('getContent throws → catch returns null', async () => {
    const getContent = jest.fn().mockRejectedValue(new Error('Not found'));
    const result = await readRepoFileText(mkContext(getContent as any), mkRepo(), 'some/path.txt');
    expect(result).toBeNull();
  });

  it('path with leading slashes is stripped before request', async () => {
    const content = Buffer.from('data').toString('base64');
    const getContent = jest.fn().mockResolvedValue({ data: { content, encoding: 'base64' } });
    await readRepoFileText(mkContext(getContent as any), mkRepo(), '///some/path.txt');
    expect(getContent).toHaveBeenCalledWith({ owner: 'test-owner', repo: 'test-repo', path: 'some/path.txt' });
  });
});
