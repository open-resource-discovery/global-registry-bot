/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type */
/* eslint-disable require-await */
import { jest } from '@jest/globals';

async function loadSubject(opts?: { category?: string }) {
  jest.resetModules();

  const categoryFromTemplate = jest.fn().mockReturnValue(opts?.category ?? 'system');

  await jest.unstable_mockModule('../src/handlers/request/template.js', () => ({
    categoryFromTemplate,
  }));

  const mod = await import('../src/handlers/request/pr/snapshot.js');
  return { mod, mocks: { categoryFromTemplate } };
}

describe('src/handlers/request/pr/snapshot.ts', () => {
  describe('pickSnapshot', () => {
    it('template-driven: picks only template fields, skips markdown, normalizes scalars, adds _template/_category', async () => {
      const { mod, mocks } = await loadSubject({ category: 'system' });

      const template = {
        name: 'System Request',
        body: [
          { id: 'identifier', type: 'input' },
          { id: 'desc', type: 'textarea' },
          { id: 'md', type: 'markdown' }, // must be ignored
          { id: 'arr', type: 'input' },
          { id: 'obj', type: 'input' },
          { id: 'empty', type: 'input' },
          { id: 'missing', type: 'input' }, // not present in formData -> ignored
        ],
      };

      const formData = {
        identifier: '  acme.system  ',
        desc: ' hello ',
        md: 'should not appear',
        arr: [' a ', 'b', '', '  c  '],
        obj: { x: 1 },
        empty: '   ',
      };

      const snap = mod.pickSnapshot(formData, template);

      expect(mocks.categoryFromTemplate).toHaveBeenCalledWith(template);

      expect(snap).toEqual({
        _template: 'System Request',
        _category: 'system',
        identifier: 'acme.system',
        desc: 'hello',
        arr: 'a, b, c',
      });

      // explizit: markdown + object + empty dürfen NICHT drin sein
      expect(Object.prototype.hasOwnProperty.call(snap, 'md')).toBe(false);
      expect(Object.prototype.hasOwnProperty.call(snap, 'obj')).toBe(false);
      expect(Object.prototype.hasOwnProperty.call(snap, 'empty')).toBe(false);
    });

    it('template-driven: does not include _category when categoryFromTemplate returns empty', async () => {
      const { mod } = await loadSubject({ category: '' });

      const template = {
        title: 'Req',
        body: [{ id: 'identifier', type: 'input' }],
      };

      const snap = mod.pickSnapshot({ identifier: 'x' }, template);

      expect(snap._template).toBe('Req');
      expect(Object.prototype.hasOwnProperty.call(snap, '_category')).toBe(false);
      expect(snap.identifier).toBe('x');
    });

    it('fallback (no template): uses sorted keys, drops objects/null/undefined/empty, arrays become comma list', async () => {
      const { mod } = await loadSubject();

      const formData = {
        b: ' 2 ',
        a: '1',
        z: null,
        y: undefined,
        obj: { k: 1 },
        arr: [' x ', 'y', ''],
        empty: '   ',
      };

      const snap = mod.pickSnapshot(formData, null);

      // only a, b, arr in sorted insertion order, because the function iterates over sorted keys
      expect(Object.keys(snap)).toEqual(['a', 'arr', 'b']);

      expect(snap).toEqual({
        a: '1',
        arr: 'x, y',
        b: '2',
      });
    });
  });

  describe('calcSnapshotHash', () => {
    it('is deterministic and normalizes body newlines; hash changes when body changes', async () => {
      const { mod } = await loadSubject({ category: 'system' });

      const template = {
        name: 'System Request',
        body: [{ id: 'identifier', type: 'input' }],
      };

      const formData = { identifier: 'acme.system' };

      const h1 = mod.calcSnapshotHash(formData, template, 'a\r\nb\r\n');
      const h2 = mod.calcSnapshotHash(formData, template, 'a\nb');
      expect(h1).toBe(h2);

      const hNoBody = mod.calcSnapshotHash(formData, template, '');
      const hWithBody = mod.calcSnapshotHash(formData, template, 'X');
      expect(hNoBody).not.toBe(hWithBody);

      // basic sanity: sha1 hex length 40
      expect(h1).toMatch(/^[0-9a-f]{40}$/);
    });

    it('hash changes when template name changes', async () => {
      const { mod } = await loadSubject({ category: 'system' });

      const formData = { identifier: 'acme.system' };

      const t1 = { name: 'T1', body: [{ id: 'identifier', type: 'input' }] };
      const t2 = { name: 'T2', body: [{ id: 'identifier', type: 'input' }] };

      const h1 = mod.calcSnapshotHash(formData, t1, 'body');
      const h2 = mod.calcSnapshotHash(formData, t2, 'body');

      expect(h1).not.toBe(h2);
    });
  });

  describe('extractHashFromPrBody', () => {
    it('returns null for empty body', async () => {
      const { mod } = await loadSubject();

      expect(mod.extractHashFromPrBody(null)).toBeNull();
      expect(mod.extractHashFromPrBody(undefined)).toBeNull();
      expect(mod.extractHashFromPrBody('')).toBeNull();
    });

    it('extracts marker hash (case-insensitive, whitespace tolerant)', async () => {
      const { mod } = await loadSubject();

      const hash = '0123456789abcdef0123456789abcdef01234567';
      const body = `
Hello
<!--   nsreq:snapshot-hash  :   ${hash}   -->
World
`;

      expect(mod.extractHashFromPrBody(body)).toBe(hash);
    });
  });

  describe('findOpenIssuePrs', () => {
    function mkCtx(): any {
      return {
        octokit: {
          pulls: {
            list: jest.fn(),
          },
        },
        state: undefined as any,
      };
    }

    it('fetches paginated open PRs, caches them, and matches all supported body patterns', async () => {
      const { mod } = await loadSubject();

      const ctx = mkCtx();

      const issue = 123;

      const page1 = Array.from({ length: 100 }, (_, i) => ({
        number: i + 1,
        body: 'no match here',
      }));

      const page2 = [
        { number: 201, body: 'source: #123' }, // 1st regex
        { number: 202, body: 'issue #00123' }, // 2nd regex (+ parseInt)
        { number: 203, body: 'fix: #123' }, // 3rd regex
        { number: 204, body: 'fixes #123' }, // 4th regex (no colon)
        { number: 205, body: 'closes: #123' }, // 5th regex
        { number: 206, body: 'resolves: #123' }, // 6th regex
        { number: 207, body: null }, // should not match
        { number: 208, body: 'fix: #999' }, // different issue
      ];

      const list = ctx.octokit.pulls.list;

      list.mockImplementation(async (args: any) => {
        if (args.page === 1) return { data: page1 };
        if (args.page === 2) return { data: page2 };
        return { data: [] };
      });

      const res1 = await mod.findOpenIssuePrs(ctx, { owner: 'o', repo: 'r' }, issue);

      // pagination: 2 calls because first returns 100, second <100
      expect(list.mock.calls.length).toBe(2);
      expect(list.mock.calls[0][0]).toEqual(
        expect.objectContaining({ owner: 'o', repo: 'r', state: 'open', per_page: 100, page: 1 })
      );
      expect(list.mock.calls[1][0]).toEqual(
        expect.objectContaining({ owner: 'o', repo: 'r', state: 'open', per_page: 100, page: 2 })
      );

      expect(res1.map((p: any) => p.number).sort((a: number, b: number) => a - b)).toEqual([
        201, 202, 203, 204, 205, 206,
      ]);

      // state/cache
      expect(ctx.state).toBeDefined();
      expect(ctx.state._nsreqOpenPrCache).toBeInstanceOf(Map);

      // 2nd call with different issue uses cache -> no more API calls
      const res2 = await mod.findOpenIssuePrs(ctx, { owner: 'o', repo: 'r' }, 999);
      expect(list.mock.calls.length).toBe(2);
      expect(res2.map((p: any) => p.number)).toEqual([208]);
    });

    it('uses separate cache per owner/repo', async () => {
      const { mod } = await loadSubject();

      const ctx = mkCtx();

      const list = ctx.octokit.pulls.list;
      list.mockResolvedValue({ data: [{ number: 1, body: 'issue #1' }] });

      const r1 = await mod.findOpenIssuePrs(ctx, { owner: 'o1', repo: 'r1' }, 1);
      const r2 = await mod.findOpenIssuePrs(ctx, { owner: 'o2', repo: 'r2' }, 1);

      // different repo => 2 fetches
      expect(list.mock.calls.length).toBe(2);

      expect(r1.map((p: any) => p.number)).toEqual([1]);
      expect(r2.map((p: any) => p.number)).toEqual([1]);
    });
  });
});
