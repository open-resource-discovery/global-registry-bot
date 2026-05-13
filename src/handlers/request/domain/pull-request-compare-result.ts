import { toStringTrim } from './login-utils.js';

type PullRequestCompareResultLike = {
  status?: string | null;
  ahead_by?: number | null;
};

export type PullRequestCompareResultEvaluation = {
  status: string;
  aheadBy: number;
  isBehindCurrentBase: boolean | null;
};

export function evaluatePullRequestCompareResult(
  result: PullRequestCompareResultLike | null | undefined
): PullRequestCompareResultEvaluation {
  const status = toStringTrim(result?.status).toLowerCase();
  const aheadBy = typeof result?.ahead_by === 'number' ? result.ahead_by : 0;

  if (status === 'ahead' || status === 'diverged' || aheadBy > 0) {
    return {
      status,
      aheadBy,
      isBehindCurrentBase: true,
    };
  }

  if (status === 'identical') {
    return {
      status,
      aheadBy,
      isBehindCurrentBase: false,
    };
  }

  return {
    status,
    aheadBy,
    isBehindCurrentBase: null,
  };
}
