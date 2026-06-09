import { toStringTrim } from './login-utils.js';

type BranchHeadResponseLike = {
  data?: {
    commit?: {
      sha?: string | null;
    };
  };
};

export function readBranchHeadShaFromResponse(response: BranchHeadResponseLike | null | undefined): string {
  return toStringTrim(response?.data?.commit?.sha);
}
