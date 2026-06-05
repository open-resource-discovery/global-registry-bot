import type { IssueWorkflowGuardCallbacks } from '../application/issue-workflow-guard.js';

type IssueParamsBase = {
  owner: string;
  repo: string;
  issue_number: number;
};

type IssueLikeBase = {
  body?: string | null;
  labels?: unknown;
  state?: string | null;
};

type TemplateLikeBase = {
  [key: string]: unknown;
};

type FormDataBase = Record<string, string>;

type EffectiveConstantsBase = {
  labelOnApproved?: string | null;
  approverUsernames: string[];
  approverPoolUsernames: string[];
};

type IssueUpdateContextBase = {
  octokit: {
    issues: {
      update: (params: IssueParamsBase & { body: string }) => Promise<unknown>;
      addLabels: (params: IssueParamsBase & { labels: string[] }) => Promise<unknown>;
    };
  };
};

export type IssueWorkflowGuardCompositionDependencies<
  ContextType extends IssueUpdateContextBase,
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
> = Omit<
  IssueWorkflowGuardCallbacks<
    ContextType,
    IssueParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >,
  'updateIssueBody' | 'addLabels' | 'createEmptyFormData'
>;

export function composeIssueWorkflowGuardCallbacks<
  ContextType extends IssueUpdateContextBase,
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  dependencies: IssueWorkflowGuardCompositionDependencies<
    ContextType,
    IssueParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >
): IssueWorkflowGuardCallbacks<
  ContextType,
  IssueParamsType,
  IssueType,
  TemplateType,
  FormDataType,
  EffectiveConstantsType
> {
  return {
    tryLoadTemplateForLabels: dependencies.tryLoadTemplateForLabels,
    normalizeKey: dependencies.normalizeKey,
    postOnce: dependencies.postOnce,
    updateIssueBody: async (context: ContextType, params: IssueParamsType, body: string): Promise<void> => {
      await context.octokit.issues.update({ ...params, body });
    },
    fetchIssueLabels: dependencies.fetchIssueLabels,
    toLabelNames: dependencies.toLabelNames,
    removeExactLabelsFromIssue: dependencies.removeExactLabelsFromIssue,
    addLabels: async (context: ContextType, params: IssueParamsType, labels: string[]): Promise<void> => {
      await context.octokit.issues.addLabels({ ...params, labels });
    },
    labelsMatching: dependencies.labelsMatching,
    loadTemplateWithLabelRefresh: dependencies.loadTemplateWithLabelRefresh,
    parseForm: dependencies.parseForm,
    createEmptyFormData: (): FormDataType => {
      const emptyFormData = Object.create(null) as FormDataType;
      return emptyFormData;
    },
    readIssueBodyForProcessing: dependencies.readIssueBodyForProcessing,
    isRequestIssue: dependencies.isRequestIssue,
    resolveEffectiveConstants: dependencies.resolveEffectiveConstants,
    resolveLockedWorkflowLabelKeys: dependencies.resolveLockedWorkflowLabelKeys,
    resolveWorkflowLabel: dependencies.resolveWorkflowLabel,
    resolveEffectiveRequestType: dependencies.resolveEffectiveRequestType,
    resolveApproverRoutingForRequestType: dependencies.resolveApproverRoutingForRequestType,
    uniqLogins: dependencies.uniqLogins,
    isConfiguredApprover: dependencies.isConfiguredApprover,
    setStateLabel: dependencies.setStateLabel,
    removeRejectedStatusLabel: dependencies.removeRejectedStatusLabel,
    removeProgressStatusLabels: dependencies.removeProgressStatusLabels,
    log: dependencies.log,
    getErrorMessage: dependencies.getErrorMessage,
  };
}
