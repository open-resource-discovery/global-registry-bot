export const LABEL_AUTHOR: null = null;
export const LABEL_REVIEW: null = null;

// Backwards compatibility alias
export const LABEL_CPA: null = LABEL_REVIEW;

// Doc links constant is kept for compatibility, runtime resolution is config-only.
export const DOC_LINKS: null = null;

// Approvers constants are kept for compatibility, runtime resolution is config-only.
export const DEFAULT_APPROVERS: string[] = [];
export const CPA_ASSIGNEES: string[] = [];

// NA set is empty by default, runtime NA handling is config-only.
export const NA_ALLOWED: Set<string> = new Set<string>();

export type AutoMergeMethod = 'merge' | 'squash' | 'rebase' | 'MERGE' | 'SQUASH' | 'REBASE' | null;

export type WorkflowReadableConfig = {
  workflow?: {
    labels?: {
      authorAction?: unknown;
      approverAction?: unknown;
      [k: string]: unknown;
    } | null;
    approvers?: unknown;
    links?: { docs?: unknown } | null;
    [k: string]: unknown;
  } | null;
};

export interface RequestConfigEntry {
  folderName: string | null;
  schema: string | null;
  issueTemplate: string | null;
  approvers?: string[] | null;
}

export interface PrAutoMergeConfig {
  enabled: boolean | null;
  method: AutoMergeMethod;
}

export interface PrConfig {
  branchNameTemplate: string | null;
  titleTemplate: string | null;
  commitMessageTemplate: string | null;
  autoMerge: PrAutoMergeConfig;
}

export interface WorkflowLabelsConfig {
  global: string[] | null;

  authorAction: string | null;
  approverAction: string | null;

  approvalRequested: string[] | null;
  approvalSuccessful: string[] | null;
  approvalRejected: string[] | null;

  autoMergeCandidate: string | null;
}

export interface WorkflowLinksConfig {
  docs: string | null;
}

export interface WorkflowConfig {
  labels: WorkflowLabelsConfig;
  approvers: string[] | null;
  links: WorkflowLinksConfig;
}

/**
 * Minimal shape used across the bot. Runtime may carry extra keys,
 * but these are the ones we actively read in handlers.
 */
export interface StaticRegistryBotConfig {
  requests: Record<string, RequestConfigEntry>;

  pr?: PrConfig | null;
  workflow?: WorkflowConfig | null;
}

/**
 * Resolve state labels ("author" / "review") from config.yaml only.
 */
export function getStateLabelsFromConfig(cfg: WorkflowReadableConfig | null | undefined): {
  author: string | null;
  review: string | null;
} {
  const labels = cfg?.workflow?.labels;

  const author =
    typeof labels?.authorAction === 'string' && labels.authorAction.trim() ? labels.authorAction.trim() : null;

  const review =
    typeof labels?.approverAction === 'string' && labels.approverAction.trim() ? labels.approverAction.trim() : null;

  return { author, review };
}

/**
 * Resolve approvers from config.yaml (workflow.approvers) only.
 */
export function getApproversFromConfig(cfg: WorkflowReadableConfig | null | undefined): string[] {
  const appr = cfg?.workflow?.approvers;

  if (!Array.isArray(appr)) return [];
  return appr.map((s) => String(s).trim()).filter(Boolean);
}

/**
 * Resolve documentation links from config.yaml (workflow.links.docs) only.
 */
export function getDocLinksFromConfig(cfg: WorkflowReadableConfig | null | undefined): string {
  const docs = cfg?.workflow?.links?.docs;

  if (typeof docs === 'string' && docs.trim()) return docs;
  return '';
}

export const DEFAULT_CONFIG: StaticRegistryBotConfig = {
  requests: {},

  pr: {
    branchNameTemplate: null,
    titleTemplate: null,
    commitMessageTemplate: null,
    autoMerge: {
      enabled: null,
      method: null,
    },
  },

  workflow: {
    labels: {
      // generic/global
      global: null,

      // state labels (author vs review)
      authorAction: null,
      approverAction: null,

      // request/approval flow
      approvalRequested: null,
      approvalSuccessful: null,
      approvalRejected: null,
      autoMergeCandidate: null,
    },

    // primary source for approver identities
    approvers: null,

    // optional links (e.g. documentation)
    links: {
      docs: null,
    },
  },
};

/**
 * We keep this as an untyped JSON schema object on purpose:
 * - it's a Draft 2020-12 JSON schema
 * - contains ajv-errors "errorMessage" keyword
 */
export const STATIC_CONFIG_SCHEMA: Record<string, unknown> = {
  $id: 'https://open-resource-discovery.github.io/registry-bot/config.schema.json',
  $schema: 'https://json-schema.org/draft/2020-12/schema',
  title: 'Registry Bot Configuration',
  description: 'Validates the shape of registry-bot static configuration (config.yaml).',
  type: 'object',
  additionalProperties: false,
  required: ['requests'],
  properties: {
    requests: {
      type: 'object',
      description: 'Mapping from requestType to request config (folderName, schema, issueTemplate).',
      minProperties: 1,
      additionalProperties: false,
      patternProperties: {
        '^.+$': {
          type: 'object',
          additionalProperties: false,
          required: ['folderName', 'schema', 'issueTemplate'],
          properties: {
            folderName: {
              type: ['string', 'null'],
              minLength: 1,
              description: 'Target folder for generated YAML files (e.g. /data/namespaces).',
              errorMessage: {
                type: 'requests[*].folderName must be a string.',
                minLength: 'requests[*].folderName must not be empty.',
              },
            },
            schema: {
              type: ['string', 'null'],
              minLength: 1,
              description: 'Path to the JSON schema used for validating this request type.',
              errorMessage: {
                type: 'requests[*].schema must be a string.',
                minLength: 'requests[*].schema must not be empty.',
              },
            },
            issueTemplate: {
              type: ['string', 'null'],
              minLength: 1,
              description: 'Path to the GitHub issue template for this request type.',
              errorMessage: {
                type: 'requests[*].issueTemplate must be a string.',
                minLength: 'requests[*].issueTemplate must not be empty.',
              },
            },
            approvers: {
              type: ['array', 'null'],
              items: { type: 'string' },
              minItems: 1,
              description: 'Optional approvers list for this requestType. Overrides workflow.approvers when provided.',
              errorMessage: {
                type: 'requests[*].approvers must be an array of strings.',
                minItems: 'requests[*].approvers must contain at least one approver when configured.',
              },
            },
          },
          errorMessage: {
            required: {
              folderName: "Each requests entry must define 'folderName'.",
              schema: "Each requests entry must define 'schema'.",
              issueTemplate: "Each requests entry must define 'issueTemplate'.",
            },
            additionalProperties:
              "Only 'folderName', 'schema', 'issueTemplate' and 'approvers' are allowed inside each requests entry.",
          },
        },
      },
      errorMessage: {
        type: 'requests must be an object.',
        minProperties: 'requests must define at least one requestType mapping.',
        additionalProperties: 'Only requestType keys are allowed inside requests (no extra properties).',
      },
    },

    pr: {
      type: ['object', 'null'],
      additionalProperties: false,
      required: ['autoMerge'],
      properties: {
        branchNameTemplate: {
          type: ['string', 'null'],
          minLength: 1,
          errorMessage: {
            type: 'pr.branchNameTemplate must be a string.',
            minLength: 'pr.branchNameTemplate must not be empty when provided.',
          },
        },
        titleTemplate: {
          type: ['string', 'null'],
          minLength: 1,
          errorMessage: {
            type: 'pr.titleTemplate must be a string.',
            minLength: 'pr.titleTemplate must not be empty when provided.',
          },
        },
        commitMessageTemplate: {
          type: ['string', 'null'],
          minLength: 1,
          errorMessage: {
            type: 'pr.commitMessageTemplate must be a string.',
            minLength: 'pr.commitMessageTemplate must not be empty when provided.',
          },
        },
        autoMerge: {
          type: ['object', 'null'],
          additionalProperties: false,
          required: ['enabled', 'method'],
          properties: {
            enabled: {
              type: ['boolean', 'null'],
              errorMessage: {
                type: 'pr.autoMerge.enabled must be a boolean.',
              },
            },
            method: {
              type: ['string', 'null'],
              enum: ['merge', 'squash', 'rebase', 'MERGE', 'SQUASH', 'REBASE', null],
              errorMessage: {
                type: 'pr.autoMerge.method must be a string.',
                enum:
                  'pr.autoMerge.method must be one of: merge, squash, rebase, MERGE, SQUASH, REBASE, or null.',
              },
            },
          },
          errorMessage: {
            required: {
              enabled: 'pr.autoMerge.enabled is required when autoMerge is configured.',
              method: 'pr.autoMerge.method is required when autoMerge is configured.',
            },
            additionalProperties: "Only 'enabled' and 'method' are allowed inside pr.autoMerge.",
          },
        },
      },
      errorMessage: {
        type: 'pr must be an object when provided.',
        additionalProperties:
          "Only 'branchNameTemplate', 'titleTemplate', 'commitMessageTemplate' and 'autoMerge' are allowed inside pr.",
      },
    },

    workflow: {
      type: ['object', 'null'],
      additionalProperties: false,
      required: ['labels', 'approvers'],
      properties: {
        labels: {
          type: ['object', 'null'],
          additionalProperties: false,
          required: [
            'global',
            'authorAction',
            'approverAction',
            'approvalRequested',
            'approvalSuccessful',
            'approvalRejected',
            'autoMergeCandidate',
          ],
          properties: {
            global: {
              type: ['array', 'null'],
              items: { type: 'string' },
              minItems: 1,
              errorMessage: {
                type: 'workflow.labels.global must be an array of strings.',
                minItems: 'workflow.labels.global must contain at least one label when configured.',
              },
            },
            authorAction: {
              type: ['string', 'null'],
              minLength: 1,
              errorMessage: {
                type: 'workflow.labels.authorAction must be a string.',
                minLength: 'workflow.labels.authorAction must not be empty when provided.',
              },
            },
            approverAction: {
              type: ['string', 'null'],
              minLength: 1,
              errorMessage: {
                type: 'workflow.labels.approverAction must be a string.',
                minLength: 'workflow.labels.approverAction must not be empty when provided.',
              },
            },
            approvalRequested: {
              type: ['array', 'null'],
              items: { type: 'string' },
              minItems: 1,
              errorMessage: {
                type: 'workflow.labels.approvalRequested must be an array of strings.',
                minItems: 'workflow.labels.approvalRequested must contain at least one label when configured.',
              },
            },
            approvalSuccessful: {
              type: ['array', 'null'],
              items: { type: 'string' },
              minItems: 1,
              errorMessage: {
                type: 'workflow.labels.approvalSuccessful must be an array of strings.',
                minItems: 'workflow.labels.approvalSuccessful must contain at least one label when configured.',
              },
            },
            approvalRejected: {
              type: ['array', 'null'],
              items: { type: 'string' },
              minItems: 1,
              errorMessage: {
                type: 'workflow.labels.approvalRejected must be an array of strings.',
                minItems: 'workflow.labels.approvalRejected must contain at least one label when configured.',
              },
            },
            autoMergeCandidate: {
              type: ['string', 'null'],
              minLength: 1,
              errorMessage: {
                type: 'workflow.labels.autoMergeCandidate must be a string.',
                minLength: 'workflow.labels.autoMergeCandidate must not be empty when provided.',
              },
            },
          },
          errorMessage: {
            required: {
              global: 'workflow.labels.global is required when workflow.labels is configured.',
              authorAction: 'workflow.labels.authorAction is required when workflow.labels is configured.',
              approverAction: 'workflow.labels.approverAction is required when workflow.labels is configured.',
              approvalRequested: 'workflow.labels.approvalRequested is required when workflow.labels is configured.',
              approvalSuccessful: 'workflow.labels.approvalSuccessful is required when workflow.labels is configured.',
              approvalRejected: 'workflow.labels.approvalRejected is required when workflow.labels is configured.',
              autoMergeCandidate: 'workflow.labels.autoMergeCandidate is required when workflow.labels is configured.',
            },
            additionalProperties: 'Only known label keys are allowed inside workflow.labels.',
          },
        },

        approvers: {
          type: ['array', 'null'],
          items: { type: 'string' },
          minItems: 1,
          errorMessage: {
            type: 'workflow.approvers must be an array of strings.',
            minItems: 'workflow.approvers must contain at least one approver when configured.',
          },
        },

        links: {
          type: ['object', 'null'],
          additionalProperties: false,
          properties: {
            docs: {
              type: ['string', 'null'],
              minLength: 1,
              errorMessage: {
                type: 'workflow.links.docs must be a string.',
                minLength: 'workflow.links.docs must not be empty when provided.',
              },
            },
          },
          errorMessage: {
            additionalProperties: "Only 'docs' is allowed inside workflow.links.",
          },
        },
      },
      errorMessage: {
        type: 'workflow must be an object when provided.',
        additionalProperties: "Only 'labels', 'approvers', 'links' and 'assignees' are allowed inside workflow.",
      },
    },
  },
  errorMessage: {
    required: {
      requests: "Property 'requests' is required in registry-bot config.",
    },
    additionalProperties: 'Additional properties are not allowed at the top level of registry-bot config.',
  },
};
