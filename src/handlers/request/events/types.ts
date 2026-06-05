export type MaybePromise<T> = T | Promise<T>;

export type RequestEventHandler<ContextType> = (context: ContextType) => MaybePromise<void>;
