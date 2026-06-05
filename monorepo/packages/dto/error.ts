export class ApiError extends Error {
  public msg: string;
  public data?: unknown;
  public originalError: unknown;
  public statusCode?: number;
  public issues?: unknown[];

  constructor(
    originalError: unknown,
    msg?: string,
    statusCode?: number,
    data?: unknown,
    issues?: unknown[]
  ) {
    const originalMessage =
      originalError &&
      typeof originalError === 'object' &&
      'message' in originalError &&
      typeof originalError.message === 'string'
        ? originalError.message
        : undefined;
    const originalApiMessage =
      originalError &&
      typeof originalError === 'object' &&
      'msg' in originalError &&
      typeof originalError.msg === 'string'
        ? originalError.msg
        : undefined;
    const resolvedMsg =
      msg || originalApiMessage || originalMessage || 'Something went wrong!';

    super(resolvedMsg);

    this.name = 'ApiError';
    this.msg = resolvedMsg;
    this.originalError = originalError;
    this.statusCode = statusCode;
    this.data = data;
    this.issues = issues;

    Object.setPrototypeOf(this, ApiError.prototype);
  }
}
