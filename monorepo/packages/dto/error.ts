export class ApiError extends Error {
  public msg: string;
  public data?: any;
  public originalError: any;
  public statusCode?: number;
  public issues?: any[];

  constructor(
    originalError: any,
    msg?: string,
    statusCode?: number,
    data?: any,
    issues?: any[]
  ) {
    const resolvedMsg =
      msg ||
      originalError?.msg ||
      originalError?.message ||
      'Something went wrong!';

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
