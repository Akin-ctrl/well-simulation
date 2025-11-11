import { ContentfulStatusCode } from 'hono/utils/http-status';

export type ApiRes<T = any> = {
  data: T;
  msg?: string;
  status: ContentfulStatusCode | undefined;
  meta?: {
    limit: number;
    page: number;
    total: number;
  };
};
export interface ApiErr {
  error: string;
  msg?: string;
  status: ContentfulStatusCode | undefined;
}
