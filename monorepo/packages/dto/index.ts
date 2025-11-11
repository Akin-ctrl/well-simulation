import { z, ZodTypeAny } from 'zod';

/**
 * Convert Zod schema to Type
 * - e.g: const schema = z.string()
 * SToType<typeof schema> // string
 *
 */
export type SToType<T extends ZodTypeAny> = z.infer<T>;

// type Data<T> = T extends {createdAt: Date | string; updatedAt: Date | string}
//   ? T
//   : undefined;

export interface ReturnObj<T = undefined> {
  status: string;
  message: string;
  data: T;
  meta?: {
    page: number;
    nextPage: number | null;
    prevPage: null | number;
    offset: number;
    totalItems: number;
    totalPages: number;
    itemCount: number;
  };
}
