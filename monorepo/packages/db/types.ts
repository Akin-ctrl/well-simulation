import {wallets} from './schemas/wallet';
import {tvPackages} from './schemas/services';
import {transactions} from './schemas/transaction';
import {customers} from './schemas/customer';
import {coupons} from './schemas/coupon';

export type Customer = DateToString<typeof customers.$inferSelect>;
export type User = Pick<
  DateToString<typeof customers.$inferSelect>,
  | 'firstName'
  | 'lastName'
  | 'email'
  | 'phoneNumber'
  | 'emailVerified'
  | 'bio'
  | 'avatar'
  | 'userName'
>;
export type Customer_ = typeof customers.$inferSelect;
export type Wallet = DateToString<typeof wallets.$inferSelect>;
export type Wallet_ = typeof wallets.$inferSelect;
export type Transaction = DateToString<typeof transactions.$inferSelect>;
export type Transaction_ = typeof transactions.$inferSelect;
export type TvPackage_ = typeof tvPackages.$inferSelect;

export type DateToString<T> = {
  [K in keyof T]: T[K] extends Date
    ? Date | string
    : T[K] extends Date | null
      ? Date | string | null
      : T[K] extends Date | undefined
        ? Date | string | undefined
        : T[K] extends (infer U)[]
          ? DateToString<U>[]
          : T[K] extends object
            ? DateToString<T[K]>
            : T[K];
};

export interface GameHistory {
  ts: string;
  reward: number;
}

export type Coupons = DateToString<typeof coupons.$inferSelect>;
