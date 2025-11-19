import { cn } from '@corsight/utils/cn';
import type { ReactNode } from 'react';

export const Title = ({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) => {
  return (
    <>
      <title>{`${children} | Corsight`}</title>
      <h4 className={cn('font-semibold italic', className)}>{children}</h4>
    </>
  );
};
