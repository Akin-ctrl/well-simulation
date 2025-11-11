import * as React from 'react';
import { Slot } from '@radix-ui/react-slot';
import { cva, type VariantProps } from 'class-variance-authority';

import { cn } from '@corsight/utils/cn';
import { Spinner } from './spinner';

const buttonVariants = cva(
  'btn',

  {
    variants: {
      variant: {
        primary: 'btn-primary',
        destructive: 'btn-danger',
        outline: 'btn-outline',
        secondary: 'btn-outline',
        outlineSecondary: 'btn-outline-secondary',
        inactive: 'btn-inactive',
        ghost: 'btn-ghost',
        link: 'btn-link',
      },
      size: {
        md: 'btn-md',
        sm: 'btn-sm',
        xs: 'btn-xs',
        lg: 'btn-lg',
        icon: 'size-8',
        xl: 'px-5 h-12 leading-10',
      },
      icon: {
        true: 'aspect-square w-auto p-0',
        false: '',
      },
    },
    defaultVariants: {
      variant: 'primary',
      size: 'md',
      icon: false,
    },
  }
);

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {
  asChild?: boolean;
  isLoading?: boolean;
}

const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      className,
      variant,
      size,
      icon,
      asChild = false,
      isLoading,
      disabled,
      children,
      ...props
    },
    ref
  ) => {
    const Comp = asChild ? Slot : 'button';
    return (
      <Comp
        className={cn(buttonVariants({ variant, size, icon, className }))}
        ref={ref}
        disabled={isLoading || disabled}
        {...props}>
        {isLoading ? (
          <>
            {/* trick to have exact btn width when btn is loading */}
            <span className='invisible opacity-0'>{children}</span>
            <span
              className={cn(
                'absolute inset-0 flex h-full w-full items-center justify-center'
              )}>
              <Spinner className='scale-75' />
            </span>
          </>
        ) : (
          <>{children}</>
        )}
      </Comp>
    );
  }
);
Button.displayName = 'Button';

export { Button, buttonVariants };
