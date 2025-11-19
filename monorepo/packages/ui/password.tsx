import React from 'react';

import { Input, InputProps } from './input';
import { cn } from '@corsight/utils/cn';

export interface PasswordProps extends Omit<InputProps, 'type' | 'size'> {
  /** It is the password visibility toggle icon.  */
  visibilityToggleIcon?(visible: boolean): React.ReactNode;
  /** Override default CSS style of password show/hide toggle icon */
  visibilityToggleIconClassName?: string;
  size?: keyof typeof passwordToggleIconClasses.size;
}

const Password = React.forwardRef<HTMLInputElement, PasswordProps>(
  (
    {
      disabled,
      size = 'md',
      visibilityToggleIcon,
      visibilityToggleIconClassName,
      ...props
    },
    ref
  ) => {
    const [visible, setVisible] = React.useState(false);

    // @ts-expect-error type issue
    const { type: _, ...otherProps } = props;

    return (
      <Input
        ref={ref}
        type={visible ? 'text' : 'password'}
        size={size}
        suffix={
          <button
            tabIndex={0}
            type='button'
            className={cn(
              'flex h-full items-center whitespace-nowrap leading-normal',
              disabled && 'text-muted-foreground',
              visibilityToggleIconClassName
            )}
            onClick={() => {
              if (disabled) return false;
              setVisible((prevState) => !prevState);
              return;
            }}>
            {visibilityToggleIcon ? (
              visibilityToggleIcon(visible)
            ) : (
              <PasswordToggleIcon isVisible={visible} iconSize={size} />
            )}
          </button>
        }
        {...otherProps}
      />
    );
  }
);

Password.displayName = 'Password';

//// Password Toggle Icon

const passwordToggleIconClasses = {
  size: {
    sm: 'icon-sm',
    md: 'icon-md',
    lg: 'icon-lg',
  },
};

interface PasswordToggleIconProps {
  isVisible?: boolean;
  iconSize?: keyof typeof passwordToggleIconClasses.size;
}

function PasswordToggleIcon({ iconSize, isVisible }: PasswordToggleIconProps) {
  return (
    <svg
      xmlns='http://www.w3.org/2000/svg'
      fill='none'
      viewBox='0 0 24 24'
      strokeWidth={1.25}
      stroke='currentColor'
      className={cn(iconSize && passwordToggleIconClasses.size[iconSize])}>
      <path
        strokeLinecap='round'
        strokeLinejoin='round'
        d='M2.036 12.322a1.012 1.012 0 010-.639C3.423 7.51 7.36 4.5 12 4.5c4.638 0 8.573 3.007 9.963 7.178.07.207.07.431 0 .639C20.577 16.49 16.64 19.5 12 19.5c-4.638 0-8.573-3.007-9.963-7.178z'
      />
      <path
        strokeLinecap='round'
        strokeLinejoin='round'
        d={
          isVisible
            ? 'M15 12a3 3 0 11-6 0 3 3 0 016 0z'
            : 'M3.98 8.223A10.477 10.477 0 001.934 12C3.226 16.338 7.244 19.5 12 19.5c.993 0 1.953-.138 2.863-.395M6.228 6.228A10.45 10.45 0 0112 4.5c4.756 0 8.773 3.162 10.065 7.498a10.523 10.523 0 01-4.293 5.774M6.228 6.228L3 3m3.228 3.228l3.65 3.65m7.894 7.894L21 21m-3.228-3.228l-3.65-3.65m0 0a3 3 0 10-4.243-4.243m4.242 4.242L9.88 9.88'
        }
      />
    </svg>
  );
}

export { Password, PasswordToggleIcon };
