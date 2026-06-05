import * as React from 'react';
import { cva } from 'class-variance-authority';
import type { VariantProps } from 'class-variance-authority';
import { isValidElementType } from 'react-is';

import { cn } from '@corsight/utils/cn';
import { useInteractiveEvent } from './use-interactive';

const inputVariants = cva('control control-field-default', {
  variants: {
    size: {
      md: 'control-md',
      sm: 'control-sm',
      xs: 'h-7 px-2  text-xs  leading-6',
      lg: 'control-lg',
      xl: 'px-5 text-body-medium h-10 leading-9',
    },
  },
  defaultVariants: {
    size: 'md',
  },
});

const inputFieldStyles = {
  base: 'control-field-default w-full border-0  px-0.5 focus:outline-none focus:ring-0 bg-transparent',
  reset:
    '[&::-webkit-search-cancel-button]:auto [&::-ms-clear]:hidden [&::-ms-reveal]:hidden [&::-webkit-inner-spin-button]:m-0 [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:m-0 [&::-webkit-outer-spin-button]:appearance-none',
  disabled: 'control-field-disabled',
  clearable:
    '[&:placeholder-shown~.input-clear-btn]:opacity-0 [&:placeholder-shown~.input-clear-btn]:invisible [&:not(:placeholder-shown)~.input-clear-btn]:opacity-100 [&:not(:placeholder-shown)~.input-clear-btn]:visible',
  // prefix: {
  //   size: {
  //     sm: 'ps-1.5',
  //     default: 'ps-2',
  //     lg: 'ps-3.5',
  //     xl: 'ps-4',
  //   },
  // },
  // suffix: {
  //   size: {
  //     sm: 'pe-1.5',
  //     default: 'pe-2.5',
  //     lg: 'pe-3.5',
  //     xl: 'pe-4',
  //   },
  // },
};

export const errorStyle = 'control-field-error';

export interface InputProps
  extends Omit<
      React.InputHTMLAttributes<HTMLInputElement>,
      'size' | 'prefix' | 'suffix'
    >,
    VariantProps<typeof inputVariants> {
  /** This Input component only support these types */
  // type?:
  //   | 'text'
  //   | 'email'
  //   | 'number'
  //   | 'tel'
  //   | 'search'
  //   | 'url'
  //   | 'time'
  //   | 'date'
  //   | 'week'
  //   | 'month'
  //   | 'datetime-local';
  error?: boolean;
  prefix?: React.ElementType | React.ReactNode;
  prefixClassName?: string;
  suffix?: React.ElementType | React.ReactNode;
  suffixClassName?: string;
}

const Input = React.forwardRef<HTMLInputElement, InputProps>(
  (
    {
      className,
      size = 'md',
      prefix: PrefixComponent,
      suffix: SuffixComponent,
      error,
      placeholder,
      disabled,
      type,
      readOnly,
      onBlur,
      onFocus,
      prefixClassName,
      suffixClassName,
      ...props
    },
    ref
  ) => {
    const {
      isFocus,
      isHover,
      handleOnBlur,
      handleOnFocus,
      handleOnMouseEnter,
      handleOnMouseLeave,
    } = useInteractiveEvent({
      readOnly,
      onBlur,
      onFocus,
    });

    return (
      <div
        data-focus={isFocus}
        data-hover={isHover}
        onMouseEnter={handleOnMouseEnter}
        onMouseLeave={handleOnMouseLeave}
        className={cn(
          inputVariants({ size, className }),
          isHover && 'is-hover', // must have is-hover class based on mouse enter
          disabled && 'control-field-disabled',
          isFocus && 'is-focus', // must have is-focus class based on onFocus event
          error && errorStyle,
          SuffixComponent && 'pe-1.5'
        )}>
        {PrefixComponent ? (
          <span
            className={cn(
              'prefix whitespace-nowrap leading-normal',
              prefixClassName
            )}>
            {typeof PrefixComponent !== 'string' &&
            isValidElementType(PrefixComponent) ? (
              <PrefixComponent className={cn(`icon-${size}`)} />
            ) : (
              PrefixComponent
            )}
          </span>
        ) : null}
        <input
          ref={ref}
          disabled={disabled}
          placeholder={placeholder}
          onBlur={handleOnBlur}
          onFocus={handleOnFocus}
          readOnly={readOnly}
          type={type}
          className={cn(
            inputFieldStyles.base,
            inputFieldStyles.reset,
            // it's important we are using placeholder-shown pseudo class to control input clear icon btn
            !placeholder && 'placeholder:opacity-0'
            // disabled && inputFieldStyles.disabled
            // prefix &&
            //   inputFieldStyles.prefix.size[
            //     size as keyof typeof inputFieldStyles.prefix.size
            //   ],
            // suffix &&
            //   inputFieldStyles.suffix.size[
            //     size as keyof typeof inputFieldStyles.suffix.size
            //   ]
          )}
          style={{ fontSize: 'inherit' }}
          {...props}
        />
        {SuffixComponent ? (
          <span
            className={cn(
              'suffix inline-grid place-content-center whitespace-nowrap',
              suffixClassName
            )}>
            {typeof SuffixComponent !== 'string' &&
            isValidElementType(SuffixComponent) ? (
              <SuffixComponent className={cn(`icon-${size}`)} />
            ) : (
              SuffixComponent
            )}
          </span>
        ) : null}
      </div>
    );
  }
);
Input.displayName = 'Input';

export { Input, inputVariants, inputFieldStyles };
