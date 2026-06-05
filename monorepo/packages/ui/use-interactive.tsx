import { useCallback, useState } from 'react';
import type React from 'react';

type InputFocusEvent = React.FocusEvent<HTMLInputElement>;
type InputMouseEvent = React.MouseEvent<HTMLDivElement>;

type InteractiveEventTypes = {
  readOnly?: boolean;
  onFocus?: (e: InputFocusEvent) => void;
  onBlur?: (e: InputFocusEvent) => void;
  onMouseEnter?: (e: InputMouseEvent) => void;
  onMouseLeave?: (e: InputMouseEvent) => void;
};

export function useInteractiveEvent({
  readOnly,
  onFocus,
  onBlur,
  onMouseEnter,
  onMouseLeave,
}: InteractiveEventTypes) {
  const [isFocus, setIsFocus] = useState(false);
  const [isHover, setIsHover] = useState(false);

  const handleOnFocus = useCallback(
    (e: InputFocusEvent) => {
      if (readOnly === true) return;
      setIsFocus((prevState) => !prevState);
      onFocus?.(e);
      return;
    },
    [readOnly, onFocus]
  );

  const handleOnBlur = useCallback(
    (e: InputFocusEvent) => {
      if (readOnly === true) return;
      setIsFocus(() => false);
      onBlur?.(e);
      return;
    },
    [readOnly, onBlur]
  );

  const handleOnMouseEnter = useCallback(
    (e: InputMouseEvent) => {
      if (readOnly === true) return;
      setIsHover(() => true);
      onMouseEnter?.(e);
      return;
    },
    [readOnly, onMouseEnter]
  );

  const handleOnMouseLeave = useCallback(
    (e: InputMouseEvent) => {
      if (readOnly === true) return;
      setIsHover(() => false);
      onMouseLeave?.(e);
      return;
    },
    [readOnly, onMouseLeave]
  );

  return {
    isFocus,
    isHover,
    handleOnFocus,
    handleOnBlur,
    handleOnMouseEnter,
    handleOnMouseLeave,
  };
}
