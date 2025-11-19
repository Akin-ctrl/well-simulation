import React from 'react';
import * as CollapsiblePrimitive from '@radix-ui/react-collapsible';
import { cn } from '@corsight/utils/cn';

const Collapsible = CollapsiblePrimitive.Root;

const CollapsibleTrigger = CollapsiblePrimitive.CollapsibleTrigger;

// const CollapsibleContent = CollapsiblePrimitive.CollapsibleContent;

const CollapsibleContent = React.forwardRef<
  React.ElementRef<typeof CollapsiblePrimitive.CollapsibleContent>,
  React.ComponentPropsWithoutRef<typeof CollapsiblePrimitive.CollapsibleContent>
>(({ className, children, ...props }, ref) => (
  <CollapsiblePrimitive.CollapsibleContent
    ref={ref}
    className='data-[state=closed]:animate-collapsible-up data-[state=open]:animate-collapsible-down text-body-medium overflow-hidden transition-all'
    {...props}>
    <div className={cn('pb-4 pt-0', className)}>{children}</div>
  </CollapsiblePrimitive.CollapsibleContent>
));

CollapsibleContent.displayName = 'CollapsibleContent';

export { Collapsible, CollapsibleTrigger, CollapsibleContent };
