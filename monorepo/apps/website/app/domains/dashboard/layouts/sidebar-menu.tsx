import React, { Fragment } from 'react';
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from '@corsight/ui/collapsible';
import { ScrollArea } from '@corsight/ui/scroll-area';
import { ArrowUpRightIcon, ChevronDownIcon } from 'lucide-react';
import { Link, useLocation } from 'react-router';

import { cn } from '@corsight/utils/cn';
import { menuItems, type MenuItem } from '../components/menu-items';

export function SidebarMenu({ expanded }: { expanded: boolean }) {
  const [openCollapsible, setOpenCollapsible] = React.useState<number | null>(
    null
  );

  const firstSlice = React.useMemo(() => menuItems.slice(0, 8), []);
  const secondSlice = React.useMemo(() => menuItems.slice(8, 11), []);
  const thirdSlice = React.useMemo(() => menuItems.slice(11), []);

  const handleToggle = (index: number) => {
    setOpenCollapsible((prev) => (prev === index ? null : index));
  };

  return (
    <ScrollArea className='[&>div>div]:h-full'>
      <div className={cn('flex h-full flex-col gap-4', expanded ? '' : '')}>
        {RenderLinks(firstSlice, openCollapsible, handleToggle, expanded)}
        {RenderLinks(secondSlice, openCollapsible, handleToggle, expanded)}
        <div className='mt-auto space-y-3'>
          {RenderLinks(thirdSlice, openCollapsible, handleToggle, expanded)}
        </div>
      </div>
    </ScrollArea>
  );
}

function RenderLinks(
  menuItems: MenuItem[],
  openCollapsible: number | null,
  handleToggle: (index: number) => void,
  expanded: boolean
) {
  const businessLink = '/';

  const location = useLocation();

  return menuItems.map((item, index) => {
    function getPathAfterBusinessId(path: string) {
      const match = path.match(/^\/business\/\d+\/(.+)$/);
      return '/' + (match ? match[1] : '');
    }

    const isActive = getPathAfterBusinessId(location.pathname) === item.href;
    const isDropdownOpen = openCollapsible === index;

    return (
      <Fragment key={index}>
        {item?.dropdownItems ? (
          <Collapsible
            open={isDropdownOpen}
            onOpenChange={() => handleToggle(index)}>
            <div
              className={cn(
                'card flex items-center gap-3 mx-3 p-3!',
                expanded ? '' : ''
              )}>
              <CollapsibleTrigger
                disabled={item.disabled}
                className={cn(
                  'btn control-transparent text-fgColor-white p-0 [&>svg]:size-4',
                  isDropdownOpen
                    ? 'bg-control-transparent-bgColor-selected'
                    : ''
                )}>
                {item.icon}
              </CollapsibleTrigger>

              {expanded && (
                <CollapsibleTrigger
                  disabled={item?.disabled}
                  className={cn(
                    'button button-md control-transparent w-full justify-start pr-1 [&>svg]:data-[state=closed]:rotate-0 [&>svg]:data-[state=open]:rotate-90 items-center flex font-semibold',
                    isDropdownOpen
                      ? 'bg-control-transparent-bgColor-selected'
                      : 'text-fgColor-muted'
                  )}>
                  {item.name}
                  <ChevronDownIcon
                    className='ml-auto size-4.5'
                    style={{
                      transition: 'transform 150ms',
                    }}
                  />
                </CollapsibleTrigger>
              )}
            </div>

            <CollapsibleContent className='grid h-full gap-1 py-2'>
              {item?.dropdownItems?.map((dropdownItem, idx) => {
                const isChildActive = location.pathname.includes(
                  dropdownItem?.href as string
                );

                return (
                  <Link
                    prefetch='viewport'
                    key={idx}
                    to={
                      dropdownItem.href?.startsWith('http')
                        ? dropdownItem.href
                        : businessLink + dropdownItem.href!
                    }
                    state={{
                      prevLink: businessLink + dropdownItem.href!,
                    }}
                    target={dropdownItem?.new ? '_blank' : '_self'}
                    className={cn(
                      '!text-body-small !rounded-8px button button-md control-transparent ml-24 mr-1 flex items-center justify-start',
                      isChildActive
                        ? 'bg-control-transparent-bgColor-selected font-semibold'
                        : 'text-fgColor-muted hover:text-fgColor-default transition-colors',
                      dropdownItem?.disabled
                        ? 'text-fgColor-disabled pointer-events-none'
                        : ''
                    )}>
                    {dropdownItem.name}
                    {dropdownItem?.new ? (
                      <ArrowUpRightIcon className='ml-auto size-4' />
                    ) : null}
                  </Link>
                );
              })}
            </CollapsibleContent>
          </Collapsible>
        ) : (
          <div
            className={cn(
              'card flex items-center gap-3 mx-3 p-3!',
              expanded ? '' : ''
            )}>
            <Link
              prefetch='viewport'
              className={cn(
                'button control-transparent button-md text-fgColor-white hover:text- aspect-square w-auto p-0 [&>svg]:size-4',
                isActive ? 'bg-control-transparent-bgColor-selected' : '',
                item?.disabled ? 'pointer-events-none opacity-50' : ''
              )}
              to={
                item.href?.startsWith('http')
                  ? item.href
                  : businessLink + item.href!
              }
              state={{
                prevLink: 'fff',
              }}
              target={item?.new ? '_blank' : '_self'}>
              {item.icon}
            </Link>

            {expanded && (
              <Link
                to={
                  item.href?.startsWith('http')
                    ? item.href
                    : businessLink + item.href!
                }
                prefetch='viewport'
                state={{
                  prevLink: location.pathname,
                }}
                target={item?.new ? '_blank' : '_self'}
                className={cn(
                  'button button-md control-transparent w-full justify-start [&>svg]:data-[state=closed]:rotate-0 [&>svg]:data-[state=open]:rotate-90',
                  isActive
                    ? 'bg-control-transparent-bgColor-selected font-semibold'
                    : 'text-fgColor-muted',
                  item?.disabled ? 'pointer-events-none opacity-50' : ''
                )}>
                {item.name}
                {item?.new ? (
                  <ArrowUpRightIcon className='ml-auto size-4' />
                ) : null}
              </Link>
            )}
          </div>
        )}
      </Fragment>
    );
  });
}
