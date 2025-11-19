import React from 'react';
import { ChevronRightIcon } from 'lucide-react';
import { Link } from 'react-router';
import { routes } from '~/config/routes';

import { SidebarMenu } from './sidebar-menu';
import { cn } from '@corsight/utils/cn';
import Logo from '~/assets/svg/brand/logo.svg?react';

export function DesktopNav() {
  const [expanded, setExpanded] = React.useState(true);

  return (
    <aside
      className={cn(
        'bg-bgColor-default border-r-border-default sticky inset-y-0 z-2 hidden h-screen',
        'border-r transition-all ease-in-out xl:block',
        expanded ? 'w-[300px]' : 'w-16'
      )}>
      <button
        className={
          'btn btn-primary absolute! -right-2 top-4 z-2 inline-grid aspect-square size-7 rounded-full!'
        }
        onClick={() => setExpanded(!expanded)}>
        <ChevronRightIcon
          style={{
            rotate: expanded ? '-180deg' : '0deg',
            transition: 'rotate 500ms cubic-bezier(0.4, 0, 0.2, 1)',
            width: '16px',
            height: '16px',
          }}
        />
      </button>

      <div className='flex flex-col h-full gap-8 overflow-x-hidden'>
        <div className='border-b border-b-border-default h-[57px] flex items-center'>
          <Link
            prefetch='viewport'
            className={cn(
              'flex gap-1 items-center',
              expanded ? 'pl-5' : 'pl-2'
            )}
            to={routes.dashboard.overview}>
            <Logo className='size-7' />
            {expanded && (
              <span className='text-lg font-semibold text-[#023BB5] transition-all duration-300'>
                Corsight
              </span>
            )}
          </Link>
        </div>

        <SidebarMenu expanded={expanded} />
      </div>
    </aside>
  );
}
