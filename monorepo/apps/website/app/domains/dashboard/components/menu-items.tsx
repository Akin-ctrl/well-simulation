import {
  AlertTriangleIcon,
  ChartBarIcon,
  CogIcon,
  HelpCircleIcon,
  LayoutDashboardIcon,
} from 'lucide-react';

import { routes } from '~/config/routes';

/**
 * The sidebar entries, in display order.
 *
 * Order is meaningful: it is the order they appear in the sidebar.
 */

export type MenuItem = {
  name: string;
  href?: string;
  icon?: React.ReactNode;
  disabled?: boolean;
  new?: boolean;
  dropdownItems?: MenuItem[];
};

// this controle the sidebar menu items. positioning matters here and rearranging them means rearranging the sidebar menu items.
export const menuItems = [
  {
    name: 'Overview',
    href: routes.dashboard.overview,
    icon: <LayoutDashboardIcon />,
  },
  {
    name: 'Alarms',
    icon: <AlertTriangleIcon />,
    href: routes.dashboard.alarms,
  },
  {
    name: 'Analytics',
    icon: <ChartBarIcon />,
    href: routes.dashboard.analytics,
  },
  {
    name: 'Settings',
    href: routes.dashboard.settings,
    icon: <CogIcon />,
    disabled: true,
  },

  {
    name: 'Help',
    href: routes.help,
    icon: <HelpCircleIcon />,
    disabled: true,
  },
];
