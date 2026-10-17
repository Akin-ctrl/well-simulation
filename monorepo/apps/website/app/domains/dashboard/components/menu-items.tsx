import {
  AlertTriangleIcon,
  ChartBarIcon,
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
  href: string;
  icon?: React.ReactNode;
  disabled?: boolean;
  new?: boolean;
  dropdownItems?: MenuItem[];
};

// Desktop and compact navigation share this order.
export const menuItems: MenuItem[] = [
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
    name: 'Guide',
    href: routes.help,
    icon: <HelpCircleIcon />,
  },
];
