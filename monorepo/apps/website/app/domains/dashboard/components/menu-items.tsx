import {
  ActivitySquareIcon,
  BotIcon,
  ChartBarIcon,
  CogIcon,
  HelpCircleIcon,
  Laptop2Icon,
  LayersIcon,
  LayoutDashboardIcon,
  LinkIcon,
  PackageCheckIcon,
  PrinterIcon,
  ScrollTextIcon,
  SettingsIcon,
  ShoppingBagIcon,
} from 'lucide-react';

import { routes } from '~/config/routes';

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
    name: 'Analytics',
    icon: <ChartBarIcon />,
    href: routes.dashboard.analytics,
  },
  {
    name: 'Settings',
    href: routes.dashboard.settings,
    icon: <CogIcon />,
  },

  {
    name: 'Help',
    href: routes.help,
    icon: <HelpCircleIcon />,
    disabled: true,
  },
];
