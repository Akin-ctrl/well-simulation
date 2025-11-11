import {
  ActivitySquareIcon,
  BotIcon,
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
    href: '/',
    icon: <LayoutDashboardIcon />,
  },
  {
    name: 'Analytics',
    icon: <ScrollTextIcon />,
    href: '/',
  },
  {
    name: 'Settings',
    href: '/',
    icon: <CogIcon />,
  },

  {
    name: 'Help',
    href: '/',
    icon: <HelpCircleIcon />,
    disabled: true,
  },
];
