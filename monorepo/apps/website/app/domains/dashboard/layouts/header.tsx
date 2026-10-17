import {
  DEFAULT_ROLE,
  hasCapability,
  type Role,
} from '@well-simulation/dto/auth/roles';
import { menuItems } from '../components/menu-items';
import { LogOutIcon, UserRoundIcon } from 'lucide-react';
import { useState } from 'react';
import { NavLink, useNavigate } from 'react-router';
import { Button } from '@well-simulation/ui/button';
import { toast } from '@well-simulation/ui/toast';
import { routes } from '~/config/routes';
import { client, fetchFn } from '~/utils/api';

/**
 * The dashboard header: who is signed in, and how to sign out.
 */

export type HeaderUser = {
  role?: Role | null;
  firstName?: string | null;
  lastName?: string | null;
  userName?: string | null;
  email?: string | null;
};

function displayName(user: HeaderUser) {
  const fullName = [user.firstName, user.lastName].filter(Boolean).join(' ');
  return fullName || user.userName || user.email || 'Operator';
}

function Header({ user }: { user: HeaderUser }) {
  const navigate = useNavigate();
  const [isLoggingOut, setIsLoggingOut] = useState(false);

  async function handleLogout() {
    setIsLoggingOut(true);
    try {
      await fetchFn(client.auth.logout.$post());
      navigate(routes.auth.login, { replace: true });
    } catch {
      toast.error('Logout failed. Please try again.');
    } finally {
      setIsLoggingOut(false);
    }
  }

  return (
    <header className='dashboard-header z-10 border-b-neutral-300 sticky inset-x-0 top-0 border-b backdrop-blur-md bg-white/80 p-0!'>
      <div className='flex justify-end container items-center gap-2 px-4 min-h-14 flex-wrap py-2'>
        {hasCapability(user.role ?? DEFAULT_ROLE, 'user:manage') && (
          <NavLink className='twin-button' to={routes.auth.register}>
            Create account
          </NavLink>
        )}
        <div className='min-w-0 flex items-center gap-2 border border-border-default pl-1! pr-3! py-1! rounded-full!'>
          <div className='p-1.5 rounded-full bg-neutral-200'>
            <UserRoundIcon aria-hidden='true' className='size-4' />
          </div>

          <span className='max-w-40 truncate' title={displayName(user)}>
            {displayName(user)}
          </span>
        </div>

        <Button
          type='button'
          variant='outline'
          size='sm'
          isLoading={isLoggingOut}
          onClick={handleLogout}
        >
          <LogOutIcon className='size-4' />
          Logout
        </Button>
      </div>
      <nav
        aria-label='Main navigation'
        className='xl:hidden flex flex-wrap gap-6 px-4 pb-3 text-sm text-blue-800'
      >
        {menuItems
          .filter((item) => !item.disabled)
          .map((item) => (
            <NavLink key={item.href} to={item.href} end>
              {item.name}
            </NavLink>
          ))}
      </nav>
    </header>
  );
}

export default Header;
