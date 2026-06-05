import { BellIcon, LogOutIcon, MoonIcon, SunIcon, UserRoundIcon } from 'lucide-react';
import { useState } from 'react';
import { useNavigate } from 'react-router';
import { Button } from '@corsight/ui/button';
import { toast } from '@corsight/ui/toast';
import { routes } from '~/config/routes';
import { client, fetchFn } from '~/utils/api';

export type HeaderUser = {
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
    <header className='border-b-neutral-300 sticky inset-x-0 top-0 border-b backdrop-blur-md bg-white/80 p-0!'>
      <div className='flex justify-end container items-center gap-4 px-4 h-14'>
        <BellIcon className='size-5' />

        <div className='gap-1 hidden'>
          <button>
            <MoonIcon className='size-5' />
          </button>

          <button className=''>
            <SunIcon className='size-5' />
          </button>
        </div>

        <div
          className=' flex items-center gap-2 border border-border-default pl-1! pr-3! py-1! rounded-full!'>
          <div className='p-1.5 rounded-full bg-neutral-200'>
            <UserRoundIcon className='size-4' />
          </div>

          <span>{displayName(user)}</span>
        </div>

        <Button
          type='button'
          variant='outline'
          size='sm'
          isLoading={isLoggingOut}
          onClick={handleLogout}>
          <LogOutIcon className='size-4' />
          Logout
        </Button>
      </div>
    </header>
  );
}

export default Header;
