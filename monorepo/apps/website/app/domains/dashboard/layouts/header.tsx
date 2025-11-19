import { BellIcon, MoonIcon, SunIcon, UserRoundIcon } from 'lucide-react';
import { Link } from 'react-router';

function Header() {
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

        <Link
          to='/profile'
          className=' flex items-center gap-2 border border-border-default pl-1! pr-3! py-1! rounded-full!'>
          <div className='p-1.5 rounded-full bg-neutral-200'>
            <UserRoundIcon className='size-4' />
          </div>

          <span>Lamar</span>
        </Link>
      </div>
    </header>
  );
}

export default Header;
