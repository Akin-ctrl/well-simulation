import { useOutlet } from 'react-router';
import authBg from '~/assets/images/auth/auth-bg.webp';
import Logo from '~/assets/svg/brand/logo.svg?react';

export default function AuthLayout() {
  const outlet = useOutlet();
  return (
    <div className='relative flex w-full h-screen'>
      <div className='flex-1 container mx-auto p-10'>
        <div className='flex flex-col justify-center h-10/12'>
          <div className='flex items-center mx-auto w-fit gap-2'>
            <Logo className='size-7' />

            <span className='text-[26px] font-medium text-[#023BB5]'>
              Corsight
            </span>
          </div>

          {outlet}
        </div>
      </div>

      <div
        className='bg-center bg-cover h-full flex flex-col justify-end gap-6 flex-1 p-10 text-white'
        style={{ backgroundImage: `url(${authBg})` }}>
        <div className='w-3/4 mx-auto drop-shadow-lg bg-black/20 rounded-xl p-4'>
          <h3 className='font-semibold text-3xl text-center'>
            Your Scada, Smarter and Simpler
          </h3>
          <h5 className='font-medium text-center text-lg'>
            Manage leads, track performance, and grow your business - all in one
            powerful yet intuitive. platform
          </h5>
        </div>
      </div>
    </div>
  );
}
