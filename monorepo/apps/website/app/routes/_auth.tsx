import { Outlet } from 'react-router';
import Logo from '~/assets/svg/brand/logo.svg?react';

/** Original text layout, with no external photograph or tracking request. */
export default function AuthLayout() {
  return (
    <main className='twin-auth twin-workspace'>
      <section className='twin-auth-intro' aria-label='Well Simulation'>
        <div className='twin-inline'>
          <Logo aria-hidden='true' className='size-8' />
          <strong>Well Simulation</strong>
        </div>
        <h2>
          Follow a well.
          <br />
          Understand a change.
        </h2>
        <p>
          Check readings and alarms, explore forecasts, and compare proposed control
          changes.
        </p>
        <ol>
          <li>Choose a well</li>
          <li>Inspect its recent behaviour</li>
          <li>Compare a scenario</li>
        </ol>
      </section>
      <div className='twin-auth-form'>
        <Outlet />
      </div>
    </main>
  );
}
