import { useEffect, useState } from 'react';
import { redirect, useFetcher } from 'react-router';
import { loginSchema } from '@well-simulation/dto/req/auth';
import { ApiError } from '@well-simulation/dto/error';
import type { Route } from './+types/_auth.login';
import { routes } from '../config/routes';
import { client, fetchFn } from '../utils/api';

type LoginErrors = { emailOrUsername?: string; password?: string };

/** Validate at the route boundary as well as in the visible form. */
export async function clientAction({ request }: Route.ClientActionArgs) {
  const form = await request.formData();
  const parsed = loginSchema.safeParse(Object.fromEntries(form));
  if (!parsed.success) return { message: 'Check your account details and try again.' };
  try {
    await fetchFn(client.auth.login.$post({ json: parsed.data }));
    return redirect(routes.dashboard.overview);
  } catch (error) {
    const status = error instanceof ApiError ? error.statusCode : 0;
    return {
      message:
        status === 401
          ? 'The email, username or password did not match. Check your details and try again.'
          : status === 429
            ? 'Too many sign-in attempts. Wait a minute, then try again.'
            : 'We could not sign you in. Check your connection and try again.',
    };
  }
}

/** Visible labels, inline errors and autocomplete support password managers. */
export default function Login() {
  const fetcher = useFetcher<typeof clientAction>();
  const [visible, setVisible] = useState(false);
  const [errors, setErrors] = useState<LoginErrors>({});
  const pending = fetcher.state !== 'idle';
  useEffect(() => {
    document.title = 'Sign in | Well Simulation';
  }, []);
  useEffect(() => {
    if (!pending && fetcher.data?.message)
      document.getElementById('login-error')?.focus();
  }, [fetcher.data, pending]);
  return (
    <div>
      <h1>Sign in</h1>
      <p className='twin-muted'>Use the account your administrator gave you.</p>
      <fetcher.Form
        method='post'
        noValidate
        className='twin-form mt-6'
        onSubmit={(event) => {
          if (pending) {
            event.preventDefault();
            return;
          }
          const parsed = loginSchema.safeParse(
            Object.fromEntries(new FormData(event.currentTarget))
          );
          const next: LoginErrors = {};
          if (!parsed.success) {
            event.preventDefault();
            for (const issue of parsed.error.issues) {
              if (issue.path[0] === 'emailOrUsername')
                next.emailOrUsername =
                  'Enter your email or username (at least 3 characters).';
              if (issue.path[0] === 'password') next.password = issue.message;
            }
            requestAnimationFrame(() =>
              document.getElementById('login-errors')?.focus()
            );
          }
          setErrors(next);
        }}
      >
        {Object.keys(errors).length > 0 && (
          <div id='login-errors' tabIndex={-1} className='twin-message twin-error mb-4'>
            <h2>Check your inputs</h2>
            <ul>
              {Object.entries(errors).map(([key, message]) => (
                <li key={key}>
                  <a
                    href={`#login-${key}`}
                    onClick={(event) => {
                      event.preventDefault();
                      document.getElementById(`login-${key}`)?.focus();
                    }}
                  >
                    {message}
                  </a>
                </li>
              ))}
            </ul>
          </div>
        )}
        {fetcher.data?.message && !pending && (
          <p
            id='login-error'
            tabIndex={-1}
            role='alert'
            className='twin-message twin-error mb-4'
          >
            {fetcher.data.message}
          </p>
        )}
        <div className='twin-field'>
          <label htmlFor='login-emailOrUsername'>Email or username</label>
          <input
            id='login-emailOrUsername'
            name='emailOrUsername'
            autoComplete='username'
            autoCapitalize='none'
            spellCheck={false}
            placeholder='Enter email or username'
            required
            aria-invalid={Boolean(errors.emailOrUsername) || undefined}
            aria-describedby={
              errors.emailOrUsername ? 'login-username-error' : undefined
            }
          />
          {errors.emailOrUsername && (
            <p id='login-username-error' className='twin-field-error'>
              {errors.emailOrUsername}
            </p>
          )}
        </div>
        <div className='twin-field'>
          <label htmlFor='login-password'>Password</label>
          <input
            id='login-password'
            name='password'
            type={visible ? 'text' : 'password'}
            autoComplete='current-password'
            required
            aria-invalid={Boolean(errors.password) || undefined}
            aria-describedby={errors.password ? 'login-password-error' : undefined}
          />
          {errors.password && (
            <p id='login-password-error' className='twin-field-error'>
              {errors.password}
            </p>
          )}
          <button
            type='button'
            className='twin-button mt-2'
            aria-controls='login-password'
            aria-pressed={visible}
            onClick={() => setVisible(!visible)}
          >
            {visible ? 'Hide password' : 'Show password'}
          </button>
        </div>
        <button
          type='submit'
          className='twin-button twin-primary w-full'
          aria-disabled={pending}
        >
          {pending ? 'Signing in…' : 'Sign in'}
        </button>
        <p role='status' className='sr-only'>
          {pending ? 'Signing in…' : ''}
        </p>
        <p className='twin-muted mt-4'>
          Ask your administrator if you need an account or help with your password.
        </p>
      </fetcher.Form>
    </div>
  );
}
