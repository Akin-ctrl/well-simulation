import { useEffect } from 'react';
import { Link, useFetcher, useLoaderData } from 'react-router';
import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { registerSchema, MIN_PASSWORD_LENGTH } from '@well-simulation/dto/req/auth';
import { DEFAULT_ROLE, hasCapability } from '@well-simulation/dto/auth/roles';
import { type SToType } from '@well-simulation/dto';
import { ApiError } from '@well-simulation/dto/error';
import type { Route } from './+types/_auth.register';
import type { CurrentUserData } from './_dashboard';
import { routes } from '../config/routes';
import { client, fetchFn } from '../utils/api';

/** The API remains the authority; this read prevents an unusable public form. */
export async function clientLoader() {
  try {
    const response = await fetchFn<CurrentUserData>(client.auth.me.$get());
    return {
      allowed: hasCapability(response.data.user.role ?? DEFAULT_ROLE, 'user:manage'),
    };
  } catch (error) {
    if (error instanceof ApiError && [401, 403].includes(error.statusCode ?? 0))
      return { allowed: false };
    throw error;
  }
}

export async function clientAction({ request }: Route.ClientActionArgs) {
  const parsed = registerSchema.safeParse(await request.json());
  if (!parsed.success)
    return { created: false, message: 'Check the account details and try again.' };
  try {
    await fetchFn(client.auth.register.$post({ json: parsed.data }));
    return { created: true, message: `Account created for ${parsed.data.username}.` };
  } catch (error) {
    const status = error instanceof ApiError ? error.statusCode : 0;
    return {
      created: false,
      message:
        status === 403
          ? 'Your account cannot create users. Ask an administrator to review your access.'
          : status === 401
            ? 'Your session has ended. Sign in again.'
            : status === 400
              ? 'Check the account details and try again.'
              : status === 429
                ? 'Too many requests. Wait a minute, then try again.'
                : 'We could not confirm account creation. Check existing accounts with your administrator before trying again.',
    };
  }
}

/** Administrator creation keeps the existing USER default and never signs out its creator. */
export default function Register() {
  const { allowed } = useLoaderData<typeof clientLoader>();
  const form = useForm<SToType<typeof registerSchema>>({
    resolver: zodResolver(registerSchema),
    defaultValues: { username: '', email: '', password: '', confirmPassword: '' },
  });
  const fetcher = useFetcher<typeof clientAction>();
  const pending = fetcher.state !== 'idle';
  const { reset } = form;
  useEffect(() => {
    document.title = 'Create an account | Well Simulation';
  }, []);
  useEffect(() => {
    if (!pending && fetcher.data) {
      if (fetcher.data.created) reset();
      document.getElementById('account-result')?.focus();
    }
  }, [fetcher.data, pending, reset]);
  return (
    <div>
      <h1>Create an account</h1>
      {!allowed ? (
        <div className='twin-stack mt-4'>
          <p>
            An administrator creates dashboard accounts. Ask your administrator if you
            need access.
          </p>
          <Link className='twin-button' to={routes.auth.login}>
            Sign in
          </Link>
        </div>
      ) : (
        <>
          <p className='twin-muted'>
            Create a dashboard reader. This account starts with the USER role.
          </p>
          <form
            noValidate
            className='twin-form mt-6'
            onSubmit={form.handleSubmit((data) => {
              if (!pending)
                fetcher.submit(data, { method: 'POST', encType: 'application/json' });
            })}
          >
            {fetcher.data && !pending && (
              <p
                id='account-result'
                tabIndex={-1}
                role='status'
                className={`twin-message mb-4 ${fetcher.data.created ? 'twin-ok' : 'twin-error'}`}
              >
                {fetcher.data.message}
              </p>
            )}
            {(
              [
                ['username', 'Username', 'text', 'off'],
                ['email', 'Email', 'email', 'off'],
                ['password', 'Password', 'password', 'new-password'],
                ['confirmPassword', 'Confirm password', 'password', 'new-password'],
              ] as const
            ).map(([name, label, type, autocomplete]) => (
              <div className='twin-field' key={name}>
                <label htmlFor={`account-${name}`}>{label}</label>
                <input
                  {...form.register(name)}
                  id={`account-${name}`}
                  type={type}
                  autoComplete={autocomplete}
                  required
                  aria-invalid={Boolean(form.formState.errors[name]) || undefined}
                  aria-describedby={
                    form.formState.errors[name]
                      ? `account-${name}-error`
                      : name === 'password'
                        ? 'account-password-help'
                        : undefined
                  }
                />
                {name === 'password' && (
                  <p id='account-password-help' className='twin-caption'>
                    Use at least {MIN_PASSWORD_LENGTH} characters. You can paste a
                    password from a password manager.
                  </p>
                )}
                {form.formState.errors[name] && (
                  <p id={`account-${name}-error`} className='twin-field-error'>
                    {form.formState.errors[name]?.message}
                  </p>
                )}
              </div>
            ))}
            <button
              className='twin-button twin-primary w-full'
              type='submit'
              aria-disabled={pending}
            >
              {pending ? 'Creating account…' : 'Create account'}
            </button>
          </form>
          <Link className='twin-button mt-4' to={routes.dashboard.overview}>
            Back to the dashboard
          </Link>
        </>
      )}
    </div>
  );
}
