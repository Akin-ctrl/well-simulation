import { Input } from '@corsight/ui/input';
import { Button } from '@corsight/ui/button';
import { Link, redirect, useFetcher } from 'react-router';
import { routes } from '../config/routes';
import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { registerSchema } from '@corsight/dto/req/auth';
import { type SToType } from '@corsight/dto';
import type { Route } from './+types/_auth.register';
import { client, fetchFn } from '../utils/api';
import type { ApiError } from '@corsight/dto/error';
import { toast } from '@corsight/ui/toast';
import { Password } from '@corsight/ui/password';

export async function clientAction(args: Route.ClientActionArgs) {
  const formData = (await args.request.json()) as SToType<
    typeof registerSchema
  >;
  try {
    await fetchFn(client.auth.register.$post({ json: formData }));
    return redirect(routes.dashboard.overview);
  } catch (err) {
    const error = err as ApiError;
    const msg = error.msg ?? 'Registration failed';
    toast.error(msg);
    return { error: true, msg: msg };
  }
}

export default function Register() {
  const form = useForm<SToType<typeof registerSchema>>({
    resolver: zodResolver(registerSchema),
    defaultValues: {
      username: '',
      email: '',
      password: '',
      confirmPassword: '',
    },
  });

  const fetcher = useFetcher();

  return (
    <div className='flex flex-col'>
      <form
        onSubmit={form.handleSubmit((data) => {
          fetcher.submit(data, {
            method: 'POST',
            encType: 'application/json',
          });
        })}
        className='max-w-md flex flex-col gap-3 mt-32 mx-auto w-full'>
        <Input
          {...form.register('username')}
          placeholder='Enter username'
          error={form.getFieldState('username').invalid}
        />

        <Input
          {...form.register('email')}
          placeholder='Enter email'
          error={form.getFieldState('email').invalid}
          type='email'
        />
        <Password
          {...form.register('password')}
          placeholder='Enter password'
          error={form.getFieldState('password').invalid}
        />
        <Password
          {...form.register('confirmPassword')}
          placeholder='Confirm password'
          error={form.getFieldState('confirmPassword').invalid}
        />

        <div className='flex justify-end mt-2'>
          <Link to={routes.auth.login} className='text-blue-600 text-sm'>
            Already have an account?
          </Link>
        </div>

        <Button type='submit' disabled={fetcher.state === 'submitting'}>
          Sign Up
        </Button>
      </form>
    </div>
  );
}
