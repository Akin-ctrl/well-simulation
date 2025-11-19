import { Input } from '@corsight/ui/input';
import { Button } from '@corsight/ui/button';
import { Link, redirect, useFetcher } from 'react-router';
import { routes } from '../config/routes';
import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { loginSchema } from '@corsight/dto/req/auth';
import { type SToType } from '@corsight/dto';
import type { Route } from './+types/_auth.login';
import { client, fetchFn } from '../utils/api';
import type { ApiError } from '@corsight/dto/error';
import { toast } from '@corsight/ui/toast';
import { Password } from '@corsight/ui/password';

export async function clientAction(args: Route.ClientActionArgs) {
  const formData = (await args.request.json()) as SToType<typeof loginSchema>;
  try {
    await fetchFn(client.auth.login.$post({ json: formData }));
    return redirect(routes.dashboard.overview);
  } catch (err) {
    const error = err as ApiError;
    const msg = error.msg ?? 'Login failed';
    toast.error(msg);
    return { error: true, msg: msg };
  }
}

export default function Login() {
  const form = useForm<SToType<typeof loginSchema>>({
    resolver: zodResolver(loginSchema),
    defaultValues: {
      emailOrUsername: '',
      password: '',
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
          {...form.register('emailOrUsername')}
          placeholder='Enter email or username'
          error={form.getFieldState('emailOrUsername').invalid}
        />
        <Password
          {...form.register('password')}
          placeholder='Enter password'
          error={form.getFieldState('password').invalid}
        />

        <div className='flex justify-end mt-2'>
          <Link to={routes.auth.register} className='text-blue-600 text-sm'>
            Don't have an account?
          </Link>
        </div>

        <Button type='submit' disabled={fetcher.state === 'submitting'}>
          Sign In
        </Button>
      </form>
    </div>
  );
}
