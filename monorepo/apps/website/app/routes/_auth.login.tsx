import { Input } from '@corsight/ui/input';
import { Button } from '@corsight/ui/button';
import { Link, useFetcher } from 'react-router';
import { routes } from '../config/routes';
import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { loginSchema } from '@corsight/dto/req/auth';
import { type SToType } from '@corsight/dto';
import type { Route } from './+types/_auth.login';

export async function clientAction(args: Route.ClientActionArgs) {
  const formData = await args.request.json();
  try {
  } catch (error) {}
}

export default function Login() {
  const form = useForm<SToType<typeof loginSchema>>({
    resolver: zodResolver(loginSchema),
    defaultValues: {
      email: '',
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
          {...form.register('email')}
          placeholder='Enter email'
          error={form.getFieldState('email').invalid}
        />
        <Input
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