import { redirect, useLoaderData, useNavigation, useOutlet } from 'react-router';
import { DesktopNav } from '../domains/dashboard/layouts/sidebar';
import Header from '../domains/dashboard/layouts/header';
import { cn } from '@corsight/utils/cn';
import { motion } from 'motion/react';
import { client, fetchFn } from '../utils/api';
import { routes } from '../config/routes';

type DashboardUser = {
  firstName?: string | null;
  lastName?: string | null;
  userName?: string | null;
  email?: string | null;
};

type CurrentUserData = {
  user: DashboardUser;
};

export async function clientLoader() {
  try {
    const response = await fetchFn<CurrentUserData>(client.auth.me.$get());
    return { user: response.data.user };
  } catch {
    return redirect(routes.auth.login);
  }
}

export default function DashboardLayout() {
  const outlet = useOutlet();
  const data = useLoaderData() as CurrentUserData;

  const navigation = useNavigation();
  const isNavigating = Boolean(navigation.location);

  return (
    <div className='relative flex w-full min-h-dvh xl:flex'>
      <DesktopNav />

      <div className='min-h-dvh grow h-full'>
        <Header user={data.user} />
        <motion.main
          className={cn(
            'ease-[cubic-bezier(0.645, 0.045, 0.355, 1)] mx-auto flex justify-center gap-4 transition-opacity duration-150',
            'container',
            isNavigating ? 'opacity-50' : 'opacity-100'
          )}>
          <motion.div className='px-5 grow pb-5 pt-8'>{outlet}</motion.div>
        </motion.main>
      </div>
    </div>
  );
}
