import { DropletIcon, GridIcon, ThermometerIcon } from 'lucide-react';

function Overview() {
  return (
    <div className='space-y-5'>
      <div className='flex justify-evenly'>
        {[47, 2000, 1003, 40, 50000, 24].map((x) => (
          <div key={x}>
            <div className='py-3! px-10! rounded-lg card flex flex-col items-center'>
              <div className='flex justify-between items-center gap-3'>
                <GridIcon className='size-5' />
                <p>Total Wells</p>
              </div>

              <span className='text-xl font-semibold'>{x}</span>
            </div>
          </div>
        ))}
      </div>

      <div className='border-t border-t-neutral-300' />

      <div className='grid gap-6 grid-cols-2 px-8'>
        {[1, 2, 3, 4].map((b) => (
          <div className='card gap-2 flex flex-col' key={b}>
            <div className='flex gap-4'>
              <div className='flex-1'>
                <p>Board #1</p>
                <p className='text-sm'>Shared by Well A,B,C,D</p>
              </div>

              <div className='text-sm'>
                <div className='flex items-center gap-0'>
                  <ThermometerIcon className='size-3' />
                  <p>87%</p>
                </div>
                <div className='flex items-center gap-0'>
                  <ThermometerIcon className='size-3' />
                  <p>87%</p>
                </div>
              </div>

              <div className='text-sm'>
                <div className='flex items-center gap-0'>
                  <DropletIcon className='size-3' />
                  <p>87%</p>
                </div>
                <div className='flex items-center gap-0'>
                  <DropletIcon className='size-3' />
                  <p>87%</p>
                </div>
              </div>
            </div>

            <div className='border-t border-t-neutral-300' />

            <div className='grid gap-2 grid-cols-2 px-2'>
              {[1, 2, 3, 4].map((d) => (
                <div key={d} className='bg-neutral-200 p-2 rounded-lg h-40'>
                  <p>Well A</p>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export default Overview;
