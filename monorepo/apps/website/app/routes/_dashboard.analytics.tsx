import {
  Cell,
  Line,
  LineChart,
  Pie,
  PieChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { Title } from '../domains/dashboard/components/misc';

function Analytics() {
  const _data = [
    { date: 'hiuhiuhii', amount: 1000 },
    { date: 'hiuhiuhii', amount: 5000 },
    { date: 'hiuhiuhii', amount: 100 },
    { date: 'hiuhiuhii', amount: 100000 },
    { date: new Date().toISOString(), amount: 10000 },
  ];

  const activeCustomers = [
    {
      color: '#553AFE',
      name: 'Online',
      amount: 1000,
    },
    {
      color: '#01C0F6',
      name: 'Ordered',
      amount: 10000,
    },
  ];

  return (
    <div className='space-y-5'>
      <div className=''>
        <Title>Analytics</Title>
        <p className='text-fg-muted'>
          Monitor your oil performance metrics over time
        </p>
      </div>

      <div className='grid grid-cols-5 gap-4'>
        <div className=' card col-span-2'>
          <ResponsiveContainer className=' justify-center'>
            <LineChart data={_data} className=''>
              {_data.map((d, i) => (
                <ReferenceLine key={i} x={d.date} stroke='#EFEFF4' />
              ))}
              <XAxis
                dataKey='date'
                stroke='#636c76'
                tickLine={false}
                axisLine={false}
                underlineThickness={0}
                tickMargin={10}
                style={{ textAnchor: 'middle' }}
              />
              <YAxis
                dataKey='amount'
                stroke='#636c76'
                axisLine={false}
                tickLine={false}
                tickMargin={60}
                style={{ textAnchor: 'start' }}
                className='-ml-4'
                tickFormatter={(value) => value}
              />

              <Line
                type='monotoneX'
                dataKey='amount'
                stroke='#553AFE'
                strokeWidth={2}
                dot={false}
                activeDot={{
                  stroke: 'white',
                  strokeWidth: 2,
                  r: 5,
                  fill: '#636c76',
                }}
              />
              <Tooltip
                cursor={false}
                content={(props) => {
                  return (
                    <div className='bg-bgColor-black flex flex-col items-center rounded-lg px-4 py-2'>
                      <div className='flex items-center gap-2'>
                        <span className='text-fgColor-muted'>
                          {props.label}
                        </span>
                        <div className='bg-bgColor-accent-emphasis size-2 rounded-full'></div>
                      </div>

                      <p className='text-title-medium text-white'>{10000}</p>
                    </div>
                  );
                }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className=' card col-span-2'>
          <ResponsiveContainer className=' justify-center'>
            <LineChart data={_data} className=''>
              {_data.map((d, i) => (
                <ReferenceLine key={i} x={d.date} stroke='#EFEFF4' />
              ))}
              <XAxis
                dataKey='date'
                stroke='#636c76'
                tickLine={false}
                axisLine={false}
                underlineThickness={0}
                tickMargin={10}
                style={{ textAnchor: 'middle' }}
              />
              <YAxis
                dataKey='amount'
                stroke='#636c76'
                axisLine={false}
                tickLine={false}
                tickMargin={60}
                style={{ textAnchor: 'start' }}
                className='-ml-4'
                tickFormatter={(value) => value}
              />

              <Line
                type='monotoneX'
                dataKey='amount'
                stroke='#553AFE'
                strokeWidth={2}
                dot={false}
                activeDot={{
                  stroke: 'white',
                  strokeWidth: 2,
                  r: 5,
                  fill: '#636c76',
                }}
              />
              <Tooltip
                cursor={false}
                content={(props) => {
                  return (
                    <div className='bg-bgColor-black flex flex-col items-center rounded-lg px-4 py-2'>
                      <div className='flex items-center gap-2'>
                        <span className='text-fgColor-muted'>
                          {props.label}
                        </span>
                        <div className='bg-bgColor-accent-emphasis size-2 rounded-full'></div>
                      </div>

                      <p className='text-title-medium text-white'>{10000}</p>
                    </div>
                  );
                }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className='card'>
          <div className='relative h-48'>
            <div className='h-full w-full'>
              <ResponsiveContainer>
                <PieChart width={100} height={100}>
                  <Pie
                    data={activeCustomers}
                    dataKey='amount'
                    outerRadius={80}
                    innerRadius={70}
                    paddingAngle={5}
                    cornerRadius={5}>
                    {activeCustomers.map((d, i) => (
                      <Cell
                        key={`cell-${i}`}
                        fill={d.color}
                        strokeLinejoin='round'
                        strokeLinecap='round'
                      />
                    ))}
                  </Pie>
                </PieChart>
              </ResponsiveContainer>
              <div className='absolute left-0 top-0 flex h-full w-full flex-col items-center justify-center'>
                <p className='text-fgColor-muted'>Total</p>
                <p className='text-title-medium'>{10000}</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Analytics;
