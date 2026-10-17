import { NavLink } from 'react-router';
import { menuItems } from '../components/menu-items';

/** The configured menu is flat; every icon retains its name when collapsed. */
export function SidebarMenu({ expanded }: { expanded: boolean }) {
  return (
    <nav
      id='desktop-navigation'
      aria-label='Main navigation'
      className='grid gap-2 px-3'
    >
      {menuItems.map((item) =>
        item.disabled ? (
          <span
            key={item.name}
            className='flex min-h-11 items-center gap-3 px-2 text-slate-600'
            title={`${item.name} unavailable`}
          >
            <span aria-hidden='true' className='[&>svg]:size-5'>
              {item.icon}
            </span>
            <span className={expanded ? 'text-sm' : 'sr-only'}>
              {item.name} (unavailable)
            </span>
          </span>
        ) : (
          <NavLink
            key={item.name}
            to={item.href}
            end
            aria-label={item.name}
            className={({ isActive }) =>
              `flex min-h-11 items-center gap-3 rounded-md px-2 text-sm ${isActive ? 'bg-blue-50 text-blue-900 font-semibold' : 'text-slate-700 hover:bg-slate-100'}`
            }
          >
            <span aria-hidden='true' className='[&>svg]:size-5'>
              {item.icon}
            </span>
            {expanded && <span>{item.name}</span>}
          </NavLink>
        )
      )}
    </nav>
  );
}
