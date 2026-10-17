import type { ReactNode } from 'react';
import { Link } from 'react-router';
import { routes } from '../../../config/routes';

/** Optional instructions stay in reading order and work without a modal or tour. */
export function PageHelp({ title, children }: { title: string; children: ReactNode }) {
  return (
    <details className='twin-help'>
      <summary>{title}</summary>
      <div className='twin-help-body'>{children}</div>
    </details>
  );
}

/** Explain the first useful task without choosing a well or submitting a run. */
export function GettingStarted() {
  return (
    <PageHelp title='New here? Start with one well'>
      <ol>
        <li>Open a well below and check its readings and data age.</li>
        <li>Open Forecast to see what may happen with the controls unchanged.</li>
        <li>
          Use Scenarios to compare a proposed change, then revisit it in Saved runs.
        </li>
      </ol>
      <Link to={routes.help}>Read the guide</Link>
    </PageHelp>
  );
}
