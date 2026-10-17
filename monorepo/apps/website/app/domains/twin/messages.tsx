import { Link } from 'react-router';
import { routes } from '../../config/routes';
import type { errorState } from './presentation';

export function RequestMessage({
  error,
  historyUrl,
}: {
  error: ReturnType<typeof errorState>;
  historyUrl?: string;
}) {
  return (
    <div className='twin-message' role='alert'>
      <p>{error.message}</p>
      {error.status === 401 && <Link to={routes.auth.login}>Sign in again</Link>}
      {historyUrl &&
        error.status !== 400 &&
        error.status !== 401 &&
        error.status !== 403 &&
        error.status !== 404 &&
        error.status !== 410 &&
        error.status !== 429 && (
          <p>
            A lost response can leave the outcome unknown.{' '}
            <Link to={historyUrl}>Check saved runs</Link> before submitting again.
          </p>
        )}
    </div>
  );
}
