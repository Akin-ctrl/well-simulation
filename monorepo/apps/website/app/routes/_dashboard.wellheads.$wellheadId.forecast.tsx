import { ProjectionPage } from '../domains/twin/projection-page';
import {
  projectionAction,
  projectionLoader,
  savedResultShouldRevalidate,
} from '../domains/twin/projection-route';
export const clientLoader = projectionLoader('forecast');
export const clientAction = projectionAction('forecast');
export const shouldRevalidate = savedResultShouldRevalidate;
export default function ForecastPage() {
  return <ProjectionPage kind='forecast' />;
}
