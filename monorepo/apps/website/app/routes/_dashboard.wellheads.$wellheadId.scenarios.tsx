import { ProjectionPage } from '../domains/twin/projection-page';
import {
  projectionAction,
  projectionLoader,
  savedResultShouldRevalidate,
} from '../domains/twin/projection-route';
export const clientLoader = projectionLoader('scenario');
export const clientAction = projectionAction('scenario');
export const shouldRevalidate = savedResultShouldRevalidate;
export default function ScenarioPage() {
  return <ProjectionPage kind='scenario' />;
}
