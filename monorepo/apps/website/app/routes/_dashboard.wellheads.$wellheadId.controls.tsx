import { OperationPage } from '../domains/twin/operation-page';
import { operationAction, operationLoader } from '../domains/twin/operation-route';
export const clientLoader = operationLoader('control');
export const clientAction = operationAction('control');
export default function ControlsPage() {
  return <OperationPage kind='control' />;
}
