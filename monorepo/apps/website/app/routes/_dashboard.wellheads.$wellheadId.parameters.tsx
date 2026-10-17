import { OperationPage } from '../domains/twin/operation-page';
import { operationAction, operationLoader } from '../domains/twin/operation-route';
export const clientLoader = operationLoader('parameter');
export const clientAction = operationAction('parameter');
export default function ParametersPage() {
  return <OperationPage kind='parameter' />;
}
