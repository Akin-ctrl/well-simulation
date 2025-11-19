// This is called a "splat route" and as it's in the root `/app/routes/`
// directory, it's a catchall. If no other routes match, this one will and we
// can know that the user is hitting a URL that doesn't exist. By throwing a
// 404 from the loader, we can force the error boundary to render which will
// ensure the user gets the right status code and we can display a nicer error
// message for them than the Remix and/or browser default.

export async function clientLoader() {
  throw new Response('Not found', { status: 404 });
}

export default function NotFound() {
  // due to the loader, this component will never be rendered, but we'll return
  // the error boundary just in case.
  return (
    // <GeneralErrorBoundary
    //   statusHandlers={{
    //     401: () => <Navigate to={routes.auth.login} />,
    //     405: () => '405',
    //     500: () => '500',
    //   }}
    //   unexpectedErrorHandler={() => <ErrorScreen />}
    // />
    <></>
  );
}
