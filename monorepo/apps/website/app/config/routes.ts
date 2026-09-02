export const routes = {
  /**
   * Every route path in one place.
   *
   * Paths are built here rather than written inline so a rename is one edit
   * rather than a search across the app.
   */

  auth: {
    login: '/login',
    register: '/register',
    forgotPassword: '/forgot-password',
    resetPassword: '/reset-password',
  },
  dashboard: {
    overview: '/',
    analytics: '/analytics',
    alarms: '/alarms',
    wellhead: (wellheadId: number | string) => `/wellheads/${wellheadId}`,
    settings: '/settings',
  },
  help: '/help',
};
