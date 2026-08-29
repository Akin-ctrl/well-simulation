import pluginReact from 'eslint-plugin-react';
import pluginReactHooks from 'eslint-plugin-react-hooks';
import globals from 'globals';

import { config as libraryConfig } from './library.js';

/**
 * Shared ESLint configuration for React component packages.
 *
 * Builds on `library` rather than `base` so findings are errors that can gate
 * CI, instead of warnings downgraded by eslint-plugin-only-warn.
 *
 * @type {import("eslint").Linter.Config[]}
 */
export const config = [
  ...libraryConfig,
  pluginReact.configs.flat.recommended,
  {
    languageOptions: {
      ...pluginReact.configs.flat.recommended.languageOptions,
      globals: { ...globals.browser },
    },
    plugins: {
      'react-hooks': pluginReactHooks,
    },
    settings: { react: { version: 'detect' } },
    rules: {
      ...pluginReactHooks.configs.recommended.rules,
      // Unnecessary with the modern JSX transform.
      'react/react-in-jsx-scope': 'off',
      'react/prop-types': 'off',
    },
  },
];
