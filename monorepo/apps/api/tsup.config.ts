import { defineConfig } from 'tsup';

/**
 * Build the API into a runnable bundle.
 *
 * The service used to run through `tsx`, compiling TypeScript on every start
 * with the whole devDependency tree present in the image.
 *
 * A bundler rather than plain `tsc` because `@well-simulation/db` and `@well-simulation/dto`
 * are source-only workspace packages with no build output. `tsc` would emit
 * imports that node cannot resolve at runtime. tsup follows them into the
 * bundle, which is also how the website already consumes them through Vite.
 */
export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm'],
  target: 'node22',
  outDir: 'dist',
  sourcemap: true,
  clean: true,
  // tsup leaves `dependencies` external by default, which is right for a
  // library and wrong for a service: nothing would be in the bundle and the
  // runtime image would need the full dependency tree back.
  noExternal: [/.*/],
  // Except these. They carry native or platform-specific parts that must be
  // resolved by node at runtime, so the runtime image installs them.
  external: ['pg', 'pg-native', 'bcryptjs'],
  // Some bundled dependencies are CommonJS and call require() at runtime, which
  // does not exist in an ES module. This gives them a real one.
  banner: {
    js: [
      "import { createRequire as __createRequire } from 'node:module';",
      'const require = __createRequire(import.meta.url);',
    ].join('\n'),
  },
});
