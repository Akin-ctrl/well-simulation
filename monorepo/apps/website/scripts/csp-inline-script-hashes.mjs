/**
 * Write the dashboard's nginx config with a Content Security Policy that allows
 * exactly the inline scripts in the built index.html, and nothing else.
 *
 * React Router's SPA build puts small startup scripts inside index.html. They
 * name build-specific asset files, so their text changes on every build. A CSP
 * hash ('sha256-<base64>') is a fingerprint of one script's exact text, and the
 * browser runs an inline script only when its fingerprint is listed. Hashing
 * here, after the build, keeps the policy strict. The alternative,
 * 'unsafe-inline', would let any injected script run.
 *
 * Usage: node csp-inline-script-hashes.mjs <index.html> <nginx.conf> <output>
 *
 * The build fails if the placeholder is missing or repeated, or if index.html
 * has no inline script, so a policy that blanks the page never ships.
 */
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';

const PLACEHOLDER = '__INLINE_SCRIPT_HASHES__';

// A <script> element with no src attribute. The capture is its exact text,
// which is what the browser hashes.
const INLINE_SCRIPT = /<script(?![^>]*\bsrc\s*=)[^>]*>([\s\S]*?)<\/script>/gi;

/** Return the CSP source for every non-empty inline script, in page order. */
function inlineScriptHashes(html) {
  return [...html.matchAll(INLINE_SCRIPT)]
    .map((match) => match[1])
    .filter((body) => body.trim() !== '')
    .map(
      (body) => `'sha256-${createHash('sha256').update(body, 'utf8').digest('base64')}'`
    );
}

function main(argv) {
  const [indexPath, templatePath, outputPath] = argv;
  if (!indexPath || !templatePath || !outputPath) {
    throw new Error(
      'Usage: csp-inline-script-hashes.mjs <index.html> <nginx.conf> <output>'
    );
  }

  const hashes = inlineScriptHashes(readFileSync(indexPath, 'utf8'));
  if (hashes.length === 0) {
    throw new Error(`No inline scripts found in ${indexPath}; refusing to write a CSP`);
  }

  const template = readFileSync(templatePath, 'utf8');
  const occurrences = template.split(PLACEHOLDER).length - 1;
  if (occurrences !== 1) {
    throw new Error(
      `Expected ${PLACEHOLDER} once in ${templatePath}, found ${occurrences}`
    );
  }

  writeFileSync(outputPath, template.replace(PLACEHOLDER, hashes.join(' ')));
  console.log(`Allowed ${hashes.length} inline scripts in ${outputPath}`);
}

main(process.argv.slice(2));
