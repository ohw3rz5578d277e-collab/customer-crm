import assert from 'node:assert/strict';
import fs from 'node:fs';

const bridge = fs.readFileSync(
  '.github/workflows/dispatch-member-production-runtime-readiness-from-issue.yml',
  'utf8'
);

assert.match(
  bridge,
  /if \[\[ "\$COMMAND_BODY" =~ \$command_re \]\]; then\n\s+expected_sha="\$\{BASH_REMATCH\[1\]\}"/,
  'runtime-readiness bridge must capture BASH_REMATCH SHA immediately after the successful command regex match'
);

const shaCaptureIndex = bridge.indexOf('expected_sha="${BASH_REMATCH[1]}"');
const ownerCommentValidationIndex = bridge.indexOf('[[ "$OWNER_COMMENT_ID" =~ ^[0-9]+$ ]]');

assert.ok(shaCaptureIndex >= 0, 'runtime-readiness bridge SHA capture missing');
assert.ok(
  ownerCommentValidationIndex > shaCaptureIndex,
  'Owner comment regex must not overwrite BASH_REMATCH before the command SHA is captured'
);

console.log('MEMBER_PRODUCTION_RUNTIME_READINESS_BASH_REMATCH_ADJACENCY=PASS');
