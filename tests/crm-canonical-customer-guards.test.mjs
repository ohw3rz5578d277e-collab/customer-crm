import assert from 'node:assert/strict';
import {isFormalLineUserId} from '../src/crm-canonical-customer-guards.mjs';

assert.equal(isFormalLineUserId('U1234567890abcdef1234'), true, 'formal LINE ID should pass');
assert.equal(isFormalLineUserId('26000123'), false, 'Customer ID must not pass as LINE ID');
assert.equal(isFormalLineUserId('reservation-123'), false, 'reservation synthetic ID must not pass as LINE ID');
assert.equal(isFormalLineUserId(''), false, 'empty must not pass as LINE ID');

console.log('crm-canonical-customer-guards offline tests passed');
