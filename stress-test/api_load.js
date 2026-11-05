// k6 load test for the Temperature Observability REST API
// -e VERIFY=1     30 s at 5 requests/s: does the script work?
// default (step)  staircase 200 -> 400 -> 600 -> 800 -> 1000 requests/s, 1 min hold per step
//                 (~10 min): shows at which rate the API breaks
// -e MODE=hold    30 s ramp, then 1000 requests/s for 3 min: does it HOLD the target?
// reference: https://blog.hobosuit.com/arkade-load-testing-with-k6/

import http from 'k6/http';
import { check } from 'k6';


const BASE = __ENV.BASE_URL || 'http://localhost:3003';

const verify = {
    executor: 'constant-arrival-rate',
    rate: 5, timeUnit: '1s', duration: '30s',
    preAllocatedVUs: 5,
};

// Open model: k6 starts `target` requests per second whether or not earlier ones
// have finished. If all VUs are busy waiting, k6 counts a dropped_iteration.
const step = (rate) => [{ target: rate, duration: '30s' }, { target: rate, duration: '1m' }];

const stress = {
    executor: 'ramping-arrival-rate',
    startRate: 50,
    timeUnit: '1s',
    preAllocatedVUs: 200,  // virtual users
    maxVUs: 1500,
    stages: __ENV.MODE === 'hold'
        ? [{ target: 1000, duration: '30s' }, { target: 1000, duration: '3m' }, { target: 0, duration: '15s' }]
        : [...step(200), ...step(400), ...step(600), ...step(800), ...step(1000),
           { target: 0, duration: '15s' }],
};

export const options = {
    scenarios: { api: __ENV.VERIFY ? verify : stress },
    // pass/ fail criteria
    setupTimeout: '2m',
    thresholds: {
        http_req_failed: ['rate<0.01'],  // < 1% errors overall
        'http_req_duration{name:temperature}': ['p(95)<300'],  // 95% under 300 ms per endpoint
        'http_req_duration{name:out-of-range}': ['p(95)<300'],
        checks: ['rate>0.99'],  // > 99% of responses well-formed
    },
};

// setup
export function setup() {
    const res = http.get(`${BASE}/experiments?limit=500`);
    const ids = res.json().map((e) => e.experiment_id);
    if (ids.length === 0) throw new Error('No experiments in the database - run the producer first');

    const now = Date.now() / 1000;
    const exps = [];
    for (const id of ids) {
        const pts = http.get(`${BASE}/temperature?experiment-id=${id}&start-time=0&end-time=${now}`, { tags: { name: 'setup' } }).json();
        if (Array.isArray(pts) && pts.length > 1) {
            exps.push({ id, first: pts[0].timestamp, last: pts[pts.length - 1].timestamp });
        }
    }
    if (exps.length === 0) throw new Error('No experiments with stored measurements yet');
    return { exps };
}

// one iteration
export default function (data) {
    if (__ENV.PING) {
        const r = http.get(`${BASE}/health/live`, { tags: { name: 'ping' } });
        check(r, { 'status is 200': (x) => x.status === 200 });
        return;
    }
    const e = data.exps[Math.floor(Math.random() * data.exps.length)];
    let res;
    if (Math.random() < 0.5) {
        res = http.get(`${BASE}/temperature/out-of-range?experiment-id=${e.id}`,
            { tags: { name: 'out-of-range' } }
        );
    } else {
        const start = e.first + Math.random() * Math.max(0, e.last - e.first - 10);
        const end = start + 2 + Math.random() * 8;
        res = http.get(`${BASE}/temperature?experiment-id=${e.id}&start-time=${start}&end-time=${end}`,
            { tags: { name: 'temperature' } }
        );
    }
    // checked for BOTH endpoints (before, only /temperature was checked)
    check(res, {
        'status is 200': (r) => r.status === 200,
        'body is a JSON list': (r) => { try { return Array.isArray(r.json()); } catch (_) { return false; } },
    });
}