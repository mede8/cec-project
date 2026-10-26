// k6 load test for the Temperature Observability REST API
// Verify test (30 s at 5 requests/s, checks whether the script works)
// Stress test (~8 min, ramps to 90 then 180 requests/s)
import http from 'k6/http';
import { check } from 'k6';


const BASE = __ENV.BASE_URL || 'http://localhost:3003';

const verify = {
    executor: 'constant-arrival-rate',
    rate: 5, timeUnit: '1s', duration: '30s',
    preAllocatedVUs: 5,
};

const stress = {
    executor: 'ramping-arrival-rate',
    startRate: 10,
    timeUnit: '1s',
    preAllocatedVUs: 50,  // virtual users
    maxVUs: 400,
    stages: [
        { target: 90, duration: '1m' },
        { target: 90, duration: '3m' },  // is it stable at 90 requests/s?
        { target: 180, duration: '1m' },  // push to double
        { target: 180, duration: '2m' },  // where does it start to break?
        { target: 0, duration: '30s' },
    ],
};

export const options = {
    scenarios: { api: __ENV.VERIFY ? verify : stress},
    // pass/ fail criteria
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
    return { ids };
}

// one iteration
export default function (data) {
    const id = data.ids[Math.floor(Math.random() * data.ids.length)];
    let res;
    if (Math.random() < 0.5) {
        res = http.get(`${BASE}/temperature/out-of-range?experiment-id=${id}`,
            { tags: { name: 'out-of-range' } }
        );
    } else {
        const end = Date.now() / 1000;
        res = http.get(`${BASE}/temperature?experiment-id=${id}&start-time=0&end-time=${end}`,
            { tags: { name: 'temperature' } }
        );
        check(res, {
            'status is 200': (r) => r.status === 200,
            'body is a JSON list': (r) => { try { return Array.isArray(r.json()); } catch (_) { return false; } },
        });

    }
}
