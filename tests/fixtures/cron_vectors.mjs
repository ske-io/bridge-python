// Writes tests/fixtures/cron_vectors.json: whether each expression is due in
// each minute and time zone, as decided by the Node.js bridge
// (@ske-io/bridge), whose cron rules ske_bridge.schedule.expression ports.
// test_schedule_expression.py checks every vector. Regenerate after a change
// to the rules, from the Node.js bridge's built cron module:
//
//     node tests/fixtures/cron_vectors.mjs <bridge>/dist/esm/schedule/cron.js > tests/fixtures/cron_vectors.json
import { pathToFileURL } from 'node:url';

const { isDue, parseCron } = await import(pathToFileURL(process.argv[2]).href);

const zones = ['UTC', 'Europe/Paris', 'America/New_York', 'Australia/Lord_Howe'];

const expressions = [
  '* * * * *', '*/15 * * * *', '5/20 * * * *', '45 */2 * * *', '0-10/5 1,2 * jan-jun,oct-dec *',
  '30 1 * * *', '30 2 * * *', '0 2 * * *', '0,30 1-3 * * *', '15 3 * * *', '0 9 * * *', '30 13 * * mon-fri',
  '0 12 * * sat,sun', '0 0 * * *', '0 0 1 * *', '0 0 */2 * 1', '0 0 1,15 * mon', '0 0 13 * fri', '0 0 29 feb *',
  '0 0 31 * *', '30 13 1 */3 *', '0 0 */1 * 1', '0 0 1 * 0-6', '0 0 * * 7', '30 13 5/7 * *', '30 13 */10 * sat,sun',
  '@hourly', '@daily', '@weekly', '@monthly', '@yearly', '@weekdays', '@weekends',
  '*/30 * * * * *', '0 30 2 * * *',
];

const MINUTE = 60_000;
const minutes = new Set();
const span = (from, to, step) => {
  for (let t = Date.parse(from); t < Date.parse(to); t += step * MINUTE) minutes.add(t / MINUTE);
};
// A day either side of each daylight saving change, every 15 minutes:
// Paris and New York shift an hour, Lord Howe half an hour.
for (const day of ['2026-03-29', '2026-10-25', '2026-03-08', '2026-11-01', '2026-04-05', '2026-10-04']) {
  const at = Date.parse(`${day}T00:00:00Z`);
  span(new Date(at - 36 * 60 * MINUTE).toISOString(), new Date(at + 36 * 60 * MINUTE).toISOString(), 15);
}
// Every day of 2028, a leap year, at two times of day.
for (let t = Date.parse('2028-01-01T00:00:00Z'); t < Date.parse('2029-01-01T00:00:00Z'); t += 24 * 60 * MINUTE) {
  minutes.add(t / MINUTE);
  minutes.add(t / MINUTE + 13 * 60 + 30);
}
// One hour minute by minute.
span('2026-07-14T09:00:00Z', '2026-07-14T10:00:00Z', 1);

const sorted = [...minutes].sort((a, b) => a - b);

function hex(bits) {
  let out = '';
  for (let i = 0; i < bits.length; i += 4) {
    const nibble = bits.slice(i, i + 4).padEnd(4, '0');
    out += parseInt(nibble, 2).toString(16);
  }
  return out;
}

const cases = [];
for (const expression of expressions) {
  const cron = parseCron(expression);
  for (const timeZone of zones) {
    const bits = sorted.map((minute) => (isDue(cron, new Date(minute * MINUTE), { timeZone }) ? '1' : '0')).join('');
    cases.push({ expression, zone: timeZone, due: hex(bits) });
  }
}

process.stdout.write(JSON.stringify({ minutes: sorted, cases }) + '\n');
