// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const { execFileSync } = require('node:child_process');
const { readFileSync, writeFileSync } = require('node:fs');
const { checklistItems, skipCheckboxState, skipLabel } = require('./nightly-merge.cjs');

const repository = 'GreptimeTeam/greptimedb';
const oldNightly = 'merge nightly';
const oldDocs = 'docs-not-required';

function api(path, args = [], data) {
  const input = data === undefined ? undefined : JSON.stringify(data);
  return execFileSync('gh', ['api', `repos/${repository}/${path}`, ...args,
    ...(input === undefined ? [] : ['--input', '-'])], { encoding: 'utf8', input });
}

function migrateBody(body, hold) {
  let updated = body;
  // Work backwards so each replacement keeps the offsets of earlier items valid.
  for (const item of [...checklistItems(body)].reverse()) {
    const old = /^Merge during the nightly window\b(?:[ \t]*<!-- nightly-merge -->)?/i.exec(item.text);
    if (old) {
      const following = updated.slice(item.end).replace(/^(?:[ \t]*\r?\n)+ {0,3}<!--[\s\S]*?-->/, comment =>
        comment.replace(/^Uncheck to opt out and remove the `merge nightly` label; this does not enable immediate merging\.$/m,
          'Check to opt out. To resume, uncheck the box and remove any manually applied `skip nightly merge` label.'));
      updated = updated.slice(0, item.index) + (hold ? '[x]' : '[ ]') + item.space +
        'Skip nightly merge' + item.text.slice(old[0].length) + following;
    }
  }
  return updated;
}

function changesFor(entry, pr) {
  const labels = new Set(pr.labels.map(label => label.name));
  const targetsMain = pr.base.ref === 'main';
  const migrateNightly = targetsMain && entry.base === 'main';
  const hold = migrateNightly && !entry.optedIn;
  const body = migrateNightly ? migrateBody(pr.body, hold) : pr.body;
  const checkbox = targetsMain ? skipCheckboxState(body) : null;
  const labeled = labels.has(skipLabel);
  const obsolete = migrateNightly ? [oldNightly, oldDocs] : [oldDocs];
  return {
    body,
    // A converted checked box gets a bot-owned label from its edit event.
    addHold: hold && checkbox === null && !labeled,
    remove: obsolete.filter(label => labels.has(label)),
    waitingForSync: checkbox === true && !labeled,
    extraHold: migrateNightly && entry.optedIn && (checkbox === true || labeled),
    needsChoice: targetsMain && !migrateNightly && checkbox === null && !labeled,
  };
}

function openBefore(mergedAt) {
  const rows = api('pulls?state=open&sort=created&direction=asc&per_page=100', ['--paginate', '--jq',
    `.[] | select(.created_at < ${JSON.stringify(mergedAt)}) | {number, base: .base.ref, optedIn: any(.labels[]; .name == "${oldNightly}")} | @json`]);
  return rows.trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
}

function main([mode, file, rolloutPR]) {
  if (!file || !['snapshot', 'apply', 'verify'].includes(mode) || (mode === 'snapshot' && !/^[1-9]\d*$/.test(rolloutPR))) {
    throw new Error('Usage: node .github/scripts/migrate-pr-labels.cjs snapshot SNAPSHOT.json ROLLOUT_PR | apply|verify SNAPSHOT.json');
  }
  if (mode === 'snapshot') {
    const { merged_at: mergedAt, base } = JSON.parse(api(`pulls/${rolloutPR}`));
    if (!mergedAt || base.ref !== 'main') throw new Error(`Rollout PR #${rolloutPR} must be merged into main`);
    const prs = openBefore(mergedAt);
    writeFileSync(file, JSON.stringify({ repository, rolloutPR, mergedAt, prs }, null, 2) + '\n', { flag: 'wx' });
    console.log(`Saved ${prs.length} open PRs to ${file}`);
    return;
  }
  const snapshot = JSON.parse(readFileSync(file, 'utf8'));
  if (snapshot.repository !== repository || !snapshot.mergedAt || !Array.isArray(snapshot.prs) || !snapshot.prs.every(entry =>
    Number.isSafeInteger(entry.number) && entry.number > 0 && typeof entry.base === 'string' && typeof entry.optedIn === 'boolean')) {
    throw new Error('Invalid migration snapshot');
  }
  let open = 0;
  if (mode === 'verify') {
    const captured = new Set(snapshot.prs.map(entry => entry.number));
    for (const pr of openBefore(snapshot.mergedAt)) {
      if (!captured.has(pr.number)) {
        console.error(`#${pr.number}: open before rollout but missing from the snapshot`);
        process.exitCode = 1;
      }
    }
  }
  for (const entry of snapshot.prs) {
    const number = entry.number;
    const pr = JSON.parse(api(`pulls/${number}`));
    if (pr.state !== 'open') continue;
    open++;
    const { body, addHold, remove, waitingForSync, extraHold, needsChoice } = changesFor(entry, pr);
    if (mode === 'apply') {
      if (body !== pr.body) api(`pulls/${number}`, ['--method', 'PATCH', '--silent'], { body });
      if (addHold) api(`issues/${number}/labels`, ['--method', 'POST', '--silent'], { labels: [skipLabel] });
      for (const label of remove) {
        api(`issues/${number}/labels/${encodeURIComponent(label)}`, ['--method', 'DELETE', '--silent']);
      }
    } else {
      const pending = remove.map(label => `remove ${label}`);
      if (body !== pr.body) pending.push('convert legacy checkbox');
      if (addHold || waitingForSync) pending.push(`missing ${skipLabel}`);
      if (extraHold) pending.push('review opt-out added since snapshot');
      if (needsChoice) pending.push(`record a nightly preference after retargeting from ${entry.base} to main`);
      if (pending.length) {
        console.error(`#${number}: ${pending.join(', ')}`);
        process.exitCode = 1;
      }
    }
  }
  console.log(`${mode === 'apply' ? 'Applied migration to' : 'Checked'} ${open} open PRs from ${file}`);
}

if (require.main === module) main(process.argv.slice(2));
module.exports = { changesFor };
