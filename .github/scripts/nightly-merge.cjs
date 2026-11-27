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

const skipLabel = 'skip nightly merge';

// Exclude quoted, commented and fenced examples from both sync and migration.
function* checklistItems(body) {
  let fence = '';
  let inComment = false;
  const listIndents = [0];
  for (const match of (body || '').matchAll(/^[^\r\n]+/gm)) {
    const line = match[0];
    if (!line.trim()) continue;
    const block = line.includes('\t') ? line.split('\t').reduce((before, after) =>
      before + ' '.repeat(4 - before.length % 4) + after) : line;
    const indent = /^ */.exec(block)[0].length;
    // Leaving a list item also ends its fence, even without a closing delimiter.
    while (indent < listIndents.at(-1)) {
      listIndents.pop();
      fence = '';
    }
    let content = block.slice(listIndents.at(-1));
    if (!fence && !inComment) {
      let list;
      while ((list = /^ {0,3}(?:[-*+]|\d{1,9}[.)]) {1,4}(?=\S)/.exec(content))) {
        listIndents.push(listIndents.at(-1) + list[0].length);
        content = content.slice(list[0].length);
      }
    }
    const marker = /^ {0,3}(`{3,}(?=[^`]*$)|~{3,})/.exec(content)?.[1];
    if (fence) {
      if (marker?.startsWith(fence) && content.trim() === marker) fence = '';
    } else if (inComment) {
      inComment = !line.includes('-->');
    } else if (marker) {
      fence = marker;
    } else {
      inComment = line.trimStart().startsWith('<!--') && line.lastIndexOf('<!--') > line.lastIndexOf('-->');
      const item = /^( {0,3}[-*+][ \t]+)\[([ xX])\]([ \t]+)(.*)$/.exec(line);
      if (item) {
        yield { index: match.index + item[1].length, end: match.index + line.length,
          checked: item[2] !== ' ', space: item[3], text: item[4] };
      }
    }
  }
}

// Rollout verification distinguishes a missing checkbox from an unchecked one.
function skipCheckboxState(body) {
  let state = null;
  for (const item of checklistItems(body)) {
    if (!/^Skip nightly merge\b/i.test(item.text)) continue;
    if (item.checked) return true;
    state = false;
  }
  return state;
}

function skipChecked(body) {
  return skipCheckboxState(body) === true;
}

function eligibleForNightly(pr) {
  return pr.state === 'open' && !pr.draft && pr.base.ref === 'main' &&
    !pr.labels.some(label => label.name === skipLabel) && !skipChecked(pr.body);
}

module.exports = { checklistItems, skipLabel, skipCheckboxState, skipChecked, eligibleForNightly };
