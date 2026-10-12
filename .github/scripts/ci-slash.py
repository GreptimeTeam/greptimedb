#!/usr/bin/env python3
# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Admit `/ci` comments and dispatch a draft-PR CI workflow."""
import json, os, re, sys, urllib.request

COMMAND = re.compile(r'^/ci(?:\s+(.+))?$')
OPTIONS = {
    '': ('rust.yml,integration.yml,checks.yml,docs.yml', 'none', 'standard CI'),
    'help': (None, 'none', ''),
    'rust': ('rust.yml', 'none', 'Rust CI'),
    'integration': ('integration.yml', 'none', 'Integration CI (without fuzz)'),
    'checks': ('checks.yml', 'none', 'Checks'),
    'docs': ('docs.yml', 'none', 'Docs CI'),
    'fuzz standalone': ('integration.yml', 'standalone', 'standalone fuzz'),
    'fuzz distributed': ('integration.yml', 'distributed', 'distributed fuzz'),
    'fuzz chaos': ('integration.yml', 'chaos', 'chaos fuzz'),
    'fuzz all': ('integration.yml', 'all', 'all fuzz'),
}
HELP = '''Available draft-PR CI commands:\n\n- `/ci` — standard CI\n- `/ci rust` — Rust CI\n- `/ci integration` — integration CI without fuzz\n- `/ci checks` or `/ci docs`\n- `/ci fuzz standalone|distributed|chaos`\n- `/ci fuzz all` — all fuzz suites (admin only)\n\nCommands require the PR author or repository write/maintain/admin permission and an open draft PR. Fork PRs require repository write permission and rerun existing pull-request CI; fuzz commands are same-repository only. `/ci fuzz all` requires admin permission. Same-repository CI runs the branch head at dispatch time. Fork CI reruns the original pull-request revision (within GitHub rerun limits); sync your branch with the updated CI configuration first.'''

def api(path):
    req=urllib.request.Request(os.environ['GITHUB_API_URL']+path, headers={'Authorization':'Bearer '+os.environ['GITHUB_TOKEN'],'Accept':'application/vnd.github+json'})
    with urllib.request.urlopen(req) as r: return json.load(r)
def out(**kw):
    with open(os.environ["GITHUB_OUTPUT"], "a") as file:
        for key, value in kw.items():
            value = str(value)
            if "\n" in value:
                file.write(f"{key}<<CI_COMMAND_OUTPUT\n{value}\nCI_COMMAND_OUTPUT\n")
            else:
                file.write(f"{key}={value}\n")
def reject(number, text): out(skip='true',pr_number=number,reply='CI command ignored: '+text); return 0
def main():
    if os.environ.get('DISPATCH_SENDER') != 'github-actions[bot]': return reject('', 'invalid dispatch sender.')
    comment=api('/repos/'+os.environ['GITHUB_REPOSITORY']+'/issues/comments/'+os.environ['COMMENT_ID'])
    body=(comment.get('body') or '').partition('\n')[0].strip(); m=COMMAND.fullmatch(body)
    number=str(comment.get('issue_url','').rstrip('/').split('/')[-1])
    if not m: return reject(number,'comment is not a `/ci` command.')
    arg=(m.group(1) or '').strip().lower()
    if arg not in OPTIONS: return reject(number,'unknown command; use `/ci help`.')
    if arg=='help': out(skip='true',pr_number=number,reply=HELP); return 0
    pr=api('/repos/'+os.environ['GITHUB_REPOSITORY']+'/pulls/'+number)
    if pr.get('state')!='open' or not pr.get('draft'): return reject(number,'PR must be open and draft.')
    fork=pr.get('head',{}).get('repo',{}).get('full_name') != os.environ['GITHUB_REPOSITORY']
    if fork and arg.startswith('fuzz '): return reject(number,'fuzz commands require a same-repository PR.')
    head=pr.get('head',{})
    head_sha=head.get('sha','')
    head_ref=head.get('ref','')
    if head_sha != os.environ.get('DISPATCH_HEAD_SHA') or not re.fullmatch('[0-9a-f]{40}',head_sha) or not head_ref: return reject(number,'PR head changed; comment again.')
    actor=comment.get('user',{}).get('login','')
    if not actor: return reject(number,'comment author is missing.')
    is_author=actor==pr.get('user',{}).get('login')
    if fork or arg=='fuzz all' or not is_author:
        permission=api('/repos/'+os.environ['GITHUB_REPOSITORY']+'/collaborators/'+actor+'/permission').get('permission')
        if arg=='fuzz all':
            if permission!='admin': return reject(number,'repository admin permission is required for `/ci fuzz all`.')
        elif permission not in ('write','maintain','admin'):
            return reject(number,'PR author or repository write permission is required.')
    workflow, profile, label=OPTIONS[arg]
    if fork:
        runs=api('/repos/'+os.environ['GITHUB_REPOSITORY']+'/actions/runs?event=pull_request&head_sha='+head_sha+'&per_page=100')['workflow_runs']
        selected={}
        for run in runs:
            name=run.get('path','').split('@',1)[0].removeprefix('.github/workflows/')
            if name not in workflow.split(',') or name in selected: continue
            if run.get('head_sha') != head_sha or run.get('head_repository',{}).get('full_name') != head['repo']['full_name'] or run.get('head_branch') != head_ref: continue
            selected[name]=run
        if not selected: return reject(number,'no pull-request CI runs found for this head; push a new commit first.')
        if arg and workflow not in selected: return reject(number,'selected CI has no pull-request run for this head.')
        if any(run['status'] != 'completed' for run in selected.values()): return reject(number,'pull-request CI is still pending; approve or wait for it, then comment again.')
        for run in selected.values():
            original=run if run.get('run_attempt',1)==1 else api('/repos/'+os.environ['GITHUB_REPOSITORY']+'/actions/runs/'+str(run['id'])+'/attempts/1')
            if original.get('conclusion') != 'skipped': return reject(number,'only originally skipped draft CI runs can be requested; push a new commit while draft.')
        out(skip='false',pr_number=number,run_ids=','.join(str(run['id']) for run in selected.values()),reply=f'Requested rerun of {", ".join(selected)} for `{head_sha}`.')
        return 0
    out(skip='false',pr_number=number,head_sha=head_sha,head_ref=head_ref,workflow=workflow,fuzz_profile=profile,reply=f'Dispatched {label} for branch `{head_ref}`.')
    return 0
if __name__ == '__main__': sys.exit(main())
