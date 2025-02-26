/*
 * Copyright 2023 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as core from '@actions/core'
import {handleError, obtainClient} from "@/common";
import {context} from "@actions/github";
import {PullRequestEvent} from "@octokit/webhooks-types";
import {Options, sync as conventionalCommitsParser} from 'conventional-commits-parser';
import conventionalCommitTypes from 'conventional-commit-types';
import _ from "lodash";

const defaultTypes = Object.keys(conventionalCommitTypes.types)
const breakingChangeLabel = "breaking-change"

// These options are copied from [1].
// [1] https://github.com/conventional-changelog/conventional-changelog/blob/3f60b464/packages/conventional-changelog-conventionalcommits/src/parser.js
export const parserOpts: Options = {
    headerPattern: /^(\w*)(?:\((.*)\))?!?: (.*)$/,
    breakingHeaderPattern: /^(\w*)(?:\((.*)\))?!: (.*)$/,
    headerCorrespondence: [
        'type',
        'scope',
        'subject'
    ],
    noteKeywords: ['BREAKING CHANGE', 'BREAKING-CHANGE'],
    revertPattern: /^(?:Revert|revert:)\s"?([\s\S]+?)"?\s*This reverts commit (\w*)\./i,
    revertCorrespondence: ['header', 'hash'],
    issuePrefixes: ['#']
}

async function main() {
    if (!context.payload.pull_request) {
        throw new Error(`Only pull request event supported. ${context.eventName} is unsupported.`)
    }

    const client = obtainClient("GITHUB_TOKEN")
    const payload = context.payload as PullRequestEvent
    const { owner, repo, number } = {
        owner: payload.pull_request.base.user.login,
        repo: payload.pull_request.base.repo.name,
        number: payload.pull_request.number,
    }
    const { data: pull_request } = await client.rest.pulls.get({
        owner, repo, pull_number: number,
    })

    const commit = conventionalCommitsParser(pull_request.title, parserOpts)
    core.info(`Receive commit: ${JSON.stringify(commit)}`)

    if (!commit.type) {
        throw Error(`Malformed commit: ${JSON.stringify(commit)}`)
    }

    if (!defaultTypes.includes(commit.type)) {
        throw Error(`Unexpected type ${JSON.stringify(commit.type)} of commit: ${JSON.stringify(commit)}`)
    }

    const breakingChanges = _.filter(commit.notes, _.matches({ title: 'BREAKING CHANGE'}))
    if (breakingChanges.length > 0) {
        await client.rest.issues.addLabels({
            owner, repo, issue_number: number, labels: [breakingChangeLabel]
        })
    }
}

main().catch(handleError)
