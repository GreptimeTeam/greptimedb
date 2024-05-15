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
import {PullRequestEditedEvent, PullRequestEvent, PullRequestOpenedEvent} from "@octokit/webhooks-types";
// @ts-expect-error moduleResolution:nodenext issue 54523
import {RequestError} from "@octokit/request-error";

const needFollowUpDocs = "[x] This PR requires documentation updates."
const labelDocsNotRequired = "docs-not-required"
const labelDocsRequired = "docs-required"

async function main() {
    if (!context.payload.pull_request) {
        throw new Error(`Only pull request event supported. ${context.eventName} is unsupported.`)
    }

    const client = obtainClient("GITHUB_TOKEN")
    const docsClient = obtainClient("DOCS_REPO_TOKEN")
    const payload = context.payload as PullRequestEvent
    const { owner, repo, number, actor, title, html_url } = {
        owner: payload.pull_request.base.user.login,
        repo: payload.pull_request.base.repo.name,
        number: payload.pull_request.number,
        title: payload.pull_request.title,
        html_url: payload.pull_request.html_url,
        actor: payload.pull_request.user.login,
    }
    const followUpDocs = checkPullRequestEvent(payload)
    if (followUpDocs) {
        core.info("Follow up docs.")
        await client.rest.issues.removeLabel({
            owner, repo, issue_number: number, name: labelDocsNotRequired,
        }).catch((e: RequestError) => {
            if (e.status != 404) {
                throw e;
            }
            core.debug(`Label ${labelDocsNotRequired} not exist.`)
        })
        await client.rest.issues.addLabels({
            owner, repo, issue_number: number, labels: [labelDocsRequired],
        })
        await docsClient.rest.issues.create({
            owner: 'GreptimeTeam',
            repo: 'docs',
            title: `Update docs for ${title}`,
            body: `A document change request is generated from ${html_url}`,
            assignee: actor,
        }).then((res) => {
            core.info(`Created issue ${res.data}`)
        })
    } else {
        core.info("No need to follow up docs.")
        await client.rest.issues.removeLabel({
            owner, repo, issue_number: number, name: labelDocsRequired
        }).catch((e: RequestError) => {
            if (e.status != 404) {
                throw e;
            }
            core.debug(`Label ${labelDocsRequired} not exist.`)
        })
        await client.rest.issues.addLabels({
            owner, repo, issue_number: number, labels: [labelDocsNotRequired],
        })
    }
}

function checkPullRequestEvent(payload: PullRequestEvent) {
    switch (payload.action) {
        case "opened":
            return checkPullRequestOpenedEvent(payload as PullRequestOpenedEvent)
        case "edited":
            return checkPullRequestEditedEvent(payload as PullRequestEditedEvent)
        default:
            throw new Error(`${payload.action} is unsupported.`)
    }
}

function checkPullRequestOpenedEvent(event: PullRequestOpenedEvent): boolean {
    // @ts-ignore
    return event.pull_request.body?.includes(needFollowUpDocs)
}

function checkPullRequestEditedEvent(event: PullRequestEditedEvent): boolean {
    const previous = event.changes.body?.from.includes(needFollowUpDocs)
    const current = event.pull_request.body?.includes(needFollowUpDocs)
    // from docs-not-need to docs-required
    return (!previous) && current
}

main().catch(handleError)
