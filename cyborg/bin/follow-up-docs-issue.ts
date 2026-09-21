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
import {
    PullRequestClosedEvent,
    PullRequestEditedEvent,
    PullRequestEvent,
    PullRequestOpenedEvent
} from "@octokit/webhooks-types";
// @ts-expect-error moduleResolution:nodenext issue 54523
import {RequestError} from "@octokit/request-error";

const needFollowUpDocs = "[x] This PR requires documentation updates."
const labelDocsNotRequired = "docs-not-required"
const labelDocsRequired = "docs-required"

async function main() {
    if (!context.payload.pull_request) {
        throw new Error(`Only pull request event supported. ${context.eventName} is unsupported.`)
    }

    const payload = context.payload as PullRequestEvent
    const { owner, repo, number, actor } = {
        owner: payload.pull_request.base.user.login,
        repo: payload.pull_request.base.repo.name,
        number: payload.pull_request.number,
        actor: payload.pull_request.user.login,
    }

    switch (payload.action) {
        case "opened": {
            const client = obtainClient("GITHUB_TOKEN")
            await updateDocsLabels(client, owner, repo, number, checkPullRequestOpenedEvent(payload as PullRequestOpenedEvent))
            break
        }
        case "edited": {
            const followUpDocs = checkPullRequestEditedEvent(payload as PullRequestEditedEvent)
            if (followUpDocs === undefined) {
                core.info("Docs checkbox state unchanged; leaving labels as-is.")
                return
            }
            const client = obtainClient("GITHUB_TOKEN")
            await updateDocsLabels(client, owner, repo, number, followUpDocs)
            break
        }
        case "closed": {
            const event = payload as PullRequestClosedEvent
            if (!event.pull_request.merged) {
                core.info("PR closed without merging; no docs issue needed.")
                return
            }
            // Re-read the live PR state instead of trusting the event payload's
            // label snapshot: a concurrent edit run may not have applied the
            // docs-required label yet when the PR is merged.
            const client = obtainClient("GITHUB_TOKEN")
            const { data: pr } = await client.rest.pulls.get({ owner, repo, pull_number: number })
            const hasDocsLabel = pr.labels.some((label) => label.name === labelDocsRequired)
            const checkboxChecked = pr.body?.includes(needFollowUpDocs) ?? false
            if (!hasDocsLabel && !checkboxChecked) {
                core.info(`Neither label ${labelDocsRequired} nor the docs checkbox is present; no docs issue needed.`)
                return
            }
            const docsClient = obtainClient("DOCS_REPO_TOKEN")
            await createDocsIssue(docsClient, pr.title, pr.html_url, actor)
            break
        }
        default:
            throw new Error(`${payload.action} is unsupported.`)
    }
}

async function updateDocsLabels(client: ReturnType<typeof obtainClient>, owner: string, repo: string, number: number, followUpDocs: boolean) {
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

async function createDocsIssue(docsClient: ReturnType<typeof obtainClient>, title: string, html_url: string, actor: string) {
    core.info("Creating follow-up docs issue for merged PR.")

    // Get available assignees for the docs repo
    const assigneesResponse = await docsClient.rest.issues.listAssignees({
        owner: 'GreptimeTeam',
        repo: 'docs',
    })
    const validAssignees = assigneesResponse.data.map(assignee => assignee.login)
    core.info(`Available assignees: ${validAssignees.join(', ')}`)

    // Check if the actor is a valid assignee, otherwise fallback to fengjiachun
    const assignee = validAssignees.includes(actor) ? actor : 'fengjiachun'
    core.info(`Assigning issue to: ${assignee}`)

    await docsClient.rest.issues.create({
        owner: 'GreptimeTeam',
        repo: 'docs',
        title: `Update docs for ${title}`,
        body: `A document change request is generated from ${html_url}`,
        assignee: assignee,
    }).then((res) => {
        core.info(`Created issue ${res.data}`)
    })
}

function checkPullRequestOpenedEvent(event: PullRequestOpenedEvent): boolean {
    // @ts-ignore
    return event.pull_request.body?.includes(needFollowUpDocs)
}

// Returns undefined when the checkbox state did not change in this edit, so the
// caller leaves the labels untouched (preserving manual label overrides).
function checkPullRequestEditedEvent(event: PullRequestEditedEvent): boolean | undefined {
    if (!event.changes.body) {
        // The body was not part of this edit (e.g. title-only edit).
        return undefined
    }
    const previous = event.changes.body.from.includes(needFollowUpDocs)
    const current = event.pull_request.body?.includes(needFollowUpDocs) ?? false
    if (previous === current) {
        return undefined
    }
    return current
}

main().catch(handleError)
