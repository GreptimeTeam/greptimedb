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
import {handleError, obtainClient} from "@/common"
import {context} from "@actions/github"
import _ from "lodash"

async function main() {
    const success = process.env["CI_REPORT_STATUS"] === "success"
    core.info(`CI_REPORT_STATUS=${process.env["CI_REPORT_STATUS"]}, resolved to ${success}`)

    const client = obtainClient("GITHUB_TOKEN")
    const title = `Workflow run '${context.action}' failed`
    const url =`${process.env["GITHUB_SERVER_URL"]}/${process.env["GITHUB_REPOSITORY"]}/actions/runs/${process.env["GITHUB_RUN_ID"]}`
    const failure_comment = `New failure: ${url}`
    const success_comment = `Back to success: ${url}`

    const owner = "GreptimeTeam"
    const repo = "greptimedb"
    const labels = ['O-ci-failure']

    const issues = await client.paginate(client.rest.issues.listForRepo, {
        owner,
        repo,
        labels: labels.join(','),
        state: "open",
        sort: "created",
        direction: "desc",
    });
    const issue = _.find(issues, (i) => i.title === title);

    if (issue) { // exist issue
        core.info(`Found previous issue ${issue.html_url}`)
        if (!success) {
            await client.rest.issues.createComment({
                owner,
                repo,
                issue_number: issue.number,
                body: failure_comment,
            })
        } else {
            await client.rest.issues.createComment({
                owner,
                repo,
                issue_number: issue.number,
                body: success_comment,
            })
            await client.rest.issues.update({
                owner,
                repo,
                issue_number: issue.number,
                state: "closed",
                state_reason: "completed",
            })
        }
    } else if (!success) { // create new issue for failure
        await client.rest.issues.create({
            owner,
            repo,
            title,
            labels,
            body: failure_comment,
        })
    }
}

main().catch(handleError)
