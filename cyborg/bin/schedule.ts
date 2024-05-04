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
import {GitHub} from "@actions/github/lib/utils"
import _ from "lodash";
import dayjs from "dayjs";
import {handleError, obtainClient} from "@/common";

async function main() {
    const client = obtainClient("GITHUB_TOKEN")
    await unassign(client)
}

async function unassign(client: InstanceType<typeof GitHub>) {
    const owner = "GreptimeTeam"
    const repo = "greptimedb"

    const dt = dayjs().subtract(14, 'days');
    core.info(`Open issues updated before ${dt.toISOString()} will be considered stale.`)

    const members = await client.paginate(client.rest.repos.listCollaborators, {
        owner,
        repo,
        permission: "push",
        per_page: 100
    }).then((members) => members.map((member) => member.login))
    core.info(`Members (${members.length}): ${members}`)

    const issues = await client.paginate(client.rest.issues.listForRepo, {
        owner,
        repo,
        state: "open",
        sort: "created",
        direction: "asc",
        per_page: 100
    })
    for (const issue of issues) {
        let assignees = [];
        if (issue.assignee) {
            assignees.push(issue.assignee.login)
        }
        for (const assignee of issue.assignees) {
            assignees.push(assignee.login)
        }
        assignees = _.uniq(assignees)
        assignees = _.difference(assignees, members)
        if (assignees.length > 0 && dayjs(issue.updated_at).isBefore(dt)) {
            core.info(`Assignees ${assignees} of issue ${issue.number} will be unassigned.`)
            await client.rest.issues.removeAssignees({
                owner,
                repo,
                issue_number: issue.number,
                assignees: assignees,
            })
        }
    }
}

main().catch(handleError)
