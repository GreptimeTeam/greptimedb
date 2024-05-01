import * as core from '@actions/core'
import {GitHub} from "@actions/github/lib/utils"
import _ from "lodash";
import dayjs from "dayjs";
import {handleError, obtainClient} from "@/.";

async function main() {
    const client = obtainClient()
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
