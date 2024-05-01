import * as core from "@actions/core";
import {config} from "dotenv";
import {getOctokit} from "@actions/github";
import {GitHub} from "@actions/github/lib/utils";

export function handleError(err: any): void {
    core.error(err)
    core.setFailed(`Unhandled error: ${err}`)
}

export function obtainClient(): InstanceType<typeof GitHub> {
    const env = config().parsed
    return getOctokit(env["GITHUB_TOKEN"])
}
