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

import * as core from "@actions/core";
import {config} from "dotenv";
import {getOctokit} from "@actions/github";
import {GitHub} from "@actions/github/lib/utils";

export function handleError(err: any): void {
    console.error(err)
    core.setFailed(`Unhandled error: ${err}`)
}

export function obtainClient(token: string): InstanceType<typeof GitHub> {
    config()
    return getOctokit(process.env[token])
}
