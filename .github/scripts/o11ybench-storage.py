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

"""Select File/S3 benchmark arms without changing workload adapters."""
import argparse
import os
import subprocess


def validate(env):
    mode = env.get('GREPTIMEDB_STORAGE', 'file')
    if mode not in ('file', 's3', 'both'):
        raise ValueError('GREPTIMEDB_STORAGE must be file, s3 or both')
    if mode != 'file':
        if env.get('RUN_GREPTIMEDB') != 'true':
            raise ValueError('S3 requires Run GreptimeDB')
        if env.get('PROVIDER') != 'AWS':
            raise ValueError('S3 comparisons require an AWS runner')
        if not env.get('S3_BUCKET') or not env.get('S3_REGION'):
            raise ValueError('AWS_CI_TEST_BUCKET and AWS_CI_TEST_BUCKET_REGION must be configured')
        if env['S3_REGION'] != env.get('RUNNER_REGION'):
            raise ValueError('S3 bucket and EC2 runner regions must match')
    return mode


def replace(command, key, value):
    result = list(command)
    if key in result:
        result[result.index(key) + 1] = value
    else:
        result += [key, value]
    return result


def commands(command, mode):
    stage = command[command.index('--stage') + 1]
    root = command[command.index('--root') + 1]
    owner = command[command.index('--owner') + 1]
    primary = list(command)
    if stage == 'target':
        primary += ['--greptimedb-storage', 's3' if mode == 's3' else 'file']
    result = [primary]
    if mode == 'both':
        secondary = replace(command, '--root', root + '/s3')
        secondary = replace(secondary, '--owner', owner + '-s3')
        if stage == 'target':
            source = command[command.index('--data-root') + 1] if '--data-root' in command else root
            secondary = replace(secondary, '--data-root', source)
            secondary += ['--greptimedb-storage', 's3']
        elif stage == 'report':
            source = command[command.index('--data-root') + 1] if '--data-root' in command else root
            secondary = replace(secondary, '--data-root', source)
            secondary = replace(secondary, '--targets', 'greptimedb')
            if '--baseline-run' in secondary:
                # The native report confines baseline files to its own root.
                # The top-level storage overview compares all four arms.
                index = secondary.index('--baseline-run')
                del secondary[index:index + 2]
        result.append(secondary)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['validate', 'run'])
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    mode = validate(os.environ)
    if args.action == 'validate':
        print('GreptimeDB storage: ' + mode)
        return
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    planned = commands(command, mode)
    failed = 0
    for cmd in planned:
        result = subprocess.run(cmd)
        failed = failed or result.returncode
        # Cleanup must try both arms even if one failed. Target failures remain
        # visible, while the other arm can still produce diagnostic evidence.
    raise SystemExit(failed)


if __name__ == '__main__':
    main()
