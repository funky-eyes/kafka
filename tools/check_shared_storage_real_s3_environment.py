#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Read-only GitHub Environment metadata preflight for Real AWS S3 GA evidence.

This never reads environment secret values or invokes AWS. A successful result
confirms only the presence of the GitHub Environment metadata needed to attempt
the exact-candidate compatibility test.
"""

import argparse
import json
import subprocess
import sys

DEFAULT_REPOSITORY = "funky-eyes/kafka"
DEFAULT_ENVIRONMENT = "shared-storage-aws-s3"
REQUIRED_VARIABLE = "SHARED_STORAGE_AWS_S3_BUCKET"
REQUIRED_SECRET = "SHARED_STORAGE_AWS_ROLE_ARN"


class EnvironmentMetadataError(RuntimeError):
    pass


def gh_json(args, runner=subprocess.run):
    command = ["gh", *args]
    try:
        result = runner(command, capture_output=True, text=True, check=False)
    except OSError as exc:
        raise EnvironmentMetadataError(
            "Cannot execute gh; install GitHub CLI and authenticate with an account "
            "allowed to view repository Environment metadata"
        ) from exc
    if result.returncode:
        raise EnvironmentMetadataError(
            "GitHub Environment metadata lookup failed; check gh auth and "
            "Environment permissions (gh exit code " + str(result.returncode) + ")"
        )
    try:
        payload = json.loads(result.stdout)
    except (ValueError, TypeError) as exc:
        raise EnvironmentMetadataError("GitHub CLI returned invalid metadata JSON") from exc
    if not isinstance(payload, list):
        raise EnvironmentMetadataError("GitHub CLI returned non-list Environment metadata")
    return payload


def missing_configuration(variables, secrets):
    if not isinstance(variables, list) or not isinstance(secrets, list):
        raise EnvironmentMetadataError("GitHub Environment metadata must contain lists")

    variable_values = {}
    for item in variables:
        if not isinstance(item, dict):
            raise EnvironmentMetadataError("Malformed Environment variable metadata")
        if isinstance(item.get("name"), str):
            variable_values[item["name"]] = item.get("value")

    secret_names = set()
    for item in secrets:
        if not isinstance(item, dict):
            raise EnvironmentMetadataError("Malformed Environment secret metadata")
        if isinstance(item.get("name"), str):
            secret_names.add(item["name"])

    missing = []
    bucket = variable_values.get(REQUIRED_VARIABLE)
    if not isinstance(bucket, str) or not bucket.strip():
        missing.append("Environment variable " + REQUIRED_VARIABLE + " is missing or blank")
    if REQUIRED_SECRET not in secret_names:
        missing.append("Environment secret " + REQUIRED_SECRET + " is missing")
    return missing


def inspect_environment(repo, environment, runner=subprocess.run):
    variables = gh_json(
        ["variable", "list", "--repo", repo, "--env", environment, "--json", "name,value"],
        runner,
    )
    secrets = gh_json(
        ["secret", "list", "--repo", repo, "--env", environment, "--json", "name"],
        runner,
    )
    return missing_configuration(variables, secrets)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=DEFAULT_REPOSITORY)
    parser.add_argument("--environment", default=DEFAULT_ENVIRONMENT)
    args = parser.parse_args()

    try:
        missing = inspect_environment(args.repo, args.environment)
    except EnvironmentMetadataError as exc:
        print("Real S3 Environment metadata check failed: " + str(exc), file=sys.stderr)
        return 2

    if missing:
        print("Real S3 Environment configuration is incomplete:", file=sys.stderr)
        for detail in missing:
            print("- " + detail, file=sys.stderr)
        return 1

    print("Real S3 GitHub Environment metadata: PRESENT")
    print("Bucket variable has a non-empty value and the IAM role secret name exists.")
    print(
        "NOT VERIFIED: IAM role secret value, GitHub deployment protection, "
        "OIDC trust policy, IAM permissions, or live AWS S3 behavior."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
