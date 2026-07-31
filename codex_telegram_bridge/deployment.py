from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from pathlib import Path
from typing import Any


DEPLOYMENT_MANIFEST_NAME = "deployment-manifest.json"
DEPLOYMENT_MANIFEST_FORMAT = 1
VERSION_PATTERN = re.compile(r"^[0-9]+(?:\.[0-9]+){2}$")


class DeploymentIntegrityError(RuntimeError):
    pass


def package_tree_digest(package_root: str | Path) -> str:
    root = Path(package_root).resolve(strict=True)
    digest = hashlib.sha256()
    files = sorted(
        path
        for path in root.rglob("*")
        if "__pycache__" not in path.parts and path.suffix != ".pyc"
    )
    for path in files:
        if path.is_symlink():
            raise DeploymentIntegrityError("package tree contains a symlink")
        status = path.stat()
        if not stat.S_ISREG(status.st_mode):
            continue
        relative = path.relative_to(root).as_posix().encode("utf-8")
        payload = path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def package_version_from_source(package_root: str | Path) -> str:
    source = (Path(package_root) / "__init__.py").read_text(encoding="utf-8")
    match = re.search(r'^__version__\s*=\s*["\']([^"\']+)["\']', source, re.MULTILINE)
    if match is None or VERSION_PATTERN.fullmatch(match.group(1)) is None:
        raise DeploymentIntegrityError("package version is invalid")
    return match.group(1)


def version_tuple(value: str) -> tuple[int, int, int]:
    if VERSION_PATTERN.fullmatch(value) is None:
        raise DeploymentIntegrityError("deployment version is invalid")
    major, minor, patch = value.split(".")
    return int(major), int(minor), int(patch)


def deployment_manifest_path(state_dir: str | Path) -> Path:
    return Path(state_dir) / DEPLOYMENT_MANIFEST_NAME


def read_deployment_manifest(
    state_dir: str | Path,
    *,
    require_owner_only: bool = True,
) -> dict[str, Any] | None:
    path = deployment_manifest_path(state_dir)
    try:
        status = path.stat()
    except FileNotFoundError:
        return None
    if (
        path.is_symlink()
        or not stat.S_ISREG(status.st_mode)
        or (require_owner_only and status.st_uid != os.getuid())
        or (require_owner_only and stat.S_IMODE(status.st_mode) & 0o077)
    ):
        raise DeploymentIntegrityError("deployment manifest is not owner-only")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raise DeploymentIntegrityError("deployment manifest is invalid") from None
    if not isinstance(payload, dict) or payload.get("formatVersion") != 1:
        raise DeploymentIntegrityError("deployment manifest format is invalid")
    version_tuple(str(payload.get("packageVersion") or ""))
    package_digest = str(payload.get("packageDigest") or "")
    if re.fullmatch(r"[0-9a-f]{64}", package_digest) is None:
        raise DeploymentIntegrityError("deployment package digest is invalid")
    source_commit = payload.get("sourceCommit")
    if source_commit is not None and re.fullmatch(r"[0-9a-f]{40}", str(source_commit)) is None:
        raise DeploymentIntegrityError("deployment source commit is invalid")
    return payload


def validate_deployment_transition(
    existing: dict[str, Any] | None,
    *,
    candidate_version: str,
    candidate_digest: str,
) -> None:
    candidate = version_tuple(candidate_version)
    if re.fullmatch(r"[0-9a-f]{64}", candidate_digest) is None:
        raise DeploymentIntegrityError("candidate package digest is invalid")
    if existing is None:
        return
    installed = version_tuple(str(existing["packageVersion"]))
    if candidate < installed:
        raise DeploymentIntegrityError(
            "refusing to install an older bridge version"
        )
    if candidate == installed and candidate_digest != existing["packageDigest"]:
        raise DeploymentIntegrityError(
            "bridge code changed without a version bump"
        )


def deployment_health(state_dir: str | Path, package_root: str | Path) -> dict[str, Any]:
    try:
        manifest = read_deployment_manifest(state_dir)
        current_digest = package_tree_digest(package_root)
    except (DeploymentIntegrityError, OSError):
        return {
            "deploymentManifestPresent": deployment_manifest_path(state_dir).is_file(),
            "deploymentIntegrity": False,
            "deploymentVersion": None,
            "deploymentSourceCommit": None,
        }
    return {
        "deploymentManifestPresent": manifest is not None,
        "deploymentIntegrity": bool(
            manifest is not None
            and current_digest == manifest["packageDigest"]
        ),
        "deploymentVersion": (
            None if manifest is None else manifest["packageVersion"]
        ),
        "deploymentSourceCommit": (
            None if manifest is None else manifest.get("sourceCommit")
        ),
    }
