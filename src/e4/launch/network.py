"""What a bubblewrap sandbox that shares the host network needs to reach a remote model endpoint."""
from __future__ import annotations

from pathlib import Path

# name resolution and TLS certificates
REMOTE_NETWORK_FILES = ("/etc/resolv.conf", "/etc/hosts", "/etc/nsswitch.conf", "/etc/ssl",
                        "/etc/ca-certificates")


def remote_network_binds() -> list[str]:
    """bwrap arguments mounting each present REMOTE_NETWORK_FILES entry read-only, symlinks resolved."""
    args = []
    for name in REMOTE_NETWORK_FILES:
        path = Path(name)
        if path.exists():
            args += ["--ro-bind", str(path.resolve(strict=True)), name]
    return args
