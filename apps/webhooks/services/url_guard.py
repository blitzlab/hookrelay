"""SSRF guard for subscriber URLs.

Webhook senders make outbound requests to user-supplied URLs, so without a
check anyone who can register an endpoint can make the service call internal
addresses (metadata services, databases, admin panels).
"""

import ipaddress
import socket
from urllib.parse import urlsplit


class UnsafeURLError(ValueError):
    pass


def _is_public(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return not (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def validate_destination_url(url: str, *, allow_private: bool) -> None:
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"}:
        raise UnsafeURLError("URL scheme must be http or https.")
    if not parts.hostname:
        raise UnsafeURLError("URL must include a hostname.")
    if allow_private:
        return

    try:
        infos = socket.getaddrinfo(parts.hostname, parts.port or 443, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise UnsafeURLError(f"Could not resolve {parts.hostname}.") from exc

    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if not _is_public(ip):
            raise UnsafeURLError(f"{parts.hostname} resolves to a non-public address ({ip}).")
