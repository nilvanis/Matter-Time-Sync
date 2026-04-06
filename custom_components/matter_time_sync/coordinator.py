"""Coordinator for Matter Time Sync."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import WSMsgType
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr

from .const import (
    CONF_FILTER_TARGET,
    CONF_TIMEZONE,
    CONF_WS_URL,
    DEFAULT_FILTER_TARGET,
    DEFAULT_TIMEZONE,
    DEFAULT_WS_URL,
    DOMAIN,
    TIME_SYNC_CLUSTER_ID,
)

_LOGGER = logging.getLogger(__name__)

# Matter/CHIP epoch used by Time Synchronization cluster (microseconds since 2000-01-01)
_CHIP_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)

SYNC_FAILURE_DEVICE_UNAVAILABLE = "device unavailable"
SYNC_FAILURE_TIMEOUT = "timeout"
SYNC_FAILURE_SERVER_UNAVAILABLE = "matter server unavailable"
SYNC_FAILURE_COMMAND_FAILED = "sync command failed"

_TERMINAL_SYNC_FAILURES = {
    SYNC_FAILURE_DEVICE_UNAVAILABLE,
    SYNC_FAILURE_TIMEOUT,
    SYNC_FAILURE_SERVER_UNAVAILABLE,
}


@dataclass(slots=True)
class CommandResult:
    """Internal result for Matter server commands."""

    success: bool
    response: dict[str, Any] | None = None
    reason: str | None = None
    details: str | None = None
    error_code: int | str | None = None
    should_retry: bool = False


@dataclass(slots=True)
class SyncTimeResult:
    """Result of a single device time sync."""

    success: bool
    node_name: str
    reason: str | None = None
    details: str | None = None
    failed_command: str | None = None


def format_sync_failure_message(node_id: int, node_name: str, reason: str) -> str:
    """Build the user-visible sync failure log message."""
    return f"Time sync failed for node {node_id} ({node_name}): {reason}"


def log_sync_failure(node_id: int, node_name: str, reason: str) -> None:
    """Emit the single visible warning for a failed sync attempt."""
    _LOGGER.warning(format_sync_failure_message(node_id, node_name, reason))


def _to_chip_epoch_us(dt: datetime) -> int:
    """Convert a datetime to microseconds since CHIP epoch (2000-01-01)."""
    dt_utc = dt.astimezone(timezone.utc)
    return int((dt_utc - _CHIP_EPOCH).total_seconds() * 1_000_000)


# ------------------------------------------------------------------
# Filter helpers (shared with button.py)
# ------------------------------------------------------------------


def filter_candidates_for_node(
    node: dict[str, Any], filter_target: str
) -> list[str]:
    """Return the list of strings to match the device filter against.

    Supports filter_target values:
    - any: match filter against display name + node label + product name
    - display_name: match only the resolved display name (node['name'])
    - ha_name: match only if name_source == 'home_assistant'
    - matter: match only node label + product name
    """
    node_name = node.get("name") or ""
    name_source = node.get("name_source") or ""
    product_name = node.get("product_name") or ""
    node_label = (node.get("device_info") or {}).get("node_label", "") or ""

    if filter_target == "display_name":
        return [node_name]

    if filter_target == "ha_name":
        return [node_name] if name_source == "home_assistant" else []

    if filter_target == "matter":
        candidates = [product_name, node_label]
        if name_source in ("node_label", "product_name"):
            candidates.append(node_name)
        return candidates

    # default: any
    return [node_name, product_name, node_label]


def device_matches_filter(
    filters: list[str], candidates: list[str]
) -> bool:
    """Check if any candidate string matches any of the filter terms.

    Uses case-insensitive partial matching.
    If filters is empty, all devices match.
    Expects filters to already be stripped and lowercased.
    """
    if not filters:
        return True

    haystacks = [c.lower() for c in candidates if c]
    return any(term in h for term in filters for h in haystacks)


class MatterTimeSyncCoordinator:
    """Coordinator to manage Matter Server WebSocket connection."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        """Initialize the coordinator."""
        self.hass = hass
        self.entry = entry
        self._ws_url = entry.data.get(CONF_WS_URL, DEFAULT_WS_URL)
        self._timezone = entry.data.get(CONF_TIMEZONE, DEFAULT_TIMEZONE)
        self._session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._message_id = 0
        self._nodes_cache: list[dict[str, Any]] = []
        self._connected = False
        self._lock = asyncio.Lock()
        self._command_lock = asyncio.Lock()  # Prevent concurrent WS reads

        # Per-node lock: prevents multiple sync runs for the same node_id from interleaving
        self._per_node_sync_locks: dict[int, asyncio.Lock] = {}

        # Auto-sync state tracking
        self._auto_sync_running = False
        self._auto_sync_lock = asyncio.Lock()

    @property
    def is_connected(self) -> bool:
        """Return True if connected to Matter Server."""
        return self._connected and self._ws is not None and not self._ws.closed

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    async def async_connect(self, log_failure: bool = True) -> bool:
        """Connect to Matter Server WebSocket."""
        async with self._lock:
            if self.is_connected:
                return True

            # Close any leftover resources before reconnecting
            self._connected = False
            if self._ws:
                try:
                    await self._ws.close()
                except Exception:  # noqa: BLE001
                    pass
                self._ws = None
            if self._session:
                try:
                    await self._session.close()
                except Exception:  # noqa: BLE001
                    pass
                self._session = None

            try:
                self._session = aiohttp.ClientSession()
                self._ws = await self._session.ws_connect(
                    self._ws_url, timeout=aiohttp.ClientTimeout(total=10)
                )
                self._connected = True
                _LOGGER.info("Connected to Matter Server at %s", self._ws_url)
                return True
            except Exception as err:
                if log_failure:
                    _LOGGER.error("Failed to connect to Matter Server: %s", err)
                else:
                    _LOGGER.debug("Failed to connect to Matter Server: %s", err)
                self._connected = False
                if self._ws:
                    try:
                        await self._ws.close()
                    except Exception:  # noqa: BLE001
                        pass
                    self._ws = None
                if self._session:
                    try:
                        await self._session.close()
                    except Exception:  # noqa: BLE001
                        pass
                    self._session = None
                return False

    async def _cleanup_connection(self) -> None:
        """Close and cleanup websocket + session.

        Acquires _lock internally — safe to call from any context EXCEPT
        while holding _command_lock (to avoid lock-ordering inversion).
        """
        async with self._lock:
            self._connected = False
            ws, self._ws = self._ws, None
            session, self._session = self._session, None

        # Close outside the lock to avoid holding it during I/O
        if ws:
            try:
                await ws.close()
            except Exception:  # noqa: BLE001
                pass
        if session:
            try:
                await session.close()
            except Exception:  # noqa: BLE001
                pass

    async def async_disconnect(self) -> None:
        """Disconnect from Matter Server."""
        await self._cleanup_connection()

    # ------------------------------------------------------------------
    # WebSocket command handling
    # ------------------------------------------------------------------

    async def _async_send_command(
        self, command: str, args: dict[str, Any] | None = None, retry: bool = True
    ) -> CommandResult:
        """Send a command to the Matter Server and wait for response.

        The actual send/receive is performed inside _do_send_command while
        holding _command_lock.  If the connection turns out to be broken we
        release the lock, reconnect, and retry once — avoiding a recursive
        call that could race on _message_id.

        _cleanup_connection is ONLY called outside _command_lock to prevent
        a lock-ordering inversion (_command_lock -> _lock vs _lock -> _command_lock).
        """
        async with self._command_lock:
            result, should_retry = await self._do_send_command(command, args)

        # Handle retry outside _command_lock
        if not result.success and should_retry and retry:
            _LOGGER.debug(
                "WebSocket connection lost, reconnecting and retrying command %s",
                command,
            )
            # Cleanup OUTSIDE _command_lock — safe lock ordering
            await self._cleanup_connection()
            if await self.async_connect(log_failure=False):
                async with self._command_lock:
                    result, _ = await self._do_send_command(command, args)
            return result

        # Clean up on non-retryable connection failures (outside _command_lock)
        if not result.success and not self._connected:
            await self._cleanup_connection()

        return result

    async def _do_send_command(
        self, command: str, args: dict[str, Any] | None = None
    ) -> tuple[CommandResult, bool]:
        """Send a command and wait for its response.

        Returns (response, should_retry).
        Must be called while holding _command_lock.

        IMPORTANT: This method must NEVER call _cleanup_connection() because
        that acquires _lock and we already hold _command_lock — doing so would
        create a lock-ordering inversion.  Instead we just set
        self._connected = False and let the caller handle cleanup.
        """
        if not self.is_connected:
            if not await self.async_connect(log_failure=False):
                return (
                    CommandResult(
                        success=False,
                        reason=SYNC_FAILURE_SERVER_UNAVAILABLE,
                        details="Failed to connect to Matter Server",
                    ),
                    False,
                )

        self._message_id += 1
        message_id = str(self._message_id)

        request: dict[str, Any] = {
            "message_id": message_id,
            "command": command,
        }
        if args:
            request["args"] = args

        try:
            await self._ws.send_json(request)

            async def _wait_for_response() -> CommandResult:
                async for msg in self._ws:
                    if msg.type == WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if data.get("message_id") == message_id:
                            if "error_code" in data:
                                details = data.get("details", "Unknown error")
                                return CommandResult(
                                    success=False,
                                    reason=self._classify_server_error(details),
                                    details=str(details),
                                    error_code=data.get("error_code"),
                                )
                            return CommandResult(success=True, response=data)
                        # Unsolicited / mismatched message — log and skip
                        _LOGGER.debug(
                            "Ignoring unsolicited message (id=%s)",
                            data.get("message_id"),
                        )
                    elif msg.type == WSMsgType.ERROR:
                        self._connected = False
                        return CommandResult(
                            success=False,
                            reason=SYNC_FAILURE_SERVER_UNAVAILABLE,
                            details=str(msg.data),
                        )
                    elif msg.type == WSMsgType.CLOSED:
                        self._connected = False
                        return CommandResult(
                            success=False,
                            reason=SYNC_FAILURE_SERVER_UNAVAILABLE,
                            details="WebSocket closed unexpectedly",
                            should_retry=True,
                        )
                self._connected = False
                return CommandResult(
                    success=False,
                    reason=SYNC_FAILURE_SERVER_UNAVAILABLE,
                    details="WebSocket closed before response",
                )

            response = await asyncio.wait_for(_wait_for_response(), timeout=10)
            return response, response.should_retry

        except asyncio.TimeoutError:
            return (
                CommandResult(
                    success=False,
                    reason=SYNC_FAILURE_TIMEOUT,
                    details=f"Timeout waiting for response to {command}",
                ),
                False,
            )
        except Exception as err:
            err_str = str(err).lower()
            if "closing" in err_str or "closed" in err_str:
                self._connected = False
                return (
                    CommandResult(
                        success=False,
                        reason=SYNC_FAILURE_SERVER_UNAVAILABLE,
                        details=str(err),
                        should_retry=True,
                    ),
                    True,
                )

            self._connected = False
            return (
                CommandResult(
                    success=False,
                    reason=SYNC_FAILURE_SERVER_UNAVAILABLE,
                    details=str(err),
                ),
                False,
            )

    # ------------------------------------------------------------------
    # Device name resolution
    # ------------------------------------------------------------------

    def _get_ha_device_name(self, node_id: int) -> str | None:
        """Try to get the device name from Home Assistant's device registry."""
        try:
            device_reg = dr.async_get(self.hass)
            node_id_str = str(node_id)
            for device in device_reg.devices.values():
                for identifier in device.identifiers:
                    if len(identifier) < 2:
                        _LOGGER.debug("Skipping malformed identifier: %s", identifier)
                        continue
                    if identifier[0] != "matter":
                        continue

                    id_str = str(identifier[1])
                    if (
                        id_str == node_id_str
                        or id_str == f"deviceid_{node_id_str}"
                        or id_str.rsplit("_", 1)[-1] == node_id_str
                    ):
                        if device.name_by_user:
                            _LOGGER.debug(
                                "Found HA device name for node %s: %s (user-defined)",
                                node_id,
                                device.name_by_user,
                            )
                            return device.name_by_user
                        if device.name:
                            _LOGGER.debug(
                                "Found HA device name for node %s: %s",
                                node_id,
                                device.name,
                            )
                            return device.name
        except Exception as err:
            _LOGGER.debug("Could not get HA device name: %s", err)
        return None

    # ------------------------------------------------------------------
    # Node discovery / parsing
    # ------------------------------------------------------------------

    async def async_get_matter_nodes(self) -> list[dict[str, Any]]:
        """Get all Matter nodes from the server."""
        result = await self._async_send_command("get_nodes")
        if not result.success or not result.response:
            return self._nodes_cache

        raw_nodes = result.response.get("result", [])
        self._nodes_cache = self._parse_nodes(raw_nodes)

        # Clean up locks for nodes that no longer exist
        current_node_ids = {n["node_id"] for n in self._nodes_cache}
        stale_ids = set(self._per_node_sync_locks.keys()) - current_node_ids
        for nid in stale_ids:
            lock = self._per_node_sync_locks.get(nid)
            if lock and lock.locked():
                continue
            self._per_node_sync_locks.pop(nid, None)
            _LOGGER.debug("Removed stale sync lock for node %s", nid)

        return self._nodes_cache

    def _get_time_sync_endpoints(self, attributes: dict[str, Any]) -> list[int]:
        """Return endpoint(s) that expose the Time Synchronization cluster (56)."""
        endpoints: set[int] = set()
        for key in attributes:
            parts = key.split("/")
            if len(parts) < 2:
                continue
            try:
                endpoint_id = int(parts[0])
                cluster_id = int(parts[1])
            except ValueError:
                continue
            if cluster_id == 56:
                endpoints.add(endpoint_id)
        return sorted(endpoints)

    def _parse_nodes(self, raw_nodes: list) -> list[dict[str, Any]]:
        """Parse raw node data into usable format."""
        parsed: list[dict[str, Any]] = []
        for node in raw_nodes:
            node_id = node.get("node_id")
            if node_id is None:
                continue

            attributes = node.get("attributes", {})

            device_info = {
                "vendor_name": attributes.get("0/40/1", "Unknown"),
                "product_name": attributes.get("0/40/3", ""),
                "node_label": attributes.get("0/40/5", ""),
                "serial_number": attributes.get("0/40/15", ""),
            }

            time_sync_endpoints = self._get_time_sync_endpoints(attributes)
            has_time_sync = bool(time_sync_endpoints)

            ha_name = self._get_ha_device_name(node_id)
            node_label = device_info.get("node_label", "")
            product_name = device_info.get("product_name", "")

            if ha_name:
                name = ha_name
                name_source = "home_assistant"
            elif node_label:
                name = node_label
                name_source = "node_label"
            elif product_name:
                name = product_name
                name_source = "product_name"
            else:
                name = f"Matter Node {node_id}"
                name_source = "fallback"

            _LOGGER.debug(
                "Node %s: name='%s' (source: %s), product='%s', has_time_sync=%s",
                node_id,
                name,
                name_source,
                product_name,
                has_time_sync,
            )

            parsed.append(
                {
                    "node_id": node_id,
                    "name": name,
                    "name_source": name_source,
                    "product_name": product_name,
                    "device_info": device_info,
                    "has_time_sync": has_time_sync,
                    "time_sync_endpoints": time_sync_endpoints,
                }
            )

        _LOGGER.info("Parsed %d Matter nodes", len(parsed))
        return parsed

    async def async_get_time_sync_cluster_info(
        self, node_id: int, endpoint_id: int
    ) -> dict[str, Any]:
        """Get Time Sync cluster information for diagnostics.

        Only called when debug logging is enabled to avoid unnecessary
        WebSocket round-trips during normal operation.
        """
        result = await self._async_send_command("get_nodes")
        if not result.success or not result.response:
            return {}

        raw_nodes = result.response.get("result", [])
        node = next((n for n in raw_nodes if n.get("node_id") == node_id), None)
        if not node:
            return {}

        attributes = node.get("attributes", {})
        time_sync_attrs = {}

        for key, value in attributes.items():
            parts = key.split("/")
            if len(parts) >= 2:
                try:
                    ep_id = int(parts[0])
                    cluster_id = int(parts[1])
                    if ep_id == endpoint_id and cluster_id == 56:
                        time_sync_attrs[key] = value
                except ValueError:
                    continue

        return time_sync_attrs

    # ------------------------------------------------------------------
    # Entity status update helper
    # ------------------------------------------------------------------

    def _update_entity_sync_status(self, node_id: int, success: bool) -> None:
        """Update the button entity's sync status attributes after auto-sync.

        Looks up the entity via the entity_map stored in hass.data.
        Safe to call even if no entity exists for the node_id.
        """
        try:
            domain_data = self.hass.data.get(DOMAIN, {})
            for edata in domain_data.values():
                if not isinstance(edata, dict):
                    continue
                entity = edata.get("entity_map", {}).get(node_id)
                if entity is not None:
                    entity.update_sync_status(success)
                    return
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug(
                "Could not update entity sync status for node %s: %s",
                node_id,
                err,
            )

    def _get_cached_node(self, node_id: int) -> dict[str, Any] | None:
        """Return cached node info for a node id."""
        return next((n for n in self._nodes_cache if n.get("node_id") == node_id), None)

    def _get_node_name(self, node_id: int) -> str:
        """Return a stable display name for a node id."""
        node = self._get_cached_node(node_id)
        if node and node.get("name"):
            return str(node["name"])
        return f"Node {node_id}"

    def _sync_result(
        self,
        node_id: int,
        success: bool,
        reason: str | None = None,
        details: str | None = None,
        failed_command: str | None = None,
    ) -> SyncTimeResult:
        """Build a sync result with the current node name."""
        return SyncTimeResult(
            success=success,
            node_name=self._get_node_name(node_id),
            reason=reason,
            details=details,
            failed_command=failed_command,
        )

    def _command_result_to_sync_result(
        self, node_id: int, command_name: str, result: CommandResult
    ) -> SyncTimeResult:
        """Convert a failed command into a sync result."""
        self._log_command_failure_debug(node_id, command_name, result)
        return self._sync_result(
            node_id,
            success=False,
            reason=result.reason or SYNC_FAILURE_COMMAND_FAILED,
            details=result.details,
            failed_command=command_name,
        )

    def _log_command_failure_debug(
        self, node_id: int, command_name: str, result: CommandResult
    ) -> None:
        """Log raw Matter command failures only at debug level."""
        if result.success:
            return
        _LOGGER.debug(
            "Command %s failed for node %s: reason=%s, error_code=%s, details=%s",
            command_name,
            node_id,
            result.reason,
            result.error_code,
            result.details,
        )

    def _classify_server_error(self, details: str | None) -> str:
        """Map Matter server error details to a stable sync failure reason."""
        details_lower = (details or "").lower()
        if (
            "operation aborted" in details_lower
            or "not (yet) available" in details_lower
            or "unavailable" in details_lower
            or "offline" in details_lower
        ):
            return SYNC_FAILURE_DEVICE_UNAVAILABLE
        return SYNC_FAILURE_COMMAND_FAILED

    def _is_terminal_sync_failure(self, reason: str | None) -> bool:
        """Return True when the sync should stop after a failure."""
        return reason in _TERMINAL_SYNC_FAILURES

    # ------------------------------------------------------------------
    # Time synchronisation
    # ------------------------------------------------------------------

    async def async_sync_time(self, node_id: int, endpoint: int | None = None) -> bool:
        """Backward-compatible bool wrapper for time sync."""
        result = await self.async_sync_time_result(node_id, endpoint)
        return result.success

    async def async_sync_time_result(
        self, node_id: int, endpoint: int | None = None
    ) -> SyncTimeResult:
        """Sync time on a Matter device.

        Pass endpoint=None to auto-detect the correct endpoint.
        """
        lock = self._per_node_sync_locks.setdefault(node_id, asyncio.Lock())

        async def _acquire_and_sync() -> SyncTimeResult:
            async with lock:
                return await self._do_sync_time(node_id, endpoint)

        try:
            return await asyncio.wait_for(_acquire_and_sync(), timeout=20)
        except asyncio.TimeoutError:
            return self._sync_result(
                node_id,
                success=False,
                reason=SYNC_FAILURE_TIMEOUT,
                details="Sync timed out after 20s",
            )

    async def _do_sync_time(
        self, node_id: int, endpoint: int | None = None
    ) -> SyncTimeResult:
        """Internal method to perform time sync (called within lock)."""
        _LOGGER.debug("Starting time sync for node %s (endpoint %s)", node_id, endpoint)

        # Ensure we have node info for endpoint auto-selection
        if not self._nodes_cache:
            await self.async_get_matter_nodes()

        endpoint_id = endpoint
        if endpoint_id is None:
            node = next(
                (n for n in self._nodes_cache if n.get("node_id") == node_id),
                None,
            )
            endpoints = (node or {}).get("time_sync_endpoints") or []
            if endpoints:
                endpoint_id = endpoints[0]
            else:
                endpoint_id = 0  # Fallback when no endpoints are known

            _LOGGER.debug(
                "Auto-detected Time Sync endpoint %s for node %s",
                endpoint_id,
                node_id,
            )

            # Only fetch diagnostics when debug logging is active —
            # avoids an extra get_nodes round-trip during normal operation
            if _LOGGER.isEnabledFor(logging.DEBUG):
                try:
                    time_sync_attrs = await asyncio.wait_for(
                        self.async_get_time_sync_cluster_info(node_id, endpoint_id),
                        timeout=5,
                    )
                    if time_sync_attrs:
                        _LOGGER.debug(
                            "Node %s endpoint %s Time Sync cluster attributes: %s",
                            node_id,
                            endpoint_id,
                            time_sync_attrs,
                        )
                    else:
                        _LOGGER.debug(
                            "Node %s endpoint %s: No Time Sync cluster attributes found",
                            node_id,
                            endpoint_id,
                        )
                except asyncio.TimeoutError:
                    _LOGGER.debug(
                        "Timeout getting Time Sync attributes for node %s (non-critical)",
                        node_id,
                    )
                except Exception as err:
                    _LOGGER.debug(
                        "Could not get Time Sync attributes for node %s: %s (non-critical)",
                        node_id,
                        err,
                    )

        try:
            tz = ZoneInfo(self._timezone)
        except Exception:
            _LOGGER.warning("Invalid timezone %s, using UTC", self._timezone)
            tz = ZoneInfo("UTC")

        now = datetime.now(tz)
        utc_now = now.astimezone(ZoneInfo("UTC"))

        # Total UTC offset in seconds (includes DST when applicable)
        total_offset = int(now.utcoffset().total_seconds()) if now.utcoffset() else 0

        # FORCE DST TO 0 (merge DST into utc_offset)
        utc_offset = total_offset
        dst_offset = 0

        # Matter Time Sync uses CHIP epoch (2000-01-01) in microseconds
        utc_microseconds = _to_chip_epoch_us(utc_now)

        _LOGGER.info(
            "Syncing time for node %s: local=%s, UTC=%s, offset=%ds, DST=%ds (forced to 0)",
            node_id,
            now.isoformat(),
            utc_now.isoformat(),
            utc_offset,
            dst_offset,
        )

        # ---------------------------------------------------------
        # 1) Set TimeZone FIRST
        #    camelCase keys: the Matter server expects this format
        # ---------------------------------------------------------
        tz_list = [{"offset": utc_offset, "validAt": 0}]

        tz_response = await self._async_send_command(
            "device_command",
            {
                "node_id": node_id,
                "endpoint_id": endpoint_id,
                "cluster_id": TIME_SYNC_CLUSTER_ID,
                "command_name": "SetTimeZone",
                "payload": {"timeZone": tz_list},
            },
        )

        if tz_response.success:
            _LOGGER.debug(
                "SetTimeZone successful for node %s (offset=%d)",
                node_id,
                utc_offset,
            )
        else:
            tz_result = self._command_result_to_sync_result(
                node_id, "SetTimeZone", tz_response
            )
            if self._is_terminal_sync_failure(tz_result.reason):
                return tz_result
            _LOGGER.debug(
                "SetTimeZone failed for node %s with reason=%s (continuing)",
                node_id,
                tz_result.reason,
            )

        # ---------------------------------------------------------
        # 2) Set DST Offset SECOND
        #    "DSTOffset" outer key stays PascalCase (server expects it)
        #    Inner keys use camelCase
        # ---------------------------------------------------------
        far_future_us = _to_chip_epoch_us(utc_now + timedelta(days=365))

        dst_list = [
            {
                "offset": dst_offset,
                "validStarting": 0,
                "validUntil": far_future_us,
            }
        ]

        dst_response = await self._async_send_command(
            "device_command",
            {
                "node_id": node_id,
                "endpoint_id": endpoint_id,
                "cluster_id": TIME_SYNC_CLUSTER_ID,
                "command_name": "SetDSTOffset",
                "payload": {"DSTOffset": dst_list},
            },
        )

        if dst_response.success:
            _LOGGER.debug("SetDSTOffset successful for node %s", node_id)
        else:
            dst_result = self._command_result_to_sync_result(
                node_id, "SetDSTOffset", dst_response
            )
            if self._is_terminal_sync_failure(dst_result.reason):
                return dst_result
            _LOGGER.debug(
                "SetDSTOffset not supported or failed for node %s with reason=%s (continuing)",
                node_id,
                dst_result.reason,
            )

        # ---------------------------------------------------------
        # 3) Set UTC Time LAST
        #    "UTCTime" stays PascalCase (server expects it)
        #    "granularity" uses camelCase
        # ---------------------------------------------------------
        payload_utc = {
            "UTCTime": utc_microseconds,
            "granularity": 4,
        }

        _LOGGER.debug(
            "Trying SetUTCTime for node %s, endpoint %s: %s",
            node_id,
            endpoint_id,
            payload_utc,
        )
        time_response = await self._async_send_command(
            "device_command",
            {
                "node_id": node_id,
                "endpoint_id": endpoint_id,
                "cluster_id": TIME_SYNC_CLUSTER_ID,
                "command_name": "SetUTCTime",
                "payload": payload_utc,
            },
        )

        if not time_response.success:
            return self._command_result_to_sync_result(
                node_id, "SetUTCTime", time_response
            )

        _LOGGER.debug("SetUTCTime successful for node %s", node_id)

        _LOGGER.info(
            "Time synced for node %s: %s (UTC offset: %d, DST: %d)",
            node_id,
            now.isoformat(),
            utc_offset,
            dst_offset,
        )
        return self._sync_result(node_id, success=True)

    # ------------------------------------------------------------------
    # Bulk sync
    # ------------------------------------------------------------------

    async def async_sync_all_devices(self) -> dict[str, Any]:
        """Sync time on all filtered devices.

        Returns:
            Dict with sync statistics:
            {"success": int, "failed": int, "skipped": int, "errors": list}
        """
        if self._auto_sync_running:
            _LOGGER.warning("Auto-sync already running, skipping this trigger")
            return {"success": 0, "failed": 0, "skipped": 0, "errors": ["Already running"]}

        async with self._auto_sync_lock:
            if self._auto_sync_running:
                _LOGGER.warning("Auto-sync already running (race condition), skipping")
                return {"success": 0, "failed": 0, "skipped": 0, "errors": ["Already running"]}
            self._auto_sync_running = True
            _LOGGER.debug("Auto-sync started, flag set")

        try:
            if not self.is_connected:
                _LOGGER.debug("Connection lost, reconnecting for auto-sync")
                if not await self.async_connect(log_failure=False):
                    _LOGGER.error("Failed to connect to Matter Server for auto-sync")
                    return {
                        "success": 0,
                        "failed": 0,
                        "skipped": 0,
                        "errors": ["Failed to connect"],
                    }

            nodes = await self.async_get_matter_nodes()
            if not nodes:
                _LOGGER.warning("No Matter nodes found")
                return {"success": 0, "failed": 0, "skipped": 0, "errors": ["No nodes found"]}

            _LOGGER.debug("Auto-sync: %d devices", len(nodes))

            stats: dict[str, Any] = {"success": 0, "failed": 0, "skipped": 0, "errors": []}

            async def _sync_all() -> None:
                device_filters_raw = self.entry.data.get("device_filter", "")
                device_filter_list = [
                    t.strip().lower()
                    for t in device_filters_raw.split(",")
                    if t.strip()
                ]
                only_time_sync = self.entry.data.get("only_time_sync_devices", True)
                filter_target = self.entry.data.get(
                    CONF_FILTER_TARGET, DEFAULT_FILTER_TARGET
                )

                for node in nodes:
                    node_id = node.get("node_id")
                    node_name = node.get("name", f"Node {node_id}")
                    has_time_sync = node.get("has_time_sync", False)

                    if only_time_sync and not has_time_sync:
                        stats["skipped"] += 1
                        _LOGGER.debug(
                            "Skipping node %s (%s) - no Time Sync cluster",
                            node_id,
                            node_name,
                        )
                        continue

                    candidates = filter_candidates_for_node(node, filter_target)
                    if not device_matches_filter(device_filter_list, candidates):
                        stats["skipped"] += 1
                        _LOGGER.debug(
                            "Skipping node %s (%s) - filtered out",
                            node_id,
                            node_name,
                        )
                        continue

                    _LOGGER.info("Auto-syncing node %s (%s)", node_id, node_name)
                    try:
                        result = await self.async_sync_time_result(node_id)
                        if result.success:
                            stats["success"] += 1
                            _LOGGER.debug("✓ Node %s synced successfully", node_id)
                        else:
                            stats["failed"] += 1
                            error_msg = (
                                f"Node {node_id} ({node_name}): "
                                f"{result.reason or SYNC_FAILURE_COMMAND_FAILED}"
                            )
                            stats["errors"].append(error_msg)
                            log_sync_failure(
                                node_id,
                                result.node_name or node_name,
                                result.reason or SYNC_FAILURE_COMMAND_FAILED,
                            )

                        # Update button entity attributes with sync result
                        self._update_entity_sync_status(node_id, result.success)

                    except Exception as err:
                        stats["failed"] += 1
                        error_msg = f"Node {node_id} ({node_name}): {err}"
                        stats["errors"].append(error_msg)
                        _LOGGER.error(
                            "✗ Exception syncing node %s (%s): %s",
                            node_id,
                            node_name,
                            err,
                            exc_info=True,
                        )

                        # Update button entity with failure status
                        self._update_entity_sync_status(node_id, False)

                _LOGGER.info(
                    "Auto-sync completed: %d successful, %d failed, %d skipped",
                    stats["success"],
                    stats["failed"],
                    stats["skipped"],
                )

            await asyncio.wait_for(_sync_all(), timeout=120)
            return stats

        except asyncio.TimeoutError:
            _LOGGER.error(
                "Auto-sync exceeded 120s timeout! This may indicate connectivity issues."
            )
            return {"success": 0, "failed": 0, "skipped": 0, "errors": ["Timeout after 120s"]}
        except Exception as err:
            _LOGGER.error("Auto-sync failed with unexpected error: %s", err, exc_info=True)
            return {"success": 0, "failed": 0, "skipped": 0, "errors": [str(err)]}
        finally:
            async with self._auto_sync_lock:
                self._auto_sync_running = False
                _LOGGER.debug("Auto-sync finished, flag cleared")
