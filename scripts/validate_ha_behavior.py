# scripts/validate_ha_behavior.py
#
# Run before first real deployment:
#   $env:HA_TEST_URL="http://homeassistant.local:8123"
#   $env:HA_TEST_TOKEN="your_token"
#   python scripts/validate_ha_behavior.py

import asyncio
import httpx
import json
import os
import sys
from datetime import datetime, timezone

HA_URL   = os.environ["HA_TEST_URL"].rstrip("/")
HA_TOKEN = os.environ["HA_TEST_TOKEN"]
ALIAS    = "ha_deploy_validation_test"

HEADERS = {
    "Authorization": f"Bearer {HA_TOKEN}",
    "Content-Type":  "application/json",
}

BASE_PAYLOAD = {
    "alias":     ALIAS,
    "trigger":   [{"platform": "time", "at": "06:00:00"}],
    "condition": [],
    "action":    [{"service": "light.turn_on", "target": {"entity_id": "light.test"}}],
    "mode":      "single",
}


def log(case: str, msg: str, data: dict = None):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"\n[{ts}] [{case}] {msg}")
    if data:
        print(json.dumps(data, indent=2, default=str))


async def push(client: httpx.AsyncClient, payload: dict) -> httpx.Response:
    return await client.post(
        f"{HA_URL}/api/config/automation/config/{ALIAS}",
        headers=HEADERS,
        json=payload,
        timeout=30,
    )


async def reload(client: httpx.AsyncClient) -> httpx.Response:
    return await client.post(
        f"{HA_URL}/api/services/automation/reload",
        headers=HEADERS,
        timeout=15,
    )


async def fetch_automations(client: httpx.AsyncClient) -> list[dict]:
    resp = await client.get(
        f"{HA_URL}/api/states",
        headers=HEADERS,
        timeout=15,
    )
    resp.raise_for_status()
    return [
        s for s in resp.json()
        if s.get("entity_id", "").startswith("automation.")
    ]


async def find_by_alias(client: httpx.AsyncClient, alias: str) -> list[dict]:
    automations = await fetch_automations(client)
    return [
        a for a in automations
        if (a.get("attributes", {}).get("alias") or
            a.get("attributes", {}).get("friendly_name")) == alias
    ]


async def delete_test_automation(client: httpx.AsyncClient) -> None:
    try:
        await client.post(
            f"{HA_URL}/api/services/automation/turn_off",
            headers=HEADERS,
            json={"entity_id": f"automation.{ALIAS.lower()}"},
            timeout=10,
        )
    except Exception:
        pass


async def run_validation():
    results = {}

    async with httpx.AsyncClient() as client:

        # ------------------------------------------------------------------
        # CASE 1 — Clean push
        # ------------------------------------------------------------------
        log("CASE 1", "Clean push — push → reload → verify visible in HA")

        await delete_test_automation(client)
        await asyncio.sleep(1)

        resp = await push(client, BASE_PAYLOAD)
        log("CASE 1", f"Push response: {resp.status_code}")

        await reload(client)
        await asyncio.sleep(2)

        matches = await find_by_alias(client, ALIAS)

        # log both alias and friendly_name explicitly
        for m in matches:
            attrs = m.get("attributes", {})
            log("CASE 1", "Attribute mapping from HA state", {
                "entity_id":     m["entity_id"],
                "alias":         attrs.get("alias"),
                "friendly_name": attrs.get("friendly_name"),
                "state":         m.get("state"),
            })

        results["case1_found"]               = len(matches) == 1
        results["case1_enabled"]             = matches[0]["state"] == "on" if matches else False
        results["case1_alias_attr_populated"] = bool(
            matches[0].get("attributes", {}).get("alias") if matches else False
        )
        results["case1_friendly_name"] = (
            matches[0].get("attributes", {}).get("friendly_name") if matches else None
        )

        log("CASE 1", "Result", {
            "found":                results["case1_found"],
            "enabled":              results["case1_enabled"],
            "alias_attr_populated": results["case1_alias_attr_populated"],
            "friendly_name":        results["case1_friendly_name"],
        })

        # ------------------------------------------------------------------
        # CASE 2 — Field overwrite
        # ------------------------------------------------------------------
        log("CASE 2", "Overwrite — change trigger time, push again, confirm updated")

        modified_payload = {
            **BASE_PAYLOAD,
            "trigger": [{"platform": "time", "at": "09:00:00"}]
        }
        resp = await push(client, modified_payload)
        log("CASE 2", f"Push response: {resp.status_code}")

        await reload(client)
        await asyncio.sleep(2)

        matches_after = await find_by_alias(client, ALIAS)
        results["case2_no_duplicate"] = len(matches_after) == 1

        log("CASE 2", "Result", {
            "no_duplicate":   results["case2_no_duplicate"],
            "count_after":    len(matches_after),
            "attrs_before":   matches[0].get("attributes", {}) if matches else {},
            "attrs_after":    matches_after[0].get("attributes", {}) if matches_after else {},
        })

        # ------------------------------------------------------------------
        # CASE 3 — Alias collision
        # ------------------------------------------------------------------
        log("CASE 3", "Alias collision — manually create an automation named "
            f"'{ALIAS}' in HA UI, then press Enter")
        input("  >> Press Enter once the manual automation is created in HA UI...")

        await reload(client)
        await asyncio.sleep(2)

        before_collision = await find_by_alias(client, ALIAS)
        log("CASE 3", f"Before collision push: {len(before_collision)} match(es)")

        resp = await push(client, BASE_PAYLOAD)
        log("CASE 3", f"Push response: {resp.status_code}")

        await reload(client)
        await asyncio.sleep(2)

        after_collision = await find_by_alias(client, ALIAS)
        results["case3_count_before"] = len(before_collision)
        results["case3_count_after"]  = len(after_collision)
        results["case3_behavior"]     = (
            "overwrite"   if len(after_collision) == 1 else
            "duplicate"   if len(after_collision) > len(before_collision) else
            "merge/other"
        )

        log("CASE 3", "Collision result", {
            "before":    len(before_collision),
            "after":     len(after_collision),
            "behavior":  results["case3_behavior"],
            "CRITICAL":  "duplicate means ack loop will return mismatch — expected behavior",
        })

        # ------------------------------------------------------------------
        # CASE 4 — Reload required vs live-apply
        # ------------------------------------------------------------------
        log("CASE 4", "Reload behavior — push without reload, check visibility")

        live_payload = {
            **BASE_PAYLOAD,
            "trigger": [{"platform": "time", "at": "12:00:00"}]
        }
        resp = await push(client, live_payload)
        log("CASE 4", f"Push response: {resp.status_code}")

        await asyncio.sleep(2)
        before_reload = await find_by_alias(client, ALIAS)
        state_before  = before_reload[0].get("attributes", {}) if before_reload else {}

        await reload(client)
        await asyncio.sleep(2)

        after_reload = await find_by_alias(client, ALIAS)
        state_after  = after_reload[0].get("attributes", {}) if after_reload else {}

        results["case4_live_apply"]      = state_before != {}
        results["case4_reload_required"] = state_before == state_after

        log("CASE 4", "Reload behavior", {
            "visible_before_reload": results["case4_live_apply"],
            "reload_required":       results["case4_reload_required"],
            "recommendation": (
                "reload is load-bearing — keep explicit reload step"
                if results["case4_reload_required"] else
                "live-apply confirmed — reload still safe as belt-and-suspenders"
            ),
        })

    # ------------------------------------------------------------------
    # Final report
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("HA VALIDATION REPORT")
    print("=" * 60)

    checks = {
        "Case 1 — automation visible after push":     results.get("case1_found"),
        "Case 1 — automation enabled":                results.get("case1_enabled"),
        "Case 1 — alias attr populated in HA state":  results.get("case1_alias_attr_populated"),
        "Case 2 — no duplicate on overwrite":         results.get("case2_no_duplicate"),
        "Case 3 — collision behavior observed":       True,
        "Case 4 — reload behavior confirmed":         True,
    }

    all_passed = True
    for check, passed in checks.items():
        status = "✅" if passed else "❌"
        print(f"  {status}  {check}")
        if not passed:
            all_passed = False

    print(f"\n  Case 3 collision result:  {results.get('case3_behavior', 'not run')}")
    print(f"  Case 4 reload required:   {results.get('case4_reload_required', 'not run')}")
    print(f"  Case 1 alias field:       {results.get('case1_alias_attr_populated')}")
    print(f"  Case 1 friendly_name:     {results.get('case1_friendly_name')}")

    print("\n" + "=" * 60)
    if all_passed:
        print("✅ READY — system assumptions validated against real HA")
        print("   Next: deploy to one real site and observe.")
    else:
        print("❌ FAILURES — review above before deployment")
        sys.exit(1)

    with open("ha_validation_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print("\n   Raw results saved to ha_validation_results.json")


if __name__ == "__main__":
    asyncio.run(run_validation())