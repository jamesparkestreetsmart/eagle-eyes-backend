# scripts/cleanup_ha_automations.py
# Deletes ghost automations by entity_id suffix pattern

import asyncio
import httpx
import os
from urllib.parse import quote
from dotenv import load_dotenv
load_dotenv()

HA_URL   = "http://homeassistant.local:8123"
HA_TOKEN = os.environ["HA_TEST_TOKEN"]

HEADERS = {
    "Authorization": f"Bearer {HA_TOKEN}",
    "Content-Type":  "application/json",
}

# These are the ghost entity_ids with _2 / _3 suffixes
# Internal automation IDs match the entity suffix after "automation."
GHOST_IDS = [
    "eagle_eyes_entity_discovery_push_2",
    "eagle_eyes_use_fallback_manifest_2",
    "eagle_eyes_receive_daily_manifest_3",
    "ha_deploy_validation_test_2",
    "eagle_eyes_reset_manifest_flag_at_midnight_2",
    "manifest_closed_day_set_unoccupied_2",
    "manifest_log_received_summary_2",
    # also kill the base ones with old-style IDs that keep surviving
    "eagle_eyes_reset_manifest_flag_at_midnight",
    "manifest_closed_day_set_unoccupied",
    "manifest_log_received_summary",
    "eagle_eyes_log_sensor_states",
    "eagle_eyes_entity_discovery_push_2",
    "eagle_eyes_use_fallback_manifest_2",
    "eagle_eyes_receive_daily_manifest_3",
    "eagle_eyes_receive_daily_manifest",
    "eagle_eyes_use_fallback_manifest",
    "eagle_eyes_entity_discovery_push",
]


async def main():
    async with httpx.AsyncClient() as client:
        # first fetch all to get real internal IDs
        r = await client.get(
            f"{HA_URL}/api/states",
            headers=HEADERS,
            timeout=15,
        )
        states = [s for s in r.json() if s.get("entity_id","").startswith("automation.")]

        print(f"Found {len(states)} automations in HA\n")

        deleted = 0
        kept    = 0

        for s in states:
            entity_id = s["entity_id"]
            attrs     = s.get("attributes", {})
            auto_id   = attrs.get("id", "")
            fname     = attrs.get("friendly_name", "")

            # delete anything that is NOT ha_deploy_validation_test
            # and NOT one of our clean managed IDs
            clean_ids = {
                "ha_deploy_validation_test",
            }

            if auto_id in clean_ids:
                print(f"KEEP  {entity_id} | id={repr(auto_id)}")
                kept += 1
                continue

            encoded = quote(auto_id, safe="")
            resp = await client.delete(
                f"{HA_URL}/api/config/automation/config/{encoded}",
                headers=HEADERS,
                timeout=10,
            )
            print(f"DELETE {resp.status_code} | {entity_id} | id={repr(auto_id)} | {repr(fname)}")
            if resp.status_code == 200:
                deleted += 1

        print(f"\nDeleted: {deleted} | Kept: {kept}")

        await client.post(
            f"{HA_URL}/api/services/automation/reload",
            headers=HEADERS,
            timeout=15,
        )
        print("Reload triggered.")


if __name__ == "__main__":
    asyncio.run(main())