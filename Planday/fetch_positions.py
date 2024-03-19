import logging
import httpx
from constants import utsalg_sections


async def fetch_positions(client_id, access_token, api_endpoint, client):
    valid_positions = []
    position_headers = {
        "X-ClientId": client_id,
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    position_url = f"{api_endpoint}/scheduling/v1/positions"
    position_response = await client.get(position_url, headers=position_headers)

    if position_response.status_code != 200:
        logging.info(
            f"Error: {position_response.status_code} - {position_response.text}"
        )
        return

    position_response_response = position_response.json()
    position_data = position_response_response["data"]
    for data in position_data:
        try:
            sectionId = data["sectionId"]
        except:
            continue
        if sectionId in utsalg_sections:
            valid_positions.append(data["id"])
    return valid_positions
