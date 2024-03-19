import httpx
import logging

timeout_duration = 30

from constants import sick_leave_categories


async def fetch_sick_ids(
    client_id, access_token, api_endpoint, start_date, end_date, company
):
    id_list = []
    sick_leave_ids = sick_leave_categories[company]
    for sick_type_id in sick_leave_ids:
        async with httpx.AsyncClient(timeout=timeout_duration) as client:
            sick_headers = {
                "X-ClientId": client_id,
                "Authorization": "Bearer " + access_token,
                "Content-Type": "application/json",
            }
            sick_endpoint = f"{api_endpoint}/scheduling/v1/shifts?shifttypeId={sick_type_id}&from={start_date}&to={end_date}"
            sick_response = await client.get(sick_endpoint, headers=sick_headers)
            if sick_response.status_code != 200:
                logging.info(
                    f"Error: {sick_response.status_code} - {sick_response.text}"
                )
                continue  # Skip to the next shift if there's an error

            sick_data_json = sick_response.json()
            sick_data = sick_data_json["data"]
            if len(sick_data) == 0:
                continue
            for instance in sick_data:
                sick_shift_id = instance["id"]
                if sick_shift_id in id_list:
                    continue
                id_list.append(sick_shift_id)
    return id_list
