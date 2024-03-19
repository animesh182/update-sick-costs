import logging
from datetime import timedelta, datetime
from utils import ensure_int, ensure_timedelta, timedelta_to_str
from Planday.fetch_positions import fetch_positions
import pandas as pd


async def fetch_fisketorget(
    client_id,
    access_token,
    api_endpoint,
    dep_id,
    start_date,
    end_date,
    client,
    today_date,
    sick_shifts,
    dep_data,
):
    valid_positions = await fetch_positions(
        client_id, access_token, api_endpoint, client
    )
    time_and_cost_headers = {
        "X-ClientId": client_id,
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    time_and_cost_url = f"{api_endpoint}/scheduling/v1/timeandcost/{dep_id}?from={start_date}&to={end_date}"
    time_and_cost_response = await client.get(
        time_and_cost_url, headers=time_and_cost_headers
    )

    if time_and_cost_response.status_code != 200:
        logging.info(
            f"Error: {time_and_cost_response.status_code} - {time_and_cost_response.text}"
        )
        return

    time_and_cost_json_response = time_and_cost_response.json()
    time_and_cost_data = time_and_cost_json_response["data"]["costs"]
    utsalg_data = []
    restaurant_data = []
    for data in time_and_cost_data:
        try:
            positionId = data["positionId"]
            if positionId in valid_positions:
                utsalg_data.append(data)
            else:
                restaurant_data.append(data)
        except:
            restaurant_data.append(data)

    async def fetch_data(data, department_name):
        actual_data = {}
        for data in data:
            date_key = data["date"]
            duration = ensure_timedelta(data.get("duration", timedelta(0)))
            cost = ensure_int(data.get("cost", 0))

            if date_key not in actual_data:
                actual_data[date_key] = {
                    "dep_name": department_name,
                    "duration": timedelta(0),
                    "cost": 0,
                    "sick_cost": 0,
                }
            shift_id = data.get("shiftId", None)
            if not shift_id:
                actual_data[date_key]["duration"] += duration
                actual_data[date_key]["cost"] += cost
                continue
            shift_status_url = f"{api_endpoint}/scheduling/v1/shifts/{shift_id}"
            shift_headers = {
                "X-ClientId": client_id,
                "Authorization": "Bearer " + access_token,
            }
            shift_status_response = await client.get(
                shift_status_url, headers=shift_headers
            )

            if shift_status_response.status_code != 200:
                logging.info(
                    f"Error: {shift_status_response.status_code} - {shift_status_response.text}"
                )
                continue  # Skip to the next shift if there's an error

            shift_status_data = shift_status_response.json()
            shift_status = shift_status_data["data"]["status"]
            shift_id = shift_status_data["data"]["id"]
            if shift_id in sick_shifts:
                actual_data[date_key]["sick_cost"] += cost
            # one_week_from_today = datetime.now() - timedelta(days=7)
            one_week_from_today = today_date - timedelta(weeks=1)
            # six_days_ago = today_date - timedelta(days=6)

            # if not shift_status == "Approved":
            #     continue

            date_key_datetime = datetime.strptime(
                date_key, "%Y-%m-%d"
            )  # Adjust the format string as per your date format

            if date_key_datetime <= one_week_from_today:
                if shift_status != "Approved":
                    # logging.info(f"Date {date_key} is within a week but status is not Approved.")
                    continue
                else:
                    actual_data[date_key]["duration"] += duration
                    actual_data[date_key]["cost"] += cost

            else:
                if shift_status == "Draft":
                    continue
                else:
                    actual_data[date_key]["duration"] += duration
                    actual_data[date_key]["cost"] += cost
        return actual_data

    restaurant_data = await fetch_data(restaurant_data, "Restaurant")
    utsalg_data = await fetch_data(utsalg_data, "Fisketorget Utsalg")
    restaurant_entries = [
        {
            "Restaurant": values["dep_name"],
            "Date": date,
            "Duration": timedelta_to_str(values["duration"]),
            "Cost": values["cost"],
            "Sick_Cost": values["sick_cost"],
        }
        for date, values in restaurant_data.items()
    ]
    utsalg_entries = [
        {
            "Restaurant": values["dep_name"],
            "Date": date,
            "Duration": timedelta_to_str(values["duration"]),
            "Cost": values["cost"],
            "Sick_Cost": values["sick_cost"],
        }
        for date, values in utsalg_data.items()
    ]
    # Create a DataFrame for the current department's data
    if restaurant_entries and utsalg_entries:
        restaurant_df = pd.DataFrame(restaurant_entries)
        utsalg_df = pd.DataFrame(utsalg_entries)
        dep_data.append(restaurant_df)
        dep_data.append(utsalg_df)
    return dep_data
