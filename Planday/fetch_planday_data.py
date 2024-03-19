import logging
import httpx
from datetime import datetime, timedelta
import pandas as pd

from utils import timedelta_to_str, ensure_timedelta, ensure_int
from constants import list_to_ignore
from Planday.fetch_sick_guys import fetch_sick_ids
from Planday.fetch_fisketorget_stavanger import fetch_fisketorget


async def fetch_planday(client_id, refresh_token, company):
    today_date = datetime.today()
    start_date = (today_date - timedelta(weeks=2)).strftime("%Y-%m-%d")
    end_date = (today_date + timedelta(weeks=2)).strftime("%Y-%m-%d")
    # start_date = "2024-01-01"
    # end_date = "2024-02-01"
    token_endpoint = "https://id.planday.com"
    api_endpoint = "https://openapi.planday.com"
    timeout_duration = 30.0
    access_token_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    acess_token_data = {
        "client_id": client_id,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    access_token_endpoint = f"{token_endpoint}/connect/token"
    async with httpx.AsyncClient(timeout=timeout_duration) as client:
        acess_token_response = await client.post(
            access_token_endpoint,
            headers=access_token_headers,
            data=acess_token_data,
        )
        # Variables storing for appropriate API response
        access_token = None
        dep_data = []
        actual_data = {}

        if acess_token_response.status_code == 200:
            access_token_json = acess_token_response.json()
            access_token = access_token_json["access_token"]
        else:
            logging.info(
                f"Error: {acess_token_response.status_code} - {acess_token_response.text}"
            )
            return

        if access_token:
            dept_headers = {
                "X-ClientId": client_id,
                "Authorization": "Bearer " + access_token,
            }
            department_endpoint = f"{api_endpoint}/hr/v1/departments"
            dept_response = await client.get(department_endpoint, headers=dept_headers)

            if dept_response.status_code != 200:
                logging.info(
                    f"Error: {dept_response.status_code} - {dept_response.text}"
                )
                return

            dept_json_response = dept_response.json()
            if not dept_json_response["paging"]["total"]:
                logging.info("No departments found.")
                return

            departments = dept_json_response["data"]
            sick_shifts = await fetch_sick_ids(
                client_id, access_token, api_endpoint, start_date, end_date, company
            )
            for department_data in departments:
                actual_data = {}
                dep_id = department_data["id"]
                dep_name = department_data["name"]

                if dep_id in list_to_ignore:
                    continue
                if dep_name == "Fisketorget Stavanger AS":
                    actual_data = await fetch_fisketorget(
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
                    )
                    continue

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
                    continue  # Skip to the next department if there's an error

                time_and_cost_json_response = time_and_cost_response.json()
                time_and_cost_data = time_and_cost_json_response["data"]["costs"]

                for data in time_and_cost_data:
                    date_key = data["date"]
                    duration = ensure_timedelta(data.get("duration", timedelta(0)))
                    cost = ensure_int(data.get("cost", 0))

                    # now fetch sections and check if the position in 'positionId' is in the fetched position then change dep_name to that name and add it to actual_data
                    # 2348, 2134 sections are
                    if date_key not in actual_data:
                        actual_data[date_key] = {
                            "duration": timedelta(0),
                            "dep_name": dep_name,
                            "cost": 0,
                            "sick_cost": 0,
                        }
                    shift_id = data.get("shiftId", None)
                    if not shift_id:
                        actual_data[date_key]["duration"] += duration
                        actual_data[date_key]["cost"] += cost
                        continue
                    shift_status_url = f"{api_endpoint}/scheduling/v1/shifts/{shift_id}"

                    shift_status_response = await client.get(
                        shift_status_url, headers=dept_headers
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

                # Convert actual_data to a list of dicts including the department name
                department_entries = [
                    {
                        "Restaurant": values["dep_name"],
                        "Date": date,
                        "Duration": timedelta_to_str(values["duration"]),
                        "Cost": values["cost"],
                        "Sick_Cost": values["sick_cost"],
                    }
                    for date, values in actual_data.items()
                ]

                # Create a DataFrame for the current department's data
                if department_entries:
                    df = pd.DataFrame(department_entries)
                    dep_data.append(df)

            if dep_data:
                combined_df = pd.concat(dep_data, ignore_index=True)
                return combined_df
            else:
                return pd.DataFrame()
        else:
            logging.info("Error: Did not get the access token")
            return
