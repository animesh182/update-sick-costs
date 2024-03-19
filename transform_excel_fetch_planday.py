import os
import pandas as pd
import uuid
from datetime import datetime
from constants import restaurantnames
from utils import convert_to_decimal_hours
import logging


def transform_planday(data, company):
    # Create a new DataFrame for transformed data
    transformed_data = pd.DataFrame()

    # Transform the date column
    transformed_data["date"] = pd.to_datetime(data["Date"])
    transformed_data["sick_employee_cost"] = data["Sick_Cost"].astype(float)
    # Add the company column from the original data
    transformed_data["restaurant"] = data["Restaurant"].apply(
        lambda x: restaurantnames.get(x, x)
    )
    transformed_data["company"] = company
    transformed_data["id"] = [uuid.uuid4() for _ in range(len(transformed_data))]
    logging.info("Data transformed to Krunch format")
    return transformed_data
