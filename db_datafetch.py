import psycopg2
import pandas as pd
import time
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()


def dbDataFetch(fetching_role, agency_id, retries=3):
    connection = None
    attempt = 0

    while attempt < retries:
        try:
            # Connect to PostgreSQL (using environment variables)
            connection = psycopg2.connect(
                host=os.getenv("DB_HOST"),
                database=os.getenv("DB_NAME"),
                port=int(os.getenv("DB_PORT")),  # Convert port to int
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
            )

            cursor = connection.cursor()

            # First query: Fetch locations
            loc_query = (
                "SELECT T.id, U.name as Upazilla, T.name as Thana FROM ubl.locations T Join ubl.locations U on T.parent = U.id where U.type = 3 AND U.is_deleted = False and T.type = 4 AND T.is_deleted=FALSE"
            )
            cursor.execute(loc_query)
            loc_results = cursor.fetchall()
            loc_columns = [desc[0] for desc in cursor.description]
            loc = pd.DataFrame(loc_results, columns=loc_columns)

            # Second query: Fetch users based on fetching_role and agency_id
            user_query = f"SELECT u.id, u.username, ui.full_name, loc.name as location FROM ubl.users u, ubl.user_infos ui, ubl.role_location_maps rlm, ubl.locations loc, ubl.user_agency_maps uam WHERE rlm.assigned_to = u.id AND ui.user_id = u.id AND rlm.is_current = TRUE AND rlm.assigned_role = {fetching_role} AND uam.user_id = u.id AND uam.is_deleted = FALSE AND uam.agency_id = {agency_id} AND loc.type = 4 AND loc.is_deleted = FALSE"
            cursor.execute(user_query)
            user_results = cursor.fetchall()
            user_columns = [desc[0] for desc in cursor.description]
            user = pd.DataFrame(user_results, columns=user_columns)

            # loc.to_csv("ubl_loc.csv", index=False)
            # user.to_csv("ubl_user.csv", index=False)
            return loc, user

        except (Exception, psycopg2.Error) as error:
            print(f"Attempt {attempt + 1}: Error while connecting to Database:", error)
            attempt += 1
            time.sleep(3)

        finally:
            if connection:
                cursor.close()
                connection.close()

    # Return empty DataFrames if all retries fail
    print("Failed to connect after several attempts.")
    return pd.DataFrame(), pd.DataFrame()
