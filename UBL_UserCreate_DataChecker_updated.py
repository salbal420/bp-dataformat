import db_datafetch as datafetch
import db_datapush as datapush
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()


def userCreateDatacheck(given_data, agency_id, agency_name):

    df = pd.read_excel(given_data)
    df["agency_id"] = int(agency_id)

    role = df["Role"][0].strip().lower()
    role_mapping = {"bp": 4, "brand promoter": 4,
                    "sup": 3, "supervisor": 3}

    df["assigned_role"] = role_mapping.get(role, 0)
    df.loc[df["assigned_role"] == 0, "remark1"] = (
                "Wrong role found"
            )

     # Filter DataFrame for valid roles
    df_role_filtered = df[df["assigned_role"] != 0]

    assigned_role = df_role_filtered["assigned_role"][0]
    if assigned_role == 4:
        fetching_role = 3
    else:
        fetching_role = 9

        # Fetching data from the database based on the given file
    loc, user = datafetch.dbDataFetch(fetching_role, int(agency_id))

    if loc.empty or user.empty:
        print("There was a problem connecting to the database")
    else:
        

            # Comparing with database locations

            df_loc_dict = {
                (
                    row["Upazilla"].strip(),
                    row["Thana"].strip(),
                ): index
                for index, row in df.iterrows()
            }

            loc_dict = {
                (
                    row["upazilla"].strip(),
                    row["thana"].strip(),
                ): index
                for index, row in loc.iterrows()
            }

            not_found_loc_list = []

            # Iterate through df_dict and check for missing keys in user_dict
            for key, df_index in df_loc_dict.items():
                if key not in loc_dict:
                    # Append the non-matching row (key and df_index) to the list
                    not_found_loc_list.append((*key, df_index))

            # Convert the list to a DataFrame
            not_found_in_location = pd.DataFrame(
                not_found_loc_list, columns=["Upazilla", "Thana", "df_index"]
            )

            # Extract unique Report To values
            unique_not_found_loc = not_found_in_location[["Upazilla", "Thana"]].drop_duplicates()

            # Update remarks for users not found

            df.loc[
                df[["Upazilla", "Thana"]].apply(tuple, axis=1).isin(
                    unique_not_found_loc.apply(tuple, axis=1)
                    ),
                    "remark2"
            ] = "Location not found in Database"

            if not unique_not_found_loc.empty:
                print(
                    "The following locations are not found in database:\n",
                    unique_not_found_loc,
                )

            df_dict = {
                (
                    row["Report To"].strip(),
                    row["Thana"].strip(),
                ): index
                for index, row in df.iterrows()
            }

            user_dict = {
                (
                    row["full_name"].strip(),
                    row["location"].strip(),
                ): index
                for index, row in user.iterrows()
            }

            # Find non-matched Report To
            not_found_list = []

            # Iterate through df_dict and check for missing keys in user_dict
            for key, df_index in df_dict.items():
                if key not in user_dict:
                    # Append the non-matching row (key and df_index) to the list
                    not_found_list.append((*key, df_index))

            # Convert the list to a DataFrame
            not_found_in_user = pd.DataFrame(
                not_found_list, columns=["Report To", "Thana", "df_index"]
            )

            # Extract unique Report To values
            unique_not_found_combinations = not_found_in_user[["Report To", "Thana"]].drop_duplicates()

            # Update remarks for users not found

            df.loc[
                df[["Report To", "Thana"]].apply(tuple, axis=1).isin(
                    unique_not_found_combinations.apply(tuple, axis=1)
                    ),
                    "remark3"
            ] = "This Report To user in the corresponding Thana  is not found in Database"

            if not unique_not_found_combinations.empty:
                print(
                    "These users on these thanas are not found in the database:\n",
                    unique_not_found_combinations,
                )

            pattern = r"(^0\d{10}$|^1\d{9}$)"

            # Check for invalid mobile numbers in df_user_filtered
            invalid_numbers = ~df["Mobile no"].astype(
                str).str.match(pattern)

            # Get the index of invalid numbers in df_user_filtered and map it to df
            invalid_indices_in_df = df.index[invalid_numbers]

            # Update remarks in df where the mobile number is invalid

            df.loc[invalid_indices_in_df, "remark4"] = "Invalid mobile number"

            # print(df.head())

            final_data = pd.DataFrame()

            filtered_df = df[df[['remark1', 'remark2', 'remark3', 'remark4']].isnull().all(axis=1)]

            # print(filtered_df.head())

            final_data["full_name"] = filtered_df["Name"].str.strip()
            final_data["father_name"] = filtered_df[
                "Father's Name"
            ].str.strip()
            final_data["mother_name"] = filtered_df[
                "Mother's Name"
            ].str.strip()
            final_data["personal_contact"] = filtered_df["Mobile no"]
            final_data["official_contact"] = filtered_df["Mobile no"]
            final_data["pre_address"] = (
                filtered_df["Thana"].str.strip()
                + ","
                + filtered_df["Upazilla"].str.strip()
                + ","
                + filtered_df["District"].str.strip()
                + ","
                + filtered_df["Division"].str.strip()
            )
            final_data["per_address"] = final_data["pre_address"]
            final_data["dob"] = filtered_df["Date of Birth"].dt.strftime(
                "%Y-%m-%d"
            )
            final_data["gender"] = filtered_df["Gender"].str.strip()
            final_data["blood_group"] = "B+"
            final_data["agency_id"] = filtered_df["agency_id"]
            final_data["designation"] = filtered_df[
                "Designation"
            ].str.strip()
            final_data["employment_type"] = 65
            final_data["assigned_role"] = filtered_df["assigned_role"]
            final_data["verification_type"] = "nid"
            final_data["verification_no"] = (
                filtered_df["NID Number"].astype(str).str.strip()
            )

            # Getting Locations IDs

            # Locations groupby name and list of IDs for each name
            loc_dict = (loc[['id', 'upazilla', 'thana']]
                         .drop_duplicates()
                         .assign(upazilla=lambda x: x["upazilla"].str.strip())
                         .assign(thana=lambda x: x["thana"].str.strip())
                         .set_index(["upazilla", "thana"])["id"]
                         .to_dict())

            # Function to get location IDs

            def get_loc_id(upazilla, thana):
                key = (upazilla.strip(), thana.strip())
                if key in loc_dict:
                    return loc_dict[key]
                else:
                    raise ValueError(
                        f"Error: No matching location IDs found for '{upazilla}' and '{thana}"
                    )

            final_data["location"] = filtered_df.apply(lambda row: [get_loc_id(row["Upazilla"], row["Thana"])], axis=1)

            # Checking SUP/FC & Agency and getting the ID of SUP/FC

            # Create a mapping of 'full_name' to 'id' from the user DataFrame
            user_dict = (user[['id', 'full_name', 'location']]
                         .drop_duplicates()
                         .assign(full_name=lambda x: x["full_name"].str.strip())
                         .assign(location=lambda x: x["location"].str.strip())
                         .set_index(["full_name", "location"])["id"]
                         .to_dict())

            # Function to get the report_to ID based on 'Report To'
            def get_reportto_id(report_to, location):
                key = (report_to.strip(), location.strip())
                if key in user_dict:
                    return user_dict[key]
                else:
                    raise ValueError(
                        f"Error: No matching report_to ID found for '{report_to}' in '{location}"
                    )

            final_data["reportto_id"] = filtered_df.apply(lambda row: get_reportto_id(row["Report To"], row["Thana"]), axis=1)

            # Function to process username & password
            def process_name(name):
                name = name.lower()
                name = name.replace(".", "")
                name_parts = name.split()
                result_name = "".join(name_parts[:2])

                return result_name

            temp = pd.DataFrame()

            # Apply the function to process names
            temp["Processed Name"] = (
                filtered_df["Name"].str.strip().apply(process_name)
            )

            # Extract the last 3 digits of the NID Number
            temp["Last 3 Digits"] = (
                filtered_df["NID Number"].astype(str).str[-3:]
            )

            # Merge the processed name and last 3 digits
            temp["username"] = temp["Processed Name"] + temp["Last 3 Digits"]

            final_data["email"] = temp["username"] + "@noemailfound.com"

            final_data["username"] = temp["username"] + "@ubl-" + agency_name

            final_data["password"] = temp["username"]

            final_data["hierarchy_offset"] = 4

            df.to_csv("remarks.csv", index=False)
            final_data.to_csv("output.csv", index=False)
            final_data.to_json("output_file.json", orient="records", indent=4)

            # datapush.create_users_from_dataframe(final_data, os.getenv("BASE_URL"), os.getenv("ADMIN_USERNAME"), os.getenv("ADMIN_PASSWORD"))
