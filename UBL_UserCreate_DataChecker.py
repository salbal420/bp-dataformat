import db_datafetch as datafetch
import db_datapush as datapush
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()


def userCreateDatacheck(given_data, agency_id, agency_name):

    df = pd.read_excel(given_data)
    df["agency_id"] = int(agency_id)

    # Handling roles
    get_role = df["Role"].str.strip().unique()
    if len(get_role) != 1:
        raise ValueError(
            f"Error: Expected exactly one role, but found {len(get_role)}. Roles: {get_role}"
        )
    else:
        role = get_role[0].lower()
        role_mapping = {"bp": 4, "brand promoter": 4, "sup": 3, "supervisor": 3}

        df["assigned_role"] = role_mapping.get(role, 0)
        df["remarks"] = df["assigned_role"].apply(
            lambda x: "Wrong role provided" if x == 0 else ""
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
            not_found_in_loc = df_role_filtered["Thana"][
                ~df_role_filtered["Thana"].str.strip().isin(loc["name"].str.strip())
            ]

            # Update remarks for locations not found
            df.loc[df["Thana"].str.strip().isin(not_found_in_loc.str.strip()), "remarks"] = (
                "Locations not found"
            )

            # Filtering found locations
            df_loc_filtered = df_role_filtered[
                ~df_role_filtered["Thana"].str.strip().isin(not_found_in_loc.str.strip())
            ]

            if not not_found_in_loc.empty:
                print(
                    "The following locations are not found in database:\n",
                    not_found_in_loc,
                )

            df_loc_filtered_dict = {
                (
                    row["Report To"].strip(),
                    row["Thana"].strip(),
                ): index
                for index, row in df_loc_filtered.iterrows()
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
            for key, df_index in df_loc_filtered_dict.items():
                if key not in user_dict:
                    # Append the non-matching row (key and df_index) to the list
                    not_found_list.append((*key, df_index))

            # Convert the list to a DataFrame
            not_found_in_user = pd.DataFrame(
                not_found_list, columns=["Report To", "Thana", "df_index"]
            )

            # Extract unique Report To values
            unique_not_found_user = not_found_in_user["Report To"].str.strip().unique()

            # Update remarks for users not found
            df.loc[df["Report To"].str.strip().isin(unique_not_found_user), "remarks"] = (
                "Report To user is not found"
            )

            # Filtering found users
            df_user_filtered = df_loc_filtered[
                ~df_loc_filtered["Report To"].str.strip().isin(unique_not_found_user)
            ]

            if unique_not_found_user.any():
                print(
                    "These users are not found in the database:\n",
                    unique_not_found_user,
                )

            pattern = r"(^0\d{10}$|^1\d{9}$)"

            # Check for invalid mobile numbers in df_user_filtered
            invalid_numbers = ~df_user_filtered["Mobile no"].astype(str).str.match(pattern)

            # Get the index of invalid numbers in df_user_filtered and map it to df
            invalid_indices_in_df = df_user_filtered.index[invalid_numbers]

            # Update remarks in df where the mobile number is invalid
            df.loc[invalid_indices_in_df, "remarks"] = "Invalid mobile number"
            df_user_filtered_valid = df_user_filtered[~invalid_numbers]

            final_data = pd.DataFrame()
            final_data["full_name"] = df_user_filtered_valid["Name"].str.strip()
            final_data["father_name"] = df_user_filtered_valid[
                "Father's Name"
            ].str.strip()
            final_data["mother_name"] = df_user_filtered_valid[
                "Mother's Name"
            ].str.strip()
            final_data["personal_contact"] = df_user_filtered_valid["Mobile no"]
            final_data["official_contact"] = df_user_filtered_valid["Mobile no"]
            final_data["pre_address"] = (
                df_user_filtered_valid["Thana"].str.strip()
                + ","
                + df_user_filtered_valid["Upazila"].str.strip()
                + ","
                + df_user_filtered_valid["District"].str.strip()
                + ","
                + df_user_filtered_valid["Division"].str.strip()
            )
            final_data["per_address"] = final_data["pre_address"]
            final_data["dob"] = df_user_filtered_valid["Date of Birth"].dt.strftime(
                "%Y-%m-%d"
            )
            final_data["gender"] = df_user_filtered_valid["Gender"].str.strip()
            final_data["blood_group"] = "B+"
            final_data["agency_id"] = df_user_filtered_valid["agency_id"]
            final_data["designation"] = df_user_filtered_valid[
                "Designation"
            ].str.strip()
            final_data["employment_type"] = 65
            final_data["assigned_role"] = df_user_filtered_valid["assigned_role"]
            final_data["verification_type"] = "nid"
            final_data["verification_no"] = (
                df_user_filtered_valid["NID Number"].astype(str).str.strip()
            )

            # Getting Locations IDs

            # Locations groupby name and list of IDs for each name
            loc["name"] = loc["name"].str.strip()
            loc_grouped = loc.groupby("name")["id"].apply(list)

            # Function to get location IDs
            def get_location_ids(thana):
                if thana in loc_grouped:
                    return loc_grouped[thana]
                else:
                    raise ValueError(
                        f"Error: No matching location IDs found for '{thana}'"
                    )

            final_data["location"] = (
                df_user_filtered_valid["Thana"].str.strip().apply(get_location_ids)
            )

            # Checking SUP/FC & Agency and getting the ID of SUP/FC

            # Create a mapping of 'full_name' to 'id' from the user DataFrame
            user_dict = (user[['id', 'full_name']]
             .drop_duplicates()
             .assign(full_name=lambda x: x["full_name"].str.strip())
             .set_index("full_name")["id"]
             .to_dict())

            # Function to get the report_to ID based on 'Report To'
            def get_reportto_id(report_to):
                report_to = report_to.strip()
                if report_to in user_dict:
                    return user_dict[report_to]
                else:
                    raise ValueError(
                        f"Error: No matching report_to ID found for '{report_to}'"
                    )

            final_data["reportto_id"] = df_user_filtered_valid["Report To"].str.strip().apply(lambda x: int(get_reportto_id(x)))

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
                df_user_filtered_valid["Name"].str.strip().apply(process_name)
            )

            # Extract the last 3 digits of the NID Number
            temp["Last 3 Digits"] = (
                df_user_filtered_valid["NID Number"].astype(str).str[-3:]
            )

            # Merge the processed name and last 3 digits
            temp["username"] = temp["Processed Name"] + temp["Last 3 Digits"]

            final_data["email"] = temp["username"] + "@noemailfound.com"


            final_data["username"] = temp["username"] + "@ubl-" + agency_name

            final_data["password"] = temp["username"]

            final_data["hierarchy_offset"] = 4

            df['combined'] = df[["Name", "Report To"]].apply(lambda x: (x["Name"].strip(), x["Report To"].strip()), axis=1)
            df_user_filtered_valid['combined'] = df_user_filtered_valid[["Name", "Report To"]].apply(
                lambda x: (x["Name"].strip(), x["Report To"].strip()), axis=1)

            # Find valid indices where both 'Name' and 'Report To' match
            valid_indices = df.index[df['combined'].isin(df_user_filtered_valid['combined'])]

            # Update the 'remarks' column in df for the valid rows
            df.loc[valid_indices, "remarks"] = "Successful"

            df.to_csv("remarks.csv", index=False)
            final_data.to_csv("output.csv", index=False)
            final_data.to_json("output_file.json", orient="records", indent=4)

            # datapush.create_users_from_dataframe(final_data, os.getenv("BASE_URL"), os.getenv("ADMIN_USERNAME"), os.getenv("ADMIN_PASSWORD"))
