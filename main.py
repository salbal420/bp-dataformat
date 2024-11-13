import UBL_UserCreate_DataChecker_updated as dataChecker

given_file = "Pond's Kitty Party BP@ubl-mapl.xlsx"

valid_agency_ids = [1, 3, 4, 5, 6, 7, 8]

while True:
    agency_id = input(
        "Enter Agency ID:\n1. asiatic\n3. hvl\n4. pixel\n5. sc\n6. mapl\n7. ial\n8. vtwo\n"
    )

    if agency_id.isdigit() and int(agency_id) in valid_agency_ids:
        agency_mapping = {
                1: "asiatic",
                3: "hvl",
                4: "pixel",
                5: "sc",
                6: "mapl",
                7: "ial",
                8: "vtwo",
            }
        agency_name = agency_mapping.get(int(agency_id))
        dataChecker.userCreateDatacheck(given_file, agency_id, agency_name)
        break
    else:
        print("Invalid input. Please enter a valid number from the list.")
