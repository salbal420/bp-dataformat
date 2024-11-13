import requests

# Step 1: Fetch Admin API token
def get_admin_token(base_url, admin_username, admin_password):
    login_url = f"{base_url}/signin"
    payload = {
        "username": admin_username,
        "password": admin_password
    }

    # Send request to the login endpoint
    response = requests.post(login_url, json=payload)

    if response.status_code == 200:
        token = response.json().get("token")
        return token
    else:
        raise Exception(f"Failed to get admin token: {response.text}")


# Step 2: Function to create a user using API
def create_user(api_url, user_data, token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Send a POST request to create the user
    response = requests.post(api_url, headers=headers, json=user_data)

    if response.status_code == 201:
        print(f"User {user_data['username']} created successfully.")
    else:
        print(f"Failed to create user {user_data['username']}: {response.text}")


# Step 3: Main process to iterate through DataFrame and create users
def create_users_from_dataframe(final_data, base_url, admin_username, admin_password):
    # Get the admin token
    token = get_admin_token(base_url, admin_username, admin_password)

    # Define the API endpoint to create users
    create_user_url = f"{base_url}/ff-manager/ff-create"  # Adjust the endpoint as necessary

    # Iterate through each row in the DataFrame and create them via API
    for index, row in final_data.iterrows():
        # Convert the row to a dictionary (Pandas row -> dictionary)
        user_data = row.to_dict()

        # Call the function to create a user
        create_user(create_user_url, user_data, token)