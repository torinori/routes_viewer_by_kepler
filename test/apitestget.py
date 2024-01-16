import requests
import requests

document_id = '657947a9b6ba8600300569c5'

url = f'http://127.0.0.1:8000/map/{document_id}'

response = requests.get(url)

if response.status_code == 200:
    with open("orders.csv", "wb") as orders_file:
        orders_file.write(response.content)

    print("Orders.csv file saved.")

    with open("routes.csv", "wb") as routes_file:
        routes_file.write(response.content)

    print("Routes.csv file saved.")
else:
    print(f"Error: {response.status_code} - {response.text}")
