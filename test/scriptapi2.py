from fastapi import FastAPI, HTTPException, responses, File, BackgroundTasks
import csv
import json
import zipfile
from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId
from fastapi.responses import StreamingResponse
import os
import random
import string

app = FastAPI()


def connect_to_mongodb(database_name, collection_name):
    client = MongoClient("mongodb://localhost:27017/")
    db = client[database_name]
    collection = db[collection_name]
    return collection


def cleanup_files(orders_path, routes_path, zip_file_path):
    os.remove(orders_path)
    os.remove(zip_file_path)
    os.remove(routes_path)


@app.get("/map/{file_id}")
async def get_csv_files(file_id: str, background_tasks: BackgroundTasks):
    class JSONEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, ObjectId):
                return str(o)
            elif isinstance(o, datetime):
                return o.isoformat()
            return super().default(o)

    def load_json_from_mongodb(collection, document_id):
        try:
            object_id = ObjectId(document_id)
        except Exception as e:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid document ID: ObjectID must be a 24-character hex string {document_id}",
                headers={"X-Error": f"Invalid document ID: {document_id}"},
            )

        document = collection.find_one({"_id": object_id})

        if document is None:
            raise HTTPException(
                status_code=404,
                detail=f"Document with ID {document_id} not found",
                headers={"X-Error": "Document not found"},
            )

        return json.loads(JSONEncoder().encode(document))

    def convert_json_to_csv(json_file):
        orders = []
        routes = []
        orders_map = {}

        for delivery in json_file["request"]["deliveries"]:
            # TODO: too complicated
            order = {
                "lat": delivery["location"]["lat"],
                "lng": delivery["location"]["lng"],
                "order_id": delivery["id"],
                "type": "delivery",
            }
            orders.append(order)
            orders_map[f"{order['order_id']}:{order['type']}"] = [order['lat'], order['lng']]

        for shipment in json_file["request"]["shipments"]:
            # TODO: too complicated
            
            order_pickup_ship = {
                "lat": shipment["pickup"]["location"]["lat"],
                "lng": shipment["pickup"]["location"]["lng"],
                "order_id": shipment["id"],
                "type": "pickupShipment",
            }
            orders.append(order_pickup_ship)
            orders_map[
                f"{order_pickup_ship['order_id']}:{order_pickup_ship['type']}"
            ] = order_pickup_ship

            order_delivery_ship = {
                "lat": shipment["delivery"]["location"]["lat"],
                "lng": shipment["delivery"]["location"]["lng"],
                "order_id": shipment["id"],
                "type": "deliverShipment",
            }
            orders.append(order_delivery_ship)
            orders_map[
                f"{order_delivery_ship['order_id']}:{order_delivery_ship['type']}"
            ] = order_delivery_ship

        for pickup in json_file["request"]["pickups"]:
            # TODO: make it simpler
            order = {
                "lat": pickup["location"]["lat"],
                "lng": pickup["location"]["lng"],
                "order_id": pickup["id"],
                "type": "pickup",
            }
            orders.append(order)
            orders_map[f"{order['order_id']}:{order['type']}"] = order

        for vehicle in json_file["request"]["vehicles"]:
            start_location = vehicle["startLocation"]

            orders_map[f"{vehicle['_id']}:start"] = {
                "lat": start_location["lat"],
                "lng": start_location["lng"],
                "order_id": vehicle["id"],
                "type": "start",
            }
            
            # TODO: add endLocation

        with open("orders.csv", mode="w", newline="") as orders_file:
            orders_writer = csv.DictWriter(
                orders_file, fieldnames=["lat", "lng", "order_id", "type"]
            )
            orders_writer.writeheader()
            orders_writer.writerows(orders)

        for route in json_file["response"]["routes"]:
            index = -1
            
            # TODO: too complicated
            prev_loc = None
            
            for step in route["steps"]:
                index += 1
                
                cur_loc = None
                
                if step['type']=="start" or step['type']=="end":
                    cur_loc = orders_map[f"{route['vehicleId']}:{step['type']}"]
                else:
                    cur_loc = orders_map[f"{step['id']}:{step['type']}"]
                
                if cur_loc and prev_loc:
                    routes.append(
                        {
                            "vehicle_id": route["vehicleId"],
                            "arrival_time": step["arrivalTime"],
                            "index": index,
                            "prev_lat": prev_lat_loc,
                            "curr_lat": cur_loc_lat,
                            "prev_lng": prev_lng_loc,
                            "curr_lng": cur_loc_lng,
                        }
                    )
                
                prev_loc = cur_loc
                
        with open("routes.csv", mode="w", newline="") as routes_file:
            routes_writer = csv.DictWriter(
                routes_file,
                fieldnames=[
                    "prev_lat",
                    "prev_lng",
                    "curr_lat",
                    "curr_lng",
                    "index",
                    "vehicle_id",
                    "arrival_time",
                ],
            )
            routes_writer.writeheader()
            routes_writer.writerows(routes)

    db_name = "tests"
    collection_name = "tests"
    document_id_to_process = file_id
    mongodb_collection = connect_to_mongodb(db_name, collection_name)
    json_data = load_json_from_mongodb(mongodb_collection, document_id_to_process)
    convert_json_to_csv(json_data)
    orders_path = "orders.csv"
    routes_path = "routes.csv"
    zip_file_path = "combined_files.zip"

    with zipfile.ZipFile(zip_file_path, "w") as zipf:
        zipf.write(orders_path)
        zipf.write(routes_path)

    async def generate_zip():
        with open(zip_file_path, mode="rb") as file:
            while chunk := file.read(8192):
                yield chunk

    background_tasks.add_task(cleanup_files, orders_path, routes_path, zip_file_path)

    randomi = "".join(
        [random.choice(string.ascii_letters + string.digits) for n in range(12)]
    )

    zip_name = file_id + randomi + ".zip"

    return StreamingResponse(
        generate_zip(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{zip_name}"'},
    )
