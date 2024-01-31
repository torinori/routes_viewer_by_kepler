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
import requests
import geopandas as gpd
import uvicorn
from dotenv import load_dotenv
import pandas as pd
from keplergl import KeplerGl
from fastapi.responses import HTMLResponse


app = FastAPI()

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
MONGO_CONNECTION_LINK = os.getenv("MONGO_CONNECTION_LINK")


def connect_to_mongodb(database_name, collection_name):
    client = MongoClient(MONGO_CONNECTION_LINK)
    db = client[database_name]
    collection = db[collection_name]
    return collection


def get_osrm_route(coordinates):
    url = (
        "https://dev-routing.relog.kz/route/v1/driving/"
        + coordinates
        + "?geometries=geojson&overview=false&steps=true"
    )
    response = requests.get(url)

    print(url)

    if response.status_code == 200:
        return response.json()
    else:
        raise HTTPException(
            status_code=response.status_code, detail="OSRM API request failed"
        )


mongodb_collection = connect_to_mongodb(DB_NAME, COLLECTION_NAME)


def cleanup_files(orders_path, routes_path, zip_file_path, trips_path):
    os.remove(orders_path)
    os.remove(zip_file_path)
    os.remove(routes_path)
    os.remove(trips_path)


@app.get("/map/{file_id}")
async def get_csv_files(file_id: str, background_tasks: BackgroundTasks):
    randomi = "".join(
        [random.choice(string.ascii_letters + string.digits) for n in range(12)]
    )

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
        trips = []
        orders_map = {}
        if not json_file["request"] or not json_file["response"]:
            raise HTTPException(
                status_code=422,
                detail=f"Document cannot be processed due to its invalid format",
                headers={"X-Error": "Unprocessable content"},
            )

        for delivery in json_file["request"]["deliveries"]:
            # TODO: too complicated
            location = delivery["location"]
            order = {
                "lat": location["lat"],
                "lng": location["lng"],
                "order_id": delivery["id"],
                "type": "delivery",
            }
            orders.append(order)
            orders_map[f"{order['order_id']}:{order['type']}"] = [
                order["lat"],
                order["lng"],
            ]

        for shipment in json_file["request"]["shipments"]:
            # TODO: too complicated
            location_pickup = shipment["pickup"]["location"]
            location_delivery = shipment["delivery"]["location"]

            order_pickup_ship = {
                "lat": location_pickup["lat"],
                "lng": location_pickup["lng"],
                "order_id": shipment["id"],
                "type": "pickupShipment",
            }
            orders.append(order_pickup_ship)
            orders_map[
                f"{order_pickup_ship['order_id']}:{order_pickup_ship['type']}"
            ] = [order_pickup_ship["lat"], order_pickup_ship["lng"]]

            order_delivery_ship = {
                "lat": location_delivery["lat"],
                "lng": location_delivery["lng"],
                "order_id": shipment["id"],
                "type": "deliverShipment",
            }
            orders.append(order_delivery_ship)
            orders_map[
                f"{order_delivery_ship['order_id']}:{order_delivery_ship['type']}"
            ] = [order_delivery_ship["lat"], order_delivery_ship["lng"]]

        for pickup in json_file["request"]["pickups"]:
            # TODO: make it simpler
            location = pickup["location"]
            order = {
                "lat": location["lat"],
                "lng": location["lng"],
                "order_id": pickup["id"],
                "type": "pickup",
            }
            orders.append(order)
            orders_map[f"{order['order_id']}:{order['type']}"] = [
                order["lat"],
                order["lng"],
            ]

        for vehicle in json_file["request"]["vehicles"]:
            start_location = vehicle["startLocation"]
            end_location = vehicle["endLocation"]
            if start_location:
                orders_map[f"{vehicle['id']}:start"] = [
                    start_location["lat"],
                    start_location["lng"],
                ]
            if end_location:
                orders_map[f"{vehicle['id']}:end"] = [
                    end_location["lat"],
                    end_location["lng"],
                ]

        for route in json_file["response"]["routes"]:
            index = -1

            prev_loc = [None, None]

            for step in route["steps"]:
                index += 1

                cur_loc = None

                if step["type"] == "start" or step["type"] == "end":
                    if f"{route['vehicleId']}:{step['type']}" not in orders_map:
                        continue
                    cur_loc = orders_map[f"{route['vehicleId']}:{step['type']}"]
                else:
                    cur_loc = orders_map[f"{step['id']}:{step['type']}"]

                if cur_loc and prev_loc:
                    routes.append(
                        {
                            "vehicle_id": route["vehicleId"],
                            "arrival_time": step["arrivalTime"],
                            "index": index,
                            "prev_lat": prev_loc[0],
                            "curr_lat": cur_loc[0],
                            "prev_lng": prev_loc[1],
                            "curr_lng": cur_loc[1],
                            "end_time": step["endTime"],
                        }
                    )

                prev_loc = cur_loc

        index = 0
        vehicle_coords = {}
        for route in routes:
            vehicle_coords.setdefault(route["vehicle_id"], []).append(
                {
                    "coords": [route["curr_lng"], route["curr_lat"]],
                    "arrival_time": route["arrival_time"],
                    "end_time": route["end_time"],
                }
            )

        for v_id, coords in vehicle_coords.items():
            coords_str = ";".join(
                [f"{c['coords'][0]},{c['coords'][1]}" for c in coords]
            )

            osrm_response = get_osrm_route(coords_str)

            trip = []

            for i in range(len(coords) - 1):
                route_duration = (
                    vehicle_coords[v_id][i + 1]["arrival_time"]
                    - vehicle_coords[v_id][i]["end_time"]
                )

                cnt = 0
                num = 0

                osrm_leg = osrm_response["routes"][0]["legs"][i]

                for step in osrm_leg["steps"]:
                    num += len(step["geometry"]["coordinates"])

                for step in osrm_leg["steps"]:
                    for point in step["geometry"]["coordinates"]:
                        t = (
                            vehicle_coords[v_id][i]["end_time"]
                            + cnt * route_duration / num
                        )

                        trip.append([point[0], point[1], 0, int(t)])
                        cnt += 1

                        assert cnt <= num

            trips.append(trip)

        with open(
            "orders" + "_" + randomi + ".csv", mode="w", newline=""
        ) as orders_file:
            orders_writer = csv.DictWriter(
                orders_file, fieldnames=["lat", "lng", "order_id", "type"]
            )
            orders_writer.writeheader()
            orders_writer.writerows(orders)
        global time, lati, longi
        time = routes[0]["end_time"]
        lati = routes[0]["curr_lat"]
        longi = routes[0]["curr_lng"]

        with open(
            "routes" + "_" + randomi + ".csv", mode="w", newline=""
        ) as routes_file:
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
                    "end_time",
                ],
            )
            routes_writer.writeheader()
            routes_writer.writerows(routes)
        id_index = -1
        list_of_ids = list(vehicle_coords.keys())
        geojson_data = {"type": "FeatureCollection", "features": []}
        for trip in trips:
            id_index += 1
            feature = {
                "type": "Feature",
                "properties": {
                    "vendor": list_of_ids[id_index],
                },
                "geometry": {"type": "LineString", "coordinates": trip},
            }
            geojson_data["features"].append(feature)
        with open("trips" + "_" + randomi + ".geojson", "w") as geojson_file:
            json.dump(geojson_data, geojson_file)

    document_id_to_process = file_id
    json_data = load_json_from_mongodb(mongodb_collection, document_id_to_process)
    convert_json_to_csv(json_data)
    orders_path = "orders" + "_" + randomi + ".csv"
    routes_path = "routes" + "_" + randomi + ".csv"
    trips_path = "trips" + "_" + randomi + ".geojson"
    zip_file_path = "combined_files.zip"

    with zipfile.ZipFile(zip_file_path, "w") as zipf:
        zipf.write(orders_path)
        zipf.write(routes_path)
        zipf.write(trips_path)

    # df = gpd.read_file(trips_path)
    # all_data = df
    # all_data = gpd.concat([all_data, df])
    with open(trips_path, "r") as file:
        geojson_data = json.load(file)
    my_map_config = {
        "version": "v1",
        "config": {
            "visState": {
                "filters": [],
                "layers": [
                    {
                        "id": "n7piwgv",
                        "type": "trip",
                        "config": {
                            "dataId": "-wo1lhh",
                            "label": "trips_NDtJ65T2f2g8[1]",
                            "color": [255, 203, 153],
                            "highlightColor": [252, 242, 26, 255],
                            "columns": {"geojson": "_geojson"},
                            "isVisible": True,
                            "visConfig": {
                                "opacity": 0.56,
                                "thickness": 1.9,
                                "colorRange": {
                                    "name": "Uber Viz Qualitative 4",
                                    "type": "qualitative",
                                    "category": "Uber",
                                    "colors": [
                                        "#12939A",
                                        "#DDB27C",
                                        "#88572C",
                                        "#FF991F",
                                        "#F15C17",
                                        "#223F9A",
                                        "#DA70BF",
                                        "#125C77",
                                        "#4DC19C",
                                        "#776E57",
                                        "#17B8BE",
                                        "#F6D18A",
                                        "#B7885E",
                                        "#FFCB99",
                                        "#F89570",
                                        "#829AE3",
                                        "#E79FD5",
                                        "#1E96BE",
                                        "#89DAC1",
                                        "#B3AD9E",
                                    ],
                                },
                                "trailLength": 586,
                                "sizeRange": [0, 10],
                            },
                            "hidden": False,
                            "textLabel": [
                                {
                                    "field": None,
                                    "color": [255, 255, 255],
                                    "size": 18,
                                    "offset": [0, 0],
                                    "anchor": "start",
                                    "alignment": "center",
                                    "outlineWidth": 0,
                                    "outlineColor": [255, 0, 0, 255],
                                    "background": False,
                                    "backgroundColor": [0, 0, 200, 255],
                                }
                            ],
                        },
                        "visualChannels": {
                            "colorField": {"name": "vendor", "type": "string"},
                            "colorScale": "ordinal",
                            "sizeField": None,
                            "sizeScale": "linear",
                        },
                    }
                ],
                "effects": [],
                "interactionConfig": {
                    "tooltip": {
                        "fieldsToShow": {
                            "-wo1lhh": [{"name": "vendor", "format": None}]
                        },
                        "compareMode": False,
                        "compareType": "absolute",
                        "enabled": True,
                    },
                    "brush": {"size": 0.5, "enabled": False},
                    "geocoder": {"enabled": False},
                    "coordinate": {"enabled": False},
                },
                "layerBlending": "normal",
                "overlayBlending": "normal",
                "splitMaps": [],
                "animationConfig": {"currentTime": time, "speed": 0.227},
                "editor": {"features": [], "visible": True},
            },
            "mapState": {
                "bearing": 0,
                "dragRotate": False,
                "latitude": lati,
                "longitude": longi,
                "pitch": 0,
                "zoom": 11.510293575244276,
                "isSplit": False,
                "isViewportSynced": True,
                "isZoomLocked": False,
                "splitMapViewports": [],
            },
            "mapStyle": {
                "styleType": "dark",
                "topLayerGroups": {},
                "visibleLayerGroups": {
                    "label": True,
                    "road": True,
                    "border": False,
                    "building": True,
                    "water": True,
                    "land": True,
                    "3d building": False,
                },
                "threeDBuildingColor": [
                    15.035172933000911,
                    15.035172933000911,
                    15.035172933000911,
                ],
                "backgroundColor": [0, 0, 0],
                "mapStyles": {},
            },
        },
    }
    kepler = KeplerGl(
        data={"-wo1lhh": geojson_data}, config=my_map_config, height=600
    )

    kepler_html = kepler._repr_html_()

    background_tasks.add_task(
        cleanup_files, orders_path, routes_path, zip_file_path, trips_path
    )

    return HTMLResponse(content=kepler_html, status_code=200)


if __name__ == "__main__":
    uvicorn.run("web_app:app", host="0.0.0.0", port=8000)
