import csv
import json
from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId
class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        elif isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)
    
def connect_to_mongodb(database_name, collection_name):
    client = MongoClient("mongodb://localhost:27017/")
    db = client[database_name]
    collection = db[collection_name]
    return collection

def load_json_from_mongodb(collection, document_id):
    document = collection.find_one({"_id": ObjectId(document_id)})
    return json.loads(JSONEncoder().encode(document))

def convert_json_to_csv(json_file):
    orders = []
    routes = []   
    orders_map = {}
    
    for delivery in json_file["request"]["deliveries"]:

        order = {"lat": delivery["location"]["lat"], 
                      "lng":delivery["location"]["lng"],
                      "order_id":delivery["id"],
                      "type":"delivery"}
        
        orders.append(order)
        orders_map[f"{order['order_id']}:{order['type']}"] = order

    for shipment in json_file["request"]["shipments"]:
        order_pickup_ship = {"lat": shipment['pickup']["location"]["lat"], 
                      "lng":shipment['pickup']["location"]["lng"],
                      "order_id":shipment["id"],
                      "type":"pickupShipment"}
        orders.append(order_pickup_ship)
        orders_map[f"{order_pickup_ship['order_id']}:{order_pickup_ship['type']}"] = order_pickup_ship
        
        order_delivery_ship = {"lat": shipment['delivery']["location"]["lat"], 
                      "lng":shipment['delivery']["location"]["lng"],
                      "order_id":shipment["id"],
                      "type":"deliverShipment"}
        orders.append(order_delivery_ship)
        orders_map[f"{order_delivery_ship['order_id']}:{order_delivery_ship['type']}"] = order_delivery_ship

    
    for pickup in json_file["request"]["pickups"]:
        order = {"lat": pickup["location"]["lat"], 
                      "lng":pickup["location"]["lng"],
                      "order_id":pickup["id"],
                      "type":"pickup"}
        orders.append(order)
        orders_map[f"{order['order_id']}:{order['type']}"] = order
    
    vehicleNum = 0
    for routi in json_file['response']['routes']:
        orders_map[f"{routi['vehicleId']}:start"]={"lat":json_file['request']['vehicles'][vehicleNum]['startLocation']['lat'],
                                                   "lng":json_file['request']['vehicles'][vehicleNum]['startLocation']['lng'],
                                                   "order_id":json_file['request']['vehicles'][vehicleNum]['id'],
                                                   "type":"start"}
        vehicleNum+=1
        
    with open("orders.csv", mode = "w", newline = "") as orders_file:
        orders_writer = csv.DictWriter(orders_file, fieldnames = ["lat","lng","order_id","type"])
        orders_writer.writeheader()
        orders_writer.writerows(orders)
    #index = -1
    for route in json_file["response"]["routes"]:
        index = -1
        prev_lng_loc = orders_map[f"{route['vehicleId']}:{'start'}"]["lng"]
        prev_lat_loc = orders_map[f"{route['vehicleId']}:{'start'}"]["lat"]
        for step in route["steps"]:
            index += 1  
            if step["type"]=="start":
                routes.append({
                    "vehicle_id": route["vehicleId"],
                    "arrival_time":step["arrivalTime"],
                    "index": index,
                    "prev_lat":orders_map[f"{route['vehicleId']}:{step['type']}"]["lat"],
                    "curr_lat": orders_map[f"{route['vehicleId']}:{step['type']}"]["lat"],
                    "prev_lng":orders_map[f"{route['vehicleId']}:{step['type']}"]["lng"],
                    "curr_lng": orders_map[f"{route['vehicleId']}:{step['type']}"]["lng"]
                    

                })   
          
            elif step["type"]=="end":

                continue

            else:
                cur_loc_lat = orders_map[f"{step['id']}:{step['type']}"]["lat"]
                cur_loc_lng = orders_map[f"{step['id']}:{step['type']}"]["lng"]
                

                routes.append({
                "vehicle_id": route["vehicleId"],
                "arrival_time":step['arrivalTime'],
                "index":index,
                "prev_lat":prev_lat_loc,
                "curr_lat": cur_loc_lat,
                "prev_lng":prev_lng_loc,
                "curr_lng": cur_loc_lng
                })
                prev_lng_loc = cur_loc_lng
                prev_lat_loc = cur_loc_lat
        
    with open("routes.csv", mode="w", newline="") as routes_file:

        routes_writer = csv.DictWriter(routes_file, fieldnames=[
            "prev_lat", "prev_lng", "curr_lat", "curr_lng", "index", "vehicle_id", "arrival_time"
        ])
        routes_writer.writeheader()
        routes_writer.writerows(routes)

db_name = 'tests'
collection_name = 'tests'

document_id_to_process = '657947a9b6ba8600300569c5'


mongodb_collection = connect_to_mongodb(db_name, collection_name)
json_data = load_json_from_mongodb(mongodb_collection, document_id_to_process)

convert_json_to_csv(json_data)

