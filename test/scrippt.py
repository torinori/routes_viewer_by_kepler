import csv
import json
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
    #orders_map[f"{json_file['response']['routes'][0]['vehicleId']}:start"] = {"lat":json_file['request']['vehicles'][0]['startLocation']['lat'],
                                                                              #"lng":json_file['request']['vehicles'][0]['startLocation']['lng'],
                                                                                #"order_id":json_file['request']['vehicles'][0]['id'],
                                                                                  #"type":"start"}
        
    with open("orders.csv", mode = "w", newline = "") as orders_file:
        orders_writer = csv.DictWriter(orders_file, fieldnames = ["lat","lng","order_id","type"])
        orders_writer.writeheader()
        orders_writer.writerows(orders)
    #index = -1
    for route in json_file["response"]["routes"]:
        index = -1
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
                

                routes.append({
                "vehicle_id": step['id'],
                "arrival_time":step['arrivalTime'],
                "index":index,
                "prev_lat":routes[index-1].get('curr_lat'),
                "curr_lat": orders_map[f"{step['id']}:{step['type']}"]["lat"],
                "prev_lng":routes[index-1].get('curr_lng'),
                "curr_lng": orders_map[f"{step['id']}:{step['type']}"]["lng"]
                })
        
    with open("routes.csv", mode="w", newline="") as routes_file:

        routes_writer = csv.DictWriter(routes_file, fieldnames=[
            "prev_lat", "prev_lng", "curr_lat", "curr_lng", "index", "vehicle_id", "arrival_time"
        ])
        routes_writer.writeheader()
        routes_writer.writerows(routes)

with open("C:/Users/alanz/Desktop/view route/routes_viewer_by_kepler/test/shipments.json") as json_file:
    deliveries_data = json.load(json_file)

convert_json_to_csv(deliveries_data)