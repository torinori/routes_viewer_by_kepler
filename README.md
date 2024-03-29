
  
<!-- ABOUT THE PROJECT -->
## About The Project


In this project, api of converter script from json to csv was created. Created csv files are joined by a zip file. Then created csv files can be uploaded to kepler.gl to create routes and orders map in kepler.gl. It is intended to save time for converting file format from json to csv.
API endpoint is : GET /map/{file_id}. Making GET request to this endpoint results in csv files being downloaded.





<!-- GETTING STARTED -->
## Getting Started


### Prerequisites

Python, MongoDB and FastAPI should be installed on a computer to use the API.

### Installation and use

_Below is process by which you can use API on your computer_

1. Clone the repo
   ```sh
   git clone [https://github.com/your_username_/Project-Name.git](https://github.com/torinori/routes_viewer_by_kepler.git](https://github.com/torinori/routes_viewer_by_kepler.git)
   ```
2. Install python packages

   If any of the packages in the script API are not installed, you can install required packages through pip install or install through virtual environment
   
3. Setup connection to required database

   In json_to_csv_converter.py file locate DB_NAME, COLLECTION_NAME, MONGO_CONNECTION_LINK files. For these files, database name, collection name and connection link to MongoDB database should be written in .env file. When you have filled out this information, you can then proceed to the next step.
   
4. Start using the api
   
   In git bash terminal change directory to the location of json_to_csv_converter python script. Then type uvicorn json_to_csv_converter:app --reload, to start the work of API.
   
5. Get csv files

   You can make get request in api by visiting following link. http://127.0.0.1:8000/map/fileid replace fileid with json ObjectId you want to convert in MongoDB. Accessing the link results in zip file containing2 csv files being downloaded.
   
6. Upload files to kepler.gl
   
   In this final step, now you should extract two csv files from zip file, and then you can upload them to kepler gl to create map of orders and routes.
