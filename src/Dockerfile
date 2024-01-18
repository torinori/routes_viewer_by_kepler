FROM python:3.11.1

WORKDIR /json_to_csv_converter

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000



CMD ["uvicorn", "json_to_csv_converter:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

