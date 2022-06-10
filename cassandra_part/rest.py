from flask import Flask, request, jsonify
from batches_from_kafka import CassandraClient
import json


app = Flask(__name__)
host = 'cassandra-server'
port = 9042
keyspace = 'project'



@app.get("/all_domains")
def select1():
    # Return the list of existing domains for which pages were created
    return json.dumps(client.select1(), indent=4, sort_keys=True, default=str)


@app.get("/user_pages&user_id=<user_id>")
def select2(user_id):
    #  Return all the pages which were created by the user with a specified user_id.
    return json.dumps(client.select2(user_id), indent=4, sort_keys=True, default=str)

@app.get("/domain_articles&domain=<domain>")
def select3(domain):
    #  Return the number of articles created for a specified domain.
    return json.dumps(client.select3(domain), indent=4, sort_keys=True, default=str)

@app.get("/page&page_id=<page_id>")
def select4(page_id):
    #  Return the page with the specified page_id
    return json.dumps(client.select4(page_id), indent=4, sort_keys=True, default=str)

@app.get("/all_pages&from=<date1>&to=<date2>")
def select5(date1, date2):
    # Return N most reviewed items (by # of reviews) for a given period of time
    return json.dumps(client.select5(date1, date2), indent=4, sort_keys=True, default=str)





if __name__ == "__main__":
    client = CassandraClient(host='cassandra-server', port=9042, keyspace='project')
    client.connect()
    app.run(host='0.0.0.0', port=8080)
