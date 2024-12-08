from flask import Flask, request, jsonify, abort
import argparse

app = Flask(__name__)

stored_data: dict[int, int] = {}
last_key: int = -1

# ----- App routes -----

@app.route('/resource/<resource_id>', methods=['GET'])
def read_resource(resource_id: str):
    key = int(resource_id)
    if key in stored_data:
        return jsonify({"id": key, "data": stored_data[key]}), 200
    else:
        abort(404, description=f"Resource with ID {resource_id} not found.")

@app.route('/resource', methods=['POST'])
def create_resource():
    resource_data = request.json
    if not isinstance(resource_data, int):
        abort(400, description="No data provided to create resource.")

    key = last_key + 1
    last_key += 1
    stored_data[key] = resource_data
    return jsonify({"id": key, "data": resource_data}), 201

@app.route('/resource/<resource_id>', methods=['PUT'])
def update_resource(resource_id: str):
    key = int(resource_id)
    resource_data = request.json
    if not isinstance(resource_data, int):
        abort(400, description="No data provided to update resource.")

    stored_data[key] = resource_data
    return jsonify({"id": key, "data": resource_data}), 200

@app.route('/resource/<resource_id>', methods=['DELETE'])
def delete_resource(resource_id: str):
    key = int(resource_id)
    if key in stored_data:
        del stored_data[key]
        return '', 204
    else:
        abort(404, description=f"Resource with ID {resource_id} not found.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5000,
                        help="Port to run the server on (default: 5000).")
    args = parser.parse_args()

    app.run(debug=True, host='127.0.0.1', port=args.port)
    print("Replica started at port {args.port}")
