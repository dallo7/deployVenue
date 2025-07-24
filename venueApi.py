from flask import Flask, jsonify
from flask_cors import CORS 
import venueAnalytics 

app = Flask(__name__)
CORS(app)

@app.route('/venue_report/<string:venue_name>', methods=['GET'])
def get_venue_report(venue_name):
    """
    API endpoint to get a venue report by venue name.
    Expects the venue name as a path parameter.
    """
    print(f"\n=====================================================")
    print(f"    API Call Received for Venue: {venue_name}")
    print(f"=====================================================")

    venue_id = venueAnalytics.get_venue_id_by_name(venue_name)

    if venue_id:
     
        analytics_data = venueAnalytics.generate_venue_analytics_report(venue_id)

        if "error" in analytics_data:
            print(f"    ❌ Report generation failed for venue '{venue_name}'.")
            return jsonify({"status": "error", "message": analytics_data["error"]}), 500
        else:
            print(f"    ✅ Report generation complete for venue '{venue_name}'.")
            print("=====================================================")
            return jsonify({"status": "success", "data": analytics_data}), 200
    else:
        print(f"\nCould not proceed with report generation as venue '{venue_name}' was not found.")
        return jsonify({"status": "error", "message": f"Venue '{venue_name}' not found."}), 404


if __name__ == '__main__':

    app.run(host='0.0.0.0', port=5060, debug=False, use_reloader=False)
