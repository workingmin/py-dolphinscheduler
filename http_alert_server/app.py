from flask import Flask, jsonify, request


app = Flask(__name__)

DOLPHINSCHEDULER_ALERT_CONTENT_FIELD = "dsAlertMsg"

@app.route("/alert", methods=['POST'])
def alert():
    data = request.json
    process_alerts = data.get(DOLPHINSCHEDULER_ALERT_CONTENT_FIELD)
    for process_alert in process_alerts:
        project_code = process_alert.get('projectCode')
        project_name = process_alert.get('projectName')
        owner = process_alert.get('owner')
        process_id = process_alert.get('processId')
        process_definition_code = process_alert.get('processDefinitionCode')
        process_name = process_alert.get('processName')
        process_type = process_alert.get('processType')
        process_state = process_alert.get('processState')
        modify_by = process_alert.get('modifyBy')
        recovery = process_alert.get('recovery')
        run_times = process_alert.get('runTimes')
        process_start_time = process_alert.get('processStartTime')
        process_end_time = process_alert.get('processEndTime')
        process_host = process_alert.get('processHost')
        
        
    
    return jsonify({"code": 0})

if __name__ == "__main__":
    app.run()