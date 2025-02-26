import json
from flask import Flask, current_app, jsonify, request


app = Flask(__name__)

DOLPHINSCHEDULER_ALERT_CONTENT_FIELD = "dsAlertMsg"

@app.route("/alert", methods=['POST'])
def alert():
    data = request.json
    msg_content = data.get(DOLPHINSCHEDULER_ALERT_CONTENT_FIELD)
    if not msg_content:
        current_app.logger.error("Empty alert content")
        return jsonify({"success": False})
    
    try:
        msg = json.loads(msg_content)
    except json.JSONDecodeError:
        current_app.logger.error("Invalid JSON format in alert content")
        return jsonify({"success": False})
    
    alerts = []
    if type(msg) == list:
        alerts = msg
    elif type(msg) == dict:
        alerts.append(msg)
    else:
        current_app.logger.error(f"Invalid message type. msg: {msg}")
        return jsonify({"success": False})
    
    for alert in alerts:
        process_state = alert.get('processState')
        if process_state is None:
            # ingore non process alert
            break
        
        if process_state not in ["SUCCESS", "FAILURE"]:
            # ingore non-end process alert
            break
        
        s = set(['projectCode', 'projectName', 'owner',
             'processId', 'processDefinitionCode', 'processName', 'processType',
             'recovery', 'runTimes',
             'processStartTime', 'processEndTime', 'processHost'])
        if s.issubset(alert.keys()):
            current_app.logger.info(f"Process end alert: {alert}")
            return jsonify({"success": True})
    
    current_app.logger.debug(f"Ignored: {msg}")
    return jsonify({"success": False})

if __name__ == "__main__":
    app.run()